package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	broker "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/dwspurpleteam/callrelay/internal/app/callrelay/domain"
)

var (
	errConsumerDisconnected = errors.New("event consumer disconnected")
	errEventEnvelope        = errors.New("invalid call event envelope")
)

const ephemeralConsumerReplicas = 1

type SubscriberConfig struct {
	BufferEvents      int
	BatchBytes        int
	PullExpiry        time.Duration
	InactiveThreshold time.Duration
}

type Subscriber struct {
	publisher *Publisher
	cfg       SubscriberConfig
	logger    *slog.Logger
}

func NewSubscriber(publisher *Publisher, cfg SubscriberConfig, logger *slog.Logger) *Subscriber {
	return &Subscriber{
		publisher: publisher,
		cfg:       cfg,
		logger:    logger,
	}
}

func (s *Subscriber) Run(ctx context.Context, emit func(domain.Event, int), ready func()) (runErr error) {
	defer func() {
		if runErr != nil && ctx.Err() == nil {
			s.logger.Warn("event delivery unavailable", "error", runErr)
		}
	}()

	p := s.publisher
	subjectPrefix := p.cfg.Prefix + "."

	changes := p.connection.StatusChanged(broker.RECONNECTING, broker.CLOSED)
	defer p.connection.RemoveStatusListener(changes)

	if !p.Ready() {
		return errConsumerDisconnected
	}

	setupCtx, cancel := context.WithTimeout(ctx, p.cfg.Timeout)
	defer cancel()

	consumer, err := p.js.CreateConsumer(setupCtx, p.cfg.Stream, jetstream.ConsumerConfig{
		DeliverPolicy:     jetstream.DeliverNewPolicy,
		AckPolicy:         jetstream.AckNonePolicy,
		FilterSubject:     subjectPrefix + ">",
		InactiveThreshold: s.cfg.InactiveThreshold,
		MemoryStorage:     true,
		Replicas:          ephemeralConsumerReplicas,
	})
	if err != nil {
		return fmt.Errorf("create event consumer: %w", err)
	}

	defer func() {
		cleanupCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), p.cfg.Timeout)
		defer stop()

		if deleteErr := p.js.DeleteConsumer(cleanupCtx, p.cfg.Stream, consumer.CachedInfo().Name); deleteErr != nil && !errors.Is(deleteErr, jetstream.ErrConsumerNotFound) {
			runErr = errors.Join(runErr, fmt.Errorf("delete event consumer: %w", deleteErr))
		}
	}()

	failures := make(chan error, 1)
	fail := func(err error) {
		select {
		case failures <- err:
		default:
		}
	}

	reader, err := consumer.Consume(
		func(message jetstream.Msg) {
			var data envelope
			if decodeErr := json.Unmarshal(message.Data(), &data); decodeErr != nil {
				fail(fmt.Errorf("decode call event: %w", decodeErr))

				return
			}

			event := domain.Event{
				ID:          data.ID,
				Type:        data.Type,
				Source:      data.Source,
				CallID:      data.CallID,
				Room:        data.Room,
				Participant: data.Participant,
				Track:       data.Track,
				OccurredAt:  data.OccurredAt,
			}

			kind, matched := strings.CutPrefix(message.Subject(), subjectPrefix)
			if data.Version != eventEnvelopeVersion || !event.Valid() || !matched || kind != event.Type {
				fail(errEventEnvelope)

				return
			}

			emit(event, len(message.Data()))
		},
		jetstream.PullMaxMessagesWithBytesLimit(s.cfg.BufferEvents, s.cfg.BatchBytes),
		jetstream.PullExpiry(s.cfg.PullExpiry),
		jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
			fail(fmt.Errorf("read event stream: %w", err))
		}),
	)
	if err != nil {
		return fmt.Errorf("start event consumer: %w", err)
	}

	defer func() {
		reader.Stop()
		<-reader.Closed()
	}()

	if !p.connection.IsConnected() {
		return errConsumerDisconnected
	}

	ready()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-changes:
		return errConsumerDisconnected
	case err := <-failures:
		return err
	case <-reader.Closed():
		return errConsumerDisconnected
	}
}
