package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	broker "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/dwspurpleteam/streamapi/internal/app/events/domain"
)

var errStreamBusy = errors.New("event stream initialization in progress")

const (
	unlimitedReconnects  = -1
	eventEnvelopeVersion = 1
)

type Config struct {
	Name            string
	URL             string
	Username        string
	Password        string
	Stream          string
	Prefix          string
	MaxBytes        int64
	MaxMessageBytes int32
	MaxAge          time.Duration
	DuplicateWindow time.Duration
	Timeout         time.Duration
	ReconnectWait   time.Duration
}

type Publisher struct {
	connection   *broker.Conn
	js           jetstream.JetStream
	cfg          Config
	initializing atomic.Bool
	ready        atomic.Bool
}

type envelope struct {
	Version     uint32    `json:"version"`
	ID          string    `json:"eventId"`
	Type        string    `json:"type"`
	Source      string    `json:"source"`
	CallID      string    `json:"callId"`
	Room        string    `json:"roomName"`
	Participant string    `json:"participantId,omitempty"`
	Track       string    `json:"trackId,omitempty"`
	OccurredAt  time.Time `json:"occurredAt"`
}

func New(cfg Config) (*Publisher, error) {
	opts := []broker.Option{
		broker.Name(cfg.Name),
		broker.Timeout(cfg.Timeout),
		broker.ReconnectWait(cfg.ReconnectWait),
		broker.RetryOnFailedConnect(true),
		broker.MaxReconnects(unlimitedReconnects),
		broker.ReconnectBufSize(0),
	}
	if cfg.Username != "" {
		opts = append(opts, broker.UserInfo(cfg.Username, cfg.Password))
	}

	connection, err := broker.Connect(cfg.URL, opts...)
	if err != nil {
		return nil, fmt.Errorf("connect nats: %w", err)
	}

	js, err := jetstream.New(connection)
	if err != nil {
		connection.Close()

		return nil, fmt.Errorf("create jetstream client: %w", err)
	}

	return &Publisher{
		connection: connection,
		js:         js,
		cfg:        cfg,
	}, nil
}

func (p *Publisher) Ensure(ctx context.Context) error {
	if !p.initializing.CompareAndSwap(false, true) {
		return errStreamBusy
	}

	defer p.initializing.Store(false)

	ctx, cancel := context.WithTimeout(ctx, p.cfg.Timeout)
	defer cancel()

	_, err := p.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     p.cfg.Stream,
		Subjects: []string{p.cfg.Prefix + ".>"},

		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
		Discard:   jetstream.DiscardNew,

		MaxBytes:   p.cfg.MaxBytes,
		MaxAge:     p.cfg.MaxAge,
		Duplicates: p.cfg.DuplicateWindow,
		MaxMsgSize: p.cfg.MaxMessageBytes,
	})
	p.ready.Store(err == nil)

	if err != nil {
		return fmt.Errorf("configure event stream: %w", err)
	}

	return nil
}

func (p *Publisher) Publish(ctx context.Context, e domain.Event) error {
	if !p.Ready() {
		if err := p.Ensure(ctx); err != nil {
			return fmt.Errorf("ensure event stream: %w", err)
		}
	}

	data, err := json.Marshal(envelope{
		Version: eventEnvelopeVersion,
		ID:      e.ID,
		Type:    e.Type,
		Source:  e.Source,
		CallID:  e.CallID,
		Room:    e.Room,

		Participant: e.Participant,
		Track:       e.Track,
		OccurredAt:  e.OccurredAt,
	})
	if err != nil {
		return fmt.Errorf("encode call event: %w", err)
	}

	_, err = p.js.Publish(
		ctx,
		p.cfg.Prefix+"."+e.Type,
		data,
		jetstream.WithMsgID(e.Source+":"+e.ID),
	)
	if err != nil {
		p.ready.Store(false)

		return fmt.Errorf("publish call event: %w", err)
	}

	return nil
}

func (p *Publisher) Ready() bool {
	return p.connection.IsConnected() && p.ready.Load()
}

func (p *Publisher) Close() {
	p.connection.Close()
}
