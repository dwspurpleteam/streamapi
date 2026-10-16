package grpc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	rpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/dwspurpleteam/callrelay/internal/app/callrelay/domain"
	"github.com/dwspurpleteam/callrelay/internal/app/callrelay/service/events"
	pb "github.com/dwspurpleteam/callrelay/internal/pkg/gen/callrelay/v1"
	"github.com/dwspurpleteam/callrelay/pkg/metrics"
)

type EventServer struct {
	pb.UnimplementedEventServiceServer

	dispatcher   *events.Dispatcher
	timeout      time.Duration
	logger       *slog.Logger
	wg           sync.WaitGroup
	counters     counterSet
	slow         atomic.Uint64
	queueAge     *metrics.Histogram
	sendDuration *metrics.Histogram
}

type EventSnapshot struct {
	Sent uint64
	Slow uint64
}

type eventMessage struct {
	response   pb.SubscribeEventsResponse
	occurredAt timestamppb.Timestamp
}

func NewEventServer(dispatcher *events.Dispatcher, timeout time.Duration, buckets []float64, logger *slog.Logger) *EventServer {
	buckets = slices.Clone(buckets)

	return &EventServer{
		dispatcher: dispatcher,
		timeout:    timeout,
		logger:     logger,

		queueAge: metrics.NewHistogram(prometheus.HistogramOpts{
			Name:    "callrelay_events_queue_age_seconds",
			Help:    "Event age after leaving the subscriber queue",
			Buckets: buckets,
		}),
		sendDuration: metrics.NewHistogram(prometheus.HistogramOpts{
			Name:    "callrelay_events_send_seconds",
			Help:    "Time spent in event grpc send, excluding receiver acknowledgement",
			Buckets: buckets,
		}),
	}
}

func (s *EventServer) Subscribe(request *pb.SubscribeEventsRequest, stream rpc.ServerStreamingServer[pb.SubscribeEventsResponse]) error {
	sub, err := s.dispatcher.Subscribe(stream.Context(), events.Selection{
		CallID:      request.GetCallId(),
		Participant: request.GetParticipantId(),
		Types:       request.GetEventTypes(),
	})
	if err != nil {
		return eventError(err)
	}
	defer sub.Close()

	if err := stream.SendHeader(metadata.MD{}); err != nil {
		return eventError(fmt.Errorf("send event subscription headers: %w", err))
	}

	s.logger.Info("event subscription started", "call_id", request.GetCallId())
	defer s.logger.Info("event subscription stopped", "call_id", request.GetCallId())

	counters := s.counters.start()
	result := make(chan error, 1)

	sendCtx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	s.wg.Go(func() {
		result <- s.send(sendCtx, stream, sub, counters)
	})

	const watchdogSamples = 4

	ticker := time.NewTicker(s.timeout / watchdogSamples)
	defer ticker.Stop()

	for {
		select {
		case err := <-result:
			return s.subscriptionError(err)
		case <-sub.Done():
			return s.subscriptionError(sub.Err())
		case <-stream.Context().Done():
			return status.FromContextError(stream.Context().Err()).Err()
		case now := <-ticker.C:
			started := counters.sending.Load()
			if started != 0 && now.Sub(time.Unix(0, started)) > s.timeout {
				s.slow.Add(1)

				return status.Error(codes.ResourceExhausted, "event send stalled")
			}
		}
	}
}

func (s *EventServer) send(ctx context.Context, stream rpc.ServerStreamingServer[pb.SubscribeEventsResponse], sub *events.Subscription, counters *deliveryCounters) error {
	defer s.counters.stop(counters)

	for {
		item, err := sub.Next(ctx)
		if err != nil {
			return err
		}

		response := eventResponse(item.Event)

		start := time.Now()
		s.queueAge.Observe(counters.histogram, start.Sub(item.ReceivedAt).Seconds())
		counters.sending.Store(start.UnixNano())

		err = stream.Send(response)

		counters.sending.Store(0)
		s.sendDuration.Observe(counters.histogram, time.Since(start).Seconds())

		if err != nil {
			return fmt.Errorf("send event response: %w", err)
		}

		counters.sent.Add(1)
	}
}

func eventResponse(event domain.Event) *pb.SubscribeEventsResponse {
	message := &eventMessage{}
	message.occurredAt = *timestamppb.New(event.OccurredAt)
	message.response = pb.SubscribeEventsResponse{
		EventId:       event.ID,
		Type:          event.Type,
		Source:        event.Source,
		CallId:        event.CallID,
		RoomName:      event.Room,
		ParticipantId: event.Participant,
		TrackId:       event.Track,
		OccurredAt:    &message.occurredAt,
	}

	return &message.response
}

func (s *EventServer) subscriptionError(err error) error {
	if errors.Is(err, events.ErrSlowConsumer) {
		s.slow.Add(1)
	}

	return eventError(err)
}

func eventError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, events.ErrInvalid):
		return status.Error(codes.InvalidArgument, "invalid event subscription")
	case errors.Is(err, events.ErrBusy), errors.Is(err, events.ErrSlowConsumer):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, events.ErrUnavailable):
		return status.Error(codes.Unavailable, "event source unavailable")
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	case errors.Is(err, events.ErrClosed):
		return nil
	default:
		if _, ok := status.FromError(err); ok {
			return err
		}

		return status.Error(codes.Internal, "event delivery failed")
	}
}

func (s *EventServer) Wait(ctx context.Context) error {
	done := make(chan struct{})

	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *EventServer) Snapshot() EventSnapshot {
	counters := s.counters.snapshot()

	return EventSnapshot{
		Sent: counters.Sent,
		Slow: s.slow.Load(),
	}
}

func (s *EventServer) Describe(ch chan<- *prometheus.Desc) {
	s.queueAge.Describe(ch)
	s.sendDuration.Describe(ch)
}

func (s *EventServer) Collect(ch chan<- prometheus.Metric) {
	s.queueAge.Collect(ch)
	s.sendDuration.Collect(ch)
}
