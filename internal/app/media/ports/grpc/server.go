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
	"google.golang.org/grpc/status"

	"github.com/dwspurpleteam/streamapi/internal/app/media/domain"
	media "github.com/dwspurpleteam/streamapi/internal/app/media/service"
	pb "github.com/dwspurpleteam/streamapi/internal/pkg/gen/streamapi/v1"
	streamstats "github.com/dwspurpleteam/streamapi/internal/pkg/stream"
	"github.com/dwspurpleteam/streamapi/pkg/metrics"
)

type Server struct {
	pb.UnimplementedMediaServiceServer

	manager      *media.Manager
	timeout      time.Duration
	logger       *slog.Logger
	wg           sync.WaitGroup
	counters     streamstats.Counters
	slow         atomic.Uint64
	queueAge     *metrics.Histogram
	sendDuration *metrics.Histogram
}

func New(
	manager *media.Manager,
	timeout time.Duration,
	buckets []float64,
	logger *slog.Logger,
) *Server {
	buckets = slices.Clone(buckets)

	return &Server{
		manager: manager,
		timeout: timeout,
		logger:  logger,

		queueAge: metrics.NewHistogram(prometheus.HistogramOpts{
			Name: "streamapi_media_queue_age_seconds",
			Help: "Media frame age after leaving the subscriber queue",

			Buckets: buckets,
		}),

		sendDuration: metrics.NewHistogram(prometheus.HistogramOpts{
			Name: "streamapi_media_send_seconds",
			Help: "Time spent in grpc send, excluding receiver acknowledgement",

			Buckets: buckets,
		}),
	}
}

func (s *Server) Subscribe(request *pb.SubscribeRequest, stream rpc.ServerStreamingServer[pb.SubscribeResponse]) error {
	selection := domain.Selection{
		Participant: request.GetParticipantId(),
		Track:       request.GetTrackId(),
	}
	for _, kind := range request.GetKinds() {
		switch kind {
		case pb.MediaKind_MEDIA_KIND_AUDIO:
			selection.Kinds |= 1 << domain.Audio
		case pb.MediaKind_MEDIA_KIND_VIDEO:
			selection.Kinds |= 1 << domain.Video
		default:
			return status.Error(codes.InvalidArgument, "unsupported media kind")
		}
	}

	subscription, err := s.manager.Subscribe(stream.Context(), domain.Call{
		ID:   request.GetCallId(),
		Room: request.GetRoomName(),
	}, selection)
	if err != nil {
		return rpcError(err)
	}
	defer subscription.Close()

	s.logger.Info("media subscription started", "call_id", request.GetCallId())
	defer s.logger.Info("media subscription stopped", "call_id", request.GetCallId())

	result := make(chan error, 1)
	counters := s.counters.Start()
	s.wg.Go(func() {
		result <- s.send(stream, subscription, counters)
	})

	const watchdogSamples = 4

	timer := time.NewTicker(s.timeout / watchdogSamples)
	defer timer.Stop()

	for {
		select {
		case err := <-result:
			if errors.Is(err, media.ErrSlowConsumer) {
				s.slow.Add(1)
			}

			return rpcError(err)
		case <-subscription.Done():
			if errors.Is(subscription.Err(), media.ErrSlowConsumer) {
				s.slow.Add(1)
			}

			return rpcError(subscription.Err())
		case <-stream.Context().Done():
			return status.FromContextError(stream.Context().Err()).Err()
		case now := <-timer.C:
			started := counters.Sending.Load()
			if started != 0 && now.Sub(time.Unix(0, started)) > s.timeout {
				s.slow.Add(1)

				return status.Error(codes.ResourceExhausted, "media send stalled")
			}
		}
	}
}

func (s *Server) send(
	stream rpc.ServerStreamingServer[pb.SubscribeResponse],
	sub *media.Subscription,
	counters *streamstats.Sender,
) error {
	defer s.counters.Stop(counters)

	known := make(map[uint32]struct{})

	send := func(message *pb.SubscribeResponse) error {
		start := time.Now()
		counters.Sending.Store(start.UnixNano())

		err := stream.Send(message)

		counters.Sending.Store(0)
		s.sendDuration.Observe(counters.Histogram, time.Since(start).Seconds())

		if err != nil {
			return fmt.Errorf("send media response: %w", err)
		}

		return nil
	}

	for {
		frame, err := sub.Next(stream.Context())
		if err != nil {
			return err
		}

		t := frame.Track

		s.queueAge.Observe(counters.Histogram, time.Since(frame.ReceivedAt).Seconds())

		if frame.Ended {
			if _, exists := known[t.Handle]; !exists {
				continue
			}

			delete(known, t.Handle)

			if err := send(&pb.SubscribeResponse{
				Payload: &pb.SubscribeResponse_Ended{
					Ended: &pb.TrackEnded{
						Handle: t.Handle,
					},
				},
			}); err != nil {
				return err
			}

			continue
		}

		if _, exists := known[t.Handle]; !exists {
			if err := send(&pb.SubscribeResponse{Payload: &pb.SubscribeResponse_Track{Track: &pb.TrackInfo{
				Handle:        t.Handle,
				TrackId:       t.ID,
				ParticipantId: t.Participant,
				Kind:          wireKind(t.Kind),

				MimeType:  t.MIME,
				ClockRate: t.ClockRate,
				Channels:  t.Channels,
				Fmtp:      t.FMTP,
			}}}); err != nil {
				return err
			}

			known[t.Handle] = struct{}{}
		}

		if !sub.AllowSend(len(frame.Data)) {
			return status.Error(codes.ResourceExhausted, "media egress capacity exhausted")
		}

		if err := send(packetResponse(frame)); err != nil {
			return err
		}

		counters.Sent.Add(1)
		counters.Bytes.Add(uint64(len(frame.Data)))
	}
}

func wireKind(kind domain.MediaKind) pb.MediaKind {
	switch kind {
	case domain.Audio:
		return pb.MediaKind_MEDIA_KIND_AUDIO
	case domain.Video:
		return pb.MediaKind_MEDIA_KIND_VIDEO
	default:
		return pb.MediaKind_MEDIA_KIND_UNSPECIFIED
	}
}

func (s *Server) Wait(ctx context.Context) error {
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

func rpcError(err error) error {
	switch {
	case err == nil, errors.Is(err, media.ErrClosed):
		return nil
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	case errors.Is(err, media.ErrInvalid):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, media.ErrNotFound):
		return status.Error(codes.NotFound, "call not found")
	case errors.Is(err, media.ErrLimit), errors.Is(err, media.ErrSlowConsumer):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, media.ErrSource):
		return status.Error(codes.Unavailable, "media source unavailable")
	default:
		if _, ok := status.FromError(err); ok {
			return err
		}

		return status.Error(codes.Internal, "media stream failed")
	}
}

type Snapshot struct {
	Sent  uint64
	Bytes uint64
	Slow  uint64
}

func (s *Server) Snapshot() Snapshot {
	counters := s.counters.Snapshot()

	return Snapshot{
		Sent:  counters.Sent,
		Bytes: counters.Bytes,
		Slow:  s.slow.Load(),
	}
}

func (s *Server) Describe(ch chan<- *prometheus.Desc) {
	s.queueAge.Describe(ch)
	s.sendDuration.Describe(ch)
}

func (s *Server) Collect(ch chan<- prometheus.Metric) {
	s.queueAge.Collect(ch)
	s.sendDuration.Collect(ch)
}
