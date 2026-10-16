package main

import (
	"fmt"
	"log/slog"
	"net/http"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	natsadapter "github.com/dwspurpleteam/streamapi/internal/app/events/adapters/nats"
	eventsgrpc "github.com/dwspurpleteam/streamapi/internal/app/events/ports/grpc"
	webhookport "github.com/dwspurpleteam/streamapi/internal/app/events/ports/webhook"
	webhooklivekit "github.com/dwspurpleteam/streamapi/internal/app/events/ports/webhook/livekit"
	events "github.com/dwspurpleteam/streamapi/internal/app/events/service"
	livekitadapter "github.com/dwspurpleteam/streamapi/internal/app/media/adapters/livekit"
	mediagrpc "github.com/dwspurpleteam/streamapi/internal/app/media/ports/grpc"
	media "github.com/dwspurpleteam/streamapi/internal/app/media/service"
	pb "github.com/dwspurpleteam/streamapi/internal/pkg/gen/streamapi/v1"
)

type app struct {
	media        *media.Manager
	publisher    *natsadapter.Publisher
	events       *events.Receiver
	dispatcher   *events.Dispatcher
	eventStreams *eventsgrpc.Server
	webhook      *webhookport.Handler
	streams      *mediagrpc.Server
	grpc         *grpc.Server
	http         *http.Server
}

func newApp(c *config, logger *slog.Logger) (*app, error) {
	source := livekitadapter.New(c.sourceConfig(), logger)
	manager := media.New(c.mediaConfig(), source)

	publisher, err := natsadapter.New(c.publisherConfig())
	if err != nil {
		return nil, fmt.Errorf("configure event publisher: %w", err)
	}

	receiver := events.New(publisher, c.Events.PublishTimeout)
	verifier := webhooklivekit.NewVerifier(
		c.LiveKit.APIKey,
		c.LiveKit.APISecret,
		c.LiveKit.EventSource,
		c.LiveKit.EventTypes,
	)
	hook := webhookport.New(
		verifier,
		receiver,
		c.HTTP.MaxBodyBytes,
		c.HTTP.Concurrency,
	)

	subscriber := natsadapter.NewSubscriber(
		publisher,
		natsadapter.SubscriberConfig{
			BufferEvents:      c.NATS.ConsumerBufferEvents,
			BatchBytes:        c.NATS.ConsumerBatchBytes,
			PullExpiry:        c.NATS.ConsumerPullExpiry,
			InactiveThreshold: c.NATS.ConsumerInactiveThreshold,
		},
		logger,
	)
	dispatcher := events.NewDispatcher(c.deliveryConfig(), subscriber)

	app := &app{
		media:      manager,
		publisher:  publisher,
		events:     receiver,
		dispatcher: dispatcher,
		eventStreams: eventsgrpc.New(
			dispatcher,
			c.Events.Delivery.SendTimeout,
			c.GRPC.LatencyBucketsSeconds,
			logger,
		),
		webhook: hook,
		streams: mediagrpc.New(
			manager,
			c.GRPC.SendTimeout,
			c.GRPC.LatencyBucketsSeconds,
			logger,
		),
	}

	app.grpc = newGRPCServer(c.GRPC, app.streams, app.eventStreams)
	app.http = app.newHTTPServer(c.HTTP)

	return app, nil
}

func newGRPCServer(c grpcConfig, port *mediagrpc.Server, eventPort *eventsgrpc.Server) *grpc.Server {
	rpc := grpc.NewServer(
		grpc.MaxConcurrentStreams(c.MaxConcurrentStreams),
		grpc.MaxRecvMsgSize(c.MaxMessageBytes),
		grpc.StaticStreamWindowSize(c.StreamWindowBytes),
		grpc.StaticConnWindowSize(c.ConnectionWindowBytes),
		grpc.MaxSendMsgSize(c.MaxMessageBytes),
		grpc.WriteBufferSize(c.WriteBufferBytes),
		grpc.MaxHeaderListSize(c.MaxHeaderBytes),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    c.KeepaliveTime,
			Timeout: c.KeepaliveTimeout,
		}),
	)
	pb.RegisterMediaServiceServer(rpc, port)
	pb.RegisterEventServiceServer(rpc, eventPort)

	return rpc
}

func (c *config) deliveryConfig() events.DeliveryConfig {
	return events.DeliveryConfig{
		MaxSubscriptions: c.Events.Delivery.MaxSubscriptions,
		MaxPerCall:       c.Events.Delivery.MaxPerCall,
		MaxFilters:       c.Events.Delivery.MaxFilters,
		QueueEvents:      c.Events.Delivery.QueueEvents,
		QueueBytes:       c.Events.Delivery.QueueBytes,
		QueueAge:         c.Events.Delivery.QueueAge,
		MaxReservedBytes: c.Events.Delivery.MaxReservedBytes,
		RetryInterval:    c.NATS.RecoveryInterval,
	}
}

func (c *config) mediaConfig() media.Config {
	return media.Config{
		MaxSubscriptions: c.Media.MaxSubscriptions,
		MaxPerCall:       c.Media.MaxPerCall,

		QueuePackets: c.Media.QueuePackets,
		QueueBytes:   c.Media.QueueBytes,
		QueueAge:     c.Media.QueueAge,

		MaxReservedBytes:     c.Media.MaxReservedBytes,
		EgressBytesPerSecond: c.Media.EgressBytesPerSecond,
		EgressBurstBytes:     c.Media.EgressBurstBytes,
	}
}

func (c *config) sourceConfig() livekitadapter.Config {
	return livekitadapter.Config{
		Name:      c.Name,
		URL:       c.LiveKit.URL,
		APIKey:    c.LiveKit.APIKey,
		APISecret: c.LiveKit.APISecret,

		ConnectTimeout: c.LiveKit.ConnectTimeout,
		ReadTimeout:    c.LiveKit.ReadTimeout,

		TokenTTL:       c.LiveKit.TokenTTL,
		MaxTracks:      c.LiveKit.MaxTracks,
		MaxPacketBytes: c.LiveKit.MaxPacketBytes,
	}
}

func (c *config) publisherConfig() natsadapter.Config {
	return natsadapter.Config{
		Name:     c.Name,
		URL:      c.NATS.URL,
		Username: c.NATS.Username,
		Password: c.NATS.Password,
		Stream:   c.NATS.Stream,
		Prefix:   c.NATS.Prefix,

		MaxBytes:        c.NATS.MaxBytes,
		MaxMessageBytes: c.NATS.MaxMessageBytes,
		MaxAge:          c.NATS.MaxAge,

		DuplicateWindow: c.NATS.DuplicateWindow,
		Timeout:         c.NATS.Timeout,
		ReconnectWait:   c.NATS.ReconnectWait,
	}
}
