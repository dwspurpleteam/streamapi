package main

import (
	"fmt"
	"log/slog"
	"net/http"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	livekitadapter "github.com/dwspurpleteam/callrelay/internal/app/callrelay/adapters/livekit"
	natsadapter "github.com/dwspurpleteam/callrelay/internal/app/callrelay/adapters/nats"
	grpcport "github.com/dwspurpleteam/callrelay/internal/app/callrelay/ports/grpc"
	webhookport "github.com/dwspurpleteam/callrelay/internal/app/callrelay/ports/webhook"
	webhooklivekit "github.com/dwspurpleteam/callrelay/internal/app/callrelay/ports/webhook/livekit"
	"github.com/dwspurpleteam/callrelay/internal/app/callrelay/service/events"
	"github.com/dwspurpleteam/callrelay/internal/app/callrelay/service/media"
	pb "github.com/dwspurpleteam/callrelay/internal/pkg/gen/callrelay/v1"
)

type app struct {
	media     *media.Manager
	publisher *natsadapter.Publisher
	events    *events.Receiver
	webhook   *webhookport.Handler
	streams   *grpcport.Server
	grpc      *grpc.Server
	http      *http.Server
}

func newApp(c *config, keys secrets, logger *slog.Logger) (*app, error) {
	source, err := livekitadapter.New(c.sourceConfig(keys), logger)
	if err != nil {
		return nil, fmt.Errorf("configure media source: %w", err)
	}

	manager, err := media.New(c.mediaConfig(), source)
	if err != nil {
		return nil, err
	}

	publisher, err := natsadapter.New(c.publisherConfig(keys))
	if err != nil {
		return nil, fmt.Errorf("configure event publisher: %w", err)
	}

	receiver, hook, err := newWebhook(c, keys, publisher)
	if err != nil {
		publisher.Close()

		return nil, err
	}

	app := &app{
		media:     manager,
		publisher: publisher,
		events:    receiver,
		webhook:   hook,
		streams:   grpcport.New(manager, c.GRPC.SendTimeout, c.GRPC.LatencyBucketsSeconds, logger),
	}

	app.grpc = newGRPCServer(c.GRPC, app.streams)
	app.http = app.newHTTPServer(c.HTTP)

	return app, nil
}

func newWebhook(c *config, keys secrets, publisher *natsadapter.Publisher) (*events.Receiver, *webhookport.Handler, error) {
	receiver, err := events.New(publisher, c.Events.Concurrency, c.Events.PublishTimeout)
	if err != nil {
		return nil, nil, err
	}

	verifier := webhooklivekit.NewVerifier(keys.LiveKitKey, keys.LiveKitSecret, c.LiveKit.EventSource, c.LiveKit.EventTypes)

	hook, err := webhookport.New(verifier, receiver, c.HTTP.MaxBodyBytes, c.HTTP.Concurrency)
	if err != nil {
		return nil, nil, fmt.Errorf("configure webhook receiver: %w", err)
	}

	return receiver, hook, nil
}

func newGRPCServer(c grpcConfig, port *grpcport.Server) *grpc.Server {
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

	return rpc
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

func (c *config) sourceConfig(s secrets) livekitadapter.Config {
	return livekitadapter.Config{
		URL:       c.LiveKit.URL,
		APIKey:    s.LiveKitKey,
		APISecret: s.LiveKitSecret,

		ConnectTimeout: c.LiveKit.ConnectTimeout,
		ReadTimeout:    c.LiveKit.ReadTimeout,

		TokenTTL:       c.LiveKit.TokenTTL,
		MaxTracks:      c.LiveKit.MaxTracks,
		MaxPacketBytes: c.LiveKit.MaxPacketBytes,
	}
}

func (c *config) publisherConfig(s secrets) natsadapter.Config {
	return natsadapter.Config{
		URL:      c.NATS.URL,
		Username: s.NATSUser,
		Password: s.NATSPassword,
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
