package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"google.golang.org/grpc"

	natsadapter "github.com/dwspurpleteam/streamapi/internal/app/events/adapters/nats"
	"github.com/dwspurpleteam/streamapi/pkg/listener"
)

func (a *app) serve(ctx context.Context, c *config, logger *slog.Logger) error {
	grpcListener, err := listener.Listen(ctx, c.GRPC.Address, c.GRPC.Connections)
	if err != nil {
		return fmt.Errorf("listen grpc: %w", err)
	}
	defer grpcListener.Close()

	httpListener, err := listener.Listen(ctx, c.HTTP.Address, c.HTTP.Connections)
	if err != nil {
		return fmt.Errorf("listen http: %w", err)
	}
	defer httpListener.Close()

	ensureCtx, stopEnsure := context.WithTimeout(ctx, c.NATS.Timeout)
	if err := a.publisher.Ensure(ensureCtx); err != nil {
		logger.Warn("event publication unavailable", "error", err)
	}

	stopEnsure()

	const servingTransports = 2

	errorsCh := make(chan error, servingTransports)

	var workers sync.WaitGroup

	maintenanceCtx, cancelMaintenance := context.WithCancel(ctx)
	defer cancelMaintenance()

	workers.Go(func() {
		recoverEvents(
			maintenanceCtx,
			a.publisher,
			c.NATS.RecoveryInterval,
			logger,
		)
	})
	workers.Go(func() {
		a.dispatcher.Run(maintenanceCtx)
	})
	workers.Go(func() {
		errorsCh <- a.grpc.Serve(grpcListener)
	})
	workers.Go(func() {
		errorsCh <- a.http.Serve(httpListener)
	})

	logger.Info(
		"service started",
		"name",
		c.Name,
		"grpc",
		c.GRPC.Address,
		"http",
		c.HTTP.Address,
	)

	var servingErr error
	select {
	case <-ctx.Done():
	case servingErr = <-errorsCh:
	}

	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.ShutdownTimeout)
	defer cancel()

	cancelMaintenance()

	_ = grpcListener.Close()
	_ = httpListener.Close()

	shutdownErr := a.shutdown(shutdownCtx)

	workers.Wait()

	logger.Info("service stopped", "name", c.Name)

	if errors.Is(servingErr, http.ErrServerClosed) || errors.Is(servingErr, grpc.ErrServerStopped) {
		servingErr = nil
	}

	return errors.Join(servingErr, shutdownErr)
}

func (a *app) shutdown(ctx context.Context) error {
	managerErr := a.media.Close(ctx)
	done := make(chan struct{})

	go func() {
		a.grpc.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		a.grpc.Stop()
		<-done
	}

	httpErr := a.http.Shutdown(ctx)
	if httpErr != nil {
		httpErr = errors.Join(httpErr, a.http.Close())
	}

	streamErr := a.streams.Wait(ctx)
	eventStreamErr := a.eventStreams.Wait(ctx)

	return errors.Join(
		managerErr,
		httpErr,
		streamErr,
		eventStreamErr,
	)
}

func recoverEvents(
	ctx context.Context,
	publisher *natsadapter.Publisher,
	interval time.Duration,
	logger *slog.Logger,
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if publisher.Ready() {
				continue
			}

			if err := publisher.Ensure(ctx); err != nil && ctx.Err() == nil {
				logger.Warn("event publication recovery failed", "error", err)
			}
		}
	}
}
