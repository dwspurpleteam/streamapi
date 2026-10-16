package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func run(logger *slog.Logger) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	app, err := newApp(cfg, logger)
	if err != nil {
		return fmt.Errorf("create callrelay: %w", err)
	}
	defer app.publisher.Close()

	if err := app.serve(ctx, cfg, logger); err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("run callrelay: %w", err)
	}

	return nil
}
