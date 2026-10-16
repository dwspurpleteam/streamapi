package service

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/dwspurpleteam/streamapi/internal/app/events/domain"
)

var (
	ErrInvalid = errors.New("invalid call event")
	ErrBusy    = errors.New("event capacity exhausted")
)

type Publisher interface {
	Publish(ctx context.Context, event domain.Event) error
}

type Receiver struct {
	publisher Publisher
	slots     chan struct{}
	timeout   time.Duration
	accepted  atomic.Uint64
	failed    atomic.Uint64
	rejected  atomic.Uint64
}

type Snapshot struct {
	Accepted uint64
	Failed   uint64
	Rejected uint64
}

func New(publisher Publisher, concurrency int, timeout time.Duration) *Receiver {
	return &Receiver{
		publisher: publisher,
		slots:     make(chan struct{}, concurrency),
		timeout:   timeout,
	}
}

func (r *Receiver) Accept(ctx context.Context, event domain.Event) error {
	if !event.Valid() {
		r.rejected.Add(1)

		return ErrInvalid
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	select {
	case r.slots <- struct{}{}:
		defer func() {
			<-r.slots
		}()
	default:
		r.rejected.Add(1)

		return ErrBusy
	}

	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	if err := r.publisher.Publish(ctx, event); err != nil {
		r.failed.Add(1)

		return err
	}

	r.accepted.Add(1)

	return nil
}

func (r *Receiver) Snapshot() Snapshot {
	return Snapshot{
		Accepted: r.accepted.Load(),
		Failed:   r.failed.Load(),
		Rejected: r.rejected.Load(),
	}
}
