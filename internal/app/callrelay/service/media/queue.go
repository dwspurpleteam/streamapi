package media

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/dwspurpleteam/callrelay/internal/app/callrelay/domain"
)

var (
	ErrSlowConsumer = errors.New("slow media consumer")
	ErrClosed       = errors.New("media subscription closed")
	ErrLimit        = errors.New("media capacity exhausted")
	ErrInvalid      = errors.New("invalid media subscription")
	ErrNotFound     = errors.New("call not found")
	ErrSource       = errors.New("media source unavailable")
)

type queue struct {
	mu       sync.Mutex
	items    []domain.Frame
	head     int
	count    int
	bytes    int
	maxBytes int
	maxAge   time.Duration
	err      error
	notify   chan struct{}
	done     chan struct{}
}

func newQueue(packets, bytes int, age time.Duration) *queue {
	return &queue{
		items:    make([]domain.Frame, packets),
		maxBytes: bytes,

		maxAge: age,
		notify: make(chan struct{}, 1),
		done:   make(chan struct{}),
	}
}

func (q *queue) push(f domain.Frame) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.err != nil {
		return false
	}

	if q.count == len(q.items) || len(f.Data) > q.maxBytes-q.bytes ||
		(q.count > 0 && time.Since(q.items[q.head].ReceivedAt) > q.maxAge) {
		q.stop(ErrSlowConsumer)

		return false
	}

	q.items[(q.head+q.count)%len(q.items)] = f
	q.count++
	q.bytes += len(f.Data)

	select {
	case q.notify <- struct{}{}:
	default:
	}

	return true
}

func (q *queue) pop() (domain.Frame, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.err != nil {
		return domain.Frame{}, false, q.err
	}

	if q.count == 0 {
		return domain.Frame{}, false, nil
	}

	f := q.items[q.head]
	if time.Since(f.ReceivedAt) > q.maxAge {
		q.stop(ErrSlowConsumer)

		return domain.Frame{}, false, q.err
	}

	q.items[q.head] = domain.Frame{}
	q.head = (q.head + 1) % len(q.items)
	q.count--
	q.bytes -= len(f.Data)

	return f, true, nil
}

func (q *queue) next(ctx context.Context) (domain.Frame, error) {
	for {
		if err := ctx.Err(); err != nil {
			return domain.Frame{}, err
		}

		f, ok, err := q.pop()
		if ok || err != nil {
			return f, err
		}

		select {
		case <-ctx.Done():
			return domain.Frame{}, ctx.Err()
		case <-q.done:
		case <-q.notify:
		}
	}
}

func (q *queue) stop(err error) {
	if q.err == nil {
		q.err = err
		close(q.done)
	}
}

func (q *queue) close(err error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.stop(err)
	clear(q.items)
	q.count, q.bytes = 0, 0
}

func (q *queue) cause() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	return q.err
}
