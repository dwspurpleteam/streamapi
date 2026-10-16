package service

import (
	"context"
	"sync"
	"time"

	"github.com/dwspurpleteam/streamapi/internal/app/events/domain"
)

type Delivery struct {
	Event      domain.Event
	ReceivedAt time.Time
	Bytes      int
}

type queue struct {
	mu       sync.Mutex
	items    []*Delivery
	head     int
	count    int
	bytes    int
	maxBytes int
	maxAge   time.Duration
	err      error
	notify   chan struct{}
	done     chan struct{}
}

func newQueue(capacity, bytes int, age time.Duration) *queue {
	return &queue{
		items:    make([]*Delivery, capacity),
		maxBytes: bytes,
		maxAge:   age,
		notify:   make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
}

func (q *queue) push(item *Delivery) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.err != nil {
		return
	}

	if q.count == len(q.items) || item.Bytes > q.maxBytes-q.bytes ||
		(q.count > 0 && time.Since(q.items[q.head].ReceivedAt) > q.maxAge) {
		q.stop(ErrSlowConsumer)

		return
	}

	q.items[(q.head+q.count)%len(q.items)] = item
	q.count++
	q.bytes += item.Bytes

	select {
	case q.notify <- struct{}{}:
	default:
	}
}

func (q *queue) Next(ctx context.Context) (*Delivery, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		item, ok, err := q.pop()
		if ok || err != nil {
			return item, err
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-q.done:
		case <-q.notify:
		}
	}
}

func (q *queue) pop() (*Delivery, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.err != nil {
		return nil, false, q.err
	}

	if q.count == 0 {
		return nil, false, nil
	}

	item := q.items[q.head]
	if time.Since(item.ReceivedAt) > q.maxAge {
		q.stop(ErrSlowConsumer)

		return nil, false, q.err
	}

	q.items[q.head] = nil
	q.head = (q.head + 1) % len(q.items)
	q.count--
	q.bytes -= item.Bytes

	return item, true, nil
}

func (q *queue) stop(err error) {
	if q.err == nil {
		q.err = err
		clear(q.items)
		q.count, q.bytes = 0, 0

		close(q.done)
	}
}

func (q *queue) close(err error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.stop(err)
}

func (q *queue) cause() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	return q.err
}
