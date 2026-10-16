package events

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dwspurpleteam/callrelay/internal/app/callrelay/domain"
)

var (
	ErrSlowConsumer = errors.New("slow event consumer")
	ErrClosed       = errors.New("event subscription closed")
	ErrUnavailable  = errors.New("event source unavailable")
)

type Source interface {
	Run(ctx context.Context, emit func(domain.Event, int), ready func()) error
}

type DeliveryConfig struct {
	MaxSubscriptions int
	MaxPerCall       int
	MaxFilters       int
	QueueEvents      int
	QueueBytes       int
	QueueAge         time.Duration
	MaxReservedBytes int64
	RetryInterval    time.Duration
}

type Selection struct {
	CallID      string
	Participant string
	Types       []string
}

type Dispatcher struct {
	mu       sync.Mutex
	calls    sync.Map
	cfg      DeliveryConfig
	source   Source
	count    int
	reserved int64
	ready    bool
	closed   bool

	rejected atomic.Uint64
	failures atomic.Uint64
}

type Subscription struct {
	q          *queue
	selection  Selection
	dispatcher *Dispatcher
	once       sync.Once
}

type DeliverySnapshot struct {
	Subscriptions int
	ReservedBytes int64
	Rejected      uint64
	Failures      uint64
	Ready         bool
}

func NewDispatcher(cfg DeliveryConfig, source Source) *Dispatcher {
	return &Dispatcher{
		cfg:    cfg,
		source: source,
	}
}

func (d *Dispatcher) Run(ctx context.Context) {
	defer d.stop(true)

	timer := time.NewTimer(d.cfg.RetryInterval)
	defer timer.Stop()

	for ctx.Err() == nil {
		err := d.source.Run(ctx, d.publish, func() {
			d.mu.Lock()
			d.ready = !d.closed && ctx.Err() == nil
			d.mu.Unlock()
		})
		d.stop(false)

		if err != nil && ctx.Err() == nil {
			d.failures.Add(1)
		}

		timer.Reset(d.cfg.RetryInterval)

		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
	}
}

func (d *Dispatcher) Subscribe(ctx context.Context, selection Selection) (*Subscription, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if selection.CallID == "" || len(selection.CallID) > domain.MaxIdentifierBytes ||
		len(selection.Participant) > domain.MaxIdentifierBytes || len(selection.Types) > d.cfg.MaxFilters {
		return nil, ErrInvalid
	}

	for _, name := range selection.Types {
		if !domain.ValidEventType(name) {
			return nil, ErrInvalid
		}
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if !d.ready || d.closed {
		return nil, ErrUnavailable
	}

	current := d.subscribers(selection.CallID)

	if d.count >= d.cfg.MaxSubscriptions || len(current) >= d.cfg.MaxPerCall ||
		int64(d.cfg.QueueBytes) > d.cfg.MaxReservedBytes-d.reserved {
		d.rejected.Add(1)

		return nil, ErrBusy
	}

	selection.Types = slices.Clone(selection.Types)
	sub := &Subscription{
		q:         newQueue(d.cfg.QueueEvents, d.cfg.QueueBytes, d.cfg.QueueAge),
		selection: selection,

		dispatcher: d,
	}

	items := make([]*Subscription, len(current)+1)
	copy(items, current)
	items[len(current)] = sub
	d.calls.Store(selection.CallID, items)

	d.count++
	d.reserved += int64(d.cfg.QueueBytes)

	return sub, nil
}

func (d *Dispatcher) subscribers(callID string) []*Subscription {
	value, _ := d.calls.Load(callID)
	items, _ := value.([]*Subscription)

	return items
}

func (d *Dispatcher) publish(event domain.Event, bytes int) {
	var item *Delivery

	for _, sub := range d.subscribers(event.CallID) {
		if sub.selection.Participant != "" && sub.selection.Participant != event.Participant ||
			len(sub.selection.Types) != 0 && !slices.Contains(sub.selection.Types, event.Type) {
			continue
		}

		if item == nil {
			item = &Delivery{
				Event:      event,
				ReceivedAt: time.Now(),
				Bytes:      bytes,
			}
		}

		sub.q.push(item)
	}
}

func (d *Dispatcher) stop(closed bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.ready = false
	d.closed = closed
	d.calls.Range(func(_, value any) bool {
		items, _ := value.([]*Subscription)
		for _, sub := range items {
			sub.q.close(ErrUnavailable)
		}

		return true
	})
	d.calls.Clear()
}

func (d *Dispatcher) Snapshot() DeliverySnapshot {
	d.mu.Lock()
	defer d.mu.Unlock()

	return DeliverySnapshot{
		Subscriptions: d.count,
		ReservedBytes: d.reserved,
		Rejected:      d.rejected.Load(),
		Failures:      d.failures.Load(),
		Ready:         d.ready && !d.closed,
	}
}

func (s *Subscription) Next(ctx context.Context) (*Delivery, error) { return s.q.Next(ctx) }
func (s *Subscription) Done() <-chan struct{}                       { return s.q.done }
func (s *Subscription) Err() error                                  { return s.q.cause() }

func (s *Subscription) Close() {
	s.once.Do(func() {
		d := s.dispatcher

		d.mu.Lock()
		defer d.mu.Unlock()

		current := d.subscribers(s.selection.CallID)
		if len(current) > 0 {
			items := make([]*Subscription, 0, len(current))
			for _, sub := range current {
				if sub != s {
					items = append(items, sub)
				}
			}

			if len(items) == 0 {
				d.calls.Delete(s.selection.CallID)
			} else {
				d.calls.Store(s.selection.CallID, items)
			}
		}

		d.count--
		d.reserved -= int64(d.cfg.QueueBytes)

		s.q.close(ErrClosed)
	})
}
