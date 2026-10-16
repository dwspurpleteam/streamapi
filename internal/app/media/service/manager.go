package service

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dwspurpleteam/streamapi/internal/app/media/domain"
	"github.com/dwspurpleteam/streamapi/pkg/ratelimit"
)

const cacheIsolationBytes = 128

type Source interface {
	Run(
		ctx context.Context,
		call domain.Call,
		emit func(domain.Frame),
		wants func(domain.Track) bool,
		changed <-chan struct{},
	) error
}

type Config struct {
	MaxSubscriptions     int
	MaxPerCall           int
	QueuePackets         int
	QueueBytes           int
	MaxReservedBytes     int64
	QueueAge             time.Duration
	EgressBytesPerSecond int64
	EgressBurstBytes     int64
}

type Manager struct {
	mu         sync.Mutex
	cfg        Config
	source     Source
	rooms      map[string]*room
	count      int
	reserved   int64
	closed     bool
	wg         sync.WaitGroup
	received   uint64
	rejected   atomic.Uint64
	egress     *ratelimit.Limiter
	nextEgress int
}

type room struct {
	readers  atomic.Pointer[subscribers]
	received packetCounter
	call     domain.Call
	subs     map[*Subscription]struct{}
	changed  chan struct{}
	cancel   context.CancelFunc
	closing  bool
}

type subscribers struct {
	items []*Subscription
}

type packetCounter struct {
	value atomic.Uint64
	_     [cacheIsolationBytes]byte
}

type Subscription struct {
	q           *queue
	selection   domain.Selection
	manager     *Manager
	room        *room
	egressIndex int
	once        sync.Once
}

type Snapshot struct {
	Rooms         int
	Subscriptions int
	ReservedBytes int64
	Packets       uint64
	Rejected      uint64
	Closed        bool
}

func New(cfg Config, source Source) *Manager {
	return &Manager{
		cfg:    cfg,
		source: source,
		rooms:  make(map[string]*room),

		egress: ratelimit.New(
			cfg.EgressBytesPerSecond,
			cfg.EgressBurstBytes,
			int64(cfg.QueueBytes),
			time.Now(),
		),
	}
}

func (m *Manager) Subscribe(ctx context.Context, call domain.Call, selection domain.Selection) (*Subscription, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if call.ID == "" || call.Room == "" || len(call.ID) > domain.MaxIdentifierBytes || len(call.Room) > domain.MaxIdentifierBytes ||
		len(selection.Participant) > domain.MaxIdentifierBytes || len(selection.Track) > domain.MaxIdentifierBytes || selection.Kinds&^uint8(1<<domain.Audio|1<<domain.Video) != 0 {
		return nil, ErrInvalid
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil, ErrClosed
	}

	if m.count >= m.cfg.MaxSubscriptions || int64(m.cfg.QueueBytes) > m.cfg.MaxReservedBytes-m.reserved {
		m.rejected.Add(1)

		return nil, ErrLimit
	}

	r := m.rooms[call.ID]
	if r == nil {
		runCtx, cancel := context.WithCancel(context.Background())
		r = &room{
			call:    call,
			subs:    make(map[*Subscription]struct{}),
			changed: make(chan struct{}, 1),
			cancel:  cancel,
		}

		m.rooms[call.ID] = r
		m.wg.Go(func() {
			m.run(runCtx, r)
		})
	}

	if r.closing {
		return nil, ErrSource
	}

	if r.call.Room != call.Room {
		return nil, ErrInvalid
	}

	if len(r.subs) >= m.cfg.MaxPerCall {
		m.rejected.Add(1)

		return nil, ErrLimit
	}

	s := &Subscription{
		q: newQueue(m.cfg.QueuePackets, m.cfg.QueueBytes, m.cfg.QueueAge),

		selection:   selection,
		manager:     m,
		room:        r,
		egressIndex: m.nextEgress,
	}
	m.nextEgress = (m.nextEgress + 1) % m.egress.Shards()

	r.subs[s] = struct{}{}
	r.refresh()

	m.count++
	m.reserved += int64(m.cfg.QueueBytes)

	r.signal()

	return s, nil
}

func (m *Manager) run(ctx context.Context, r *room) {
	err := m.source.Run(
		ctx,
		r.call,
		func(f domain.Frame) {
			m.publish(r, f)
		},
		r.wants,
		r.changed,
	)
	if err == nil || errors.Is(err, context.Canceled) {
		err = ErrClosed
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	r.closing = true

	for s := range r.subs {
		s.q.close(err)
	}

	if m.rooms[r.call.ID] == r {
		m.received += r.received.value.Load()
		delete(m.rooms, r.call.ID)
	}
}

func (m *Manager) publish(r *room, f domain.Frame) {
	if f.Track == nil {
		return
	}

	r.received.value.Add(1)

	readers := r.readers.Load()
	if readers == nil {
		return
	}

	for _, s := range readers.items {
		if s.selection.Matches(*f.Track) {
			s.q.push(f)
		}
	}
}

func (r *room) wants(t domain.Track) bool {
	readers := r.readers.Load()
	if readers == nil {
		return false
	}

	for _, s := range readers.items {
		if s.q.cause() == nil && s.selection.Matches(t) {
			return true
		}
	}

	return false
}

func (r *room) refresh() {
	readers := &subscribers{items: make([]*Subscription, 0, len(r.subs))}
	for sub := range r.subs {
		readers.items = append(readers.items, sub)
	}

	r.readers.Store(readers)
}

func (r *room) signal() {
	select {
	case r.changed <- struct{}{}:
	default:
	}
}

func (s *Subscription) Next(ctx context.Context) (domain.Frame, error) {
	return s.q.next(ctx)
}

func (s *Subscription) Done() <-chan struct{} {
	return s.q.done
}

func (s *Subscription) Err() error {
	return s.q.cause()
}

func (s *Subscription) AllowSend(bytes int) bool {
	return s.manager.egress.Allow(bytes, time.Now(), s.egressIndex)
}

func (s *Subscription) Close() {
	s.once.Do(func() {
		m, r := s.manager, s.room

		m.mu.Lock()
		defer m.mu.Unlock()

		delete(r.subs, s)
		r.refresh()

		m.count--
		m.reserved -= int64(m.cfg.QueueBytes)

		s.q.close(ErrClosed)

		if len(r.subs) == 0 {
			r.closing = true
			r.cancel()
		}

		r.signal()
	})
}

func (m *Manager) Snapshot() Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	packets := m.received
	for _, r := range m.rooms {
		packets += r.received.value.Load()
	}

	return Snapshot{
		Rooms:         len(m.rooms),
		Subscriptions: m.count,
		ReservedBytes: m.reserved,

		Packets:  packets,
		Rejected: m.rejected.Load(),
		Closed:   m.closed,
	}
}

func (m *Manager) Close(ctx context.Context) error {
	m.mu.Lock()

	m.closed = true
	for _, r := range m.rooms {
		r.closing = true
		for s := range r.subs {
			s.q.close(ErrClosed)
		}

		r.cancel()
	}

	m.mu.Unlock()

	done := make(chan struct{})

	go func() {
		m.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
