package grpc

import (
	"sync"
	"sync/atomic"
)

const cacheIsolationBytes = 128

type deliveryCounters struct {
	histogram uint64
	sent      atomic.Uint64
	bytes     atomic.Uint64
	sending   atomic.Int64
	_         [cacheIsolationBytes]byte
}

type counterSet struct {
	mu            sync.Mutex
	active        map[*deliveryCounters]struct{}
	nextHistogram uint64
	sent          uint64
	bytes         uint64
}

type Snapshot struct {
	Sent  uint64
	Bytes uint64
	Slow  uint64
}

func (s *counterSet) start() *deliveryCounters {
	counters := &deliveryCounters{}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.active == nil {
		s.active = make(map[*deliveryCounters]struct{})
	}

	counters.histogram = s.nextHistogram
	s.nextHistogram++
	s.active[counters] = struct{}{}

	return counters
}

func (s *counterSet) stop(counters *deliveryCounters) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.sent += counters.sent.Load()
	s.bytes += counters.bytes.Load()

	delete(s.active, counters)
}

func (s *counterSet) snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := Snapshot{
		Sent:  s.sent,
		Bytes: s.bytes,
	}
	for counters := range s.active {
		result.Sent += counters.sent.Load()
		result.Bytes += counters.bytes.Load()
	}

	return result
}
