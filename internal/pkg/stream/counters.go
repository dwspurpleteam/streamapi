package stream

import (
	"sync"
	"sync/atomic"
)

const cacheIsolationBytes = 128

type Sender struct {
	Histogram uint64
	Sent      atomic.Uint64
	Bytes     atomic.Uint64
	Sending   atomic.Int64
	_         [cacheIsolationBytes]byte
}

type Counters struct {
	mu            sync.Mutex
	active        map[*Sender]struct{}
	nextHistogram uint64
	sent          uint64
	bytes         uint64
}

type Snapshot struct {
	Sent  uint64
	Bytes uint64
}

func (s *Counters) Start() *Sender {
	counters := &Sender{}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.active == nil {
		s.active = make(map[*Sender]struct{})
	}

	counters.Histogram = s.nextHistogram
	s.nextHistogram++
	s.active[counters] = struct{}{}

	return counters
}

func (s *Counters) Stop(counters *Sender) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.sent += counters.Sent.Load()
	s.bytes += counters.Bytes.Load()

	delete(s.active, counters)
}

func (s *Counters) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := Snapshot{
		Sent:  s.sent,
		Bytes: s.bytes,
	}
	for counters := range s.active {
		result.Sent += counters.Sent.Load()
		result.Bytes += counters.Bytes.Load()
	}

	return result
}
