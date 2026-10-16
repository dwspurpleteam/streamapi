package grpc

import "sync/atomic"

const cacheIsolationBytes = 128

type deliveryCounters struct {
	histogram uint64
	sent      atomic.Uint64
	bytes     atomic.Uint64
	sending   atomic.Int64
	_         [cacheIsolationBytes]byte
}

func (s *Server) startCounters() *deliveryCounters {
	counters := &deliveryCounters{}

	s.countersMu.Lock()
	counters.histogram = s.nextHistogram
	s.nextHistogram++
	s.counters[counters] = struct{}{}
	s.countersMu.Unlock()

	return counters
}

func (s *Server) stopCounters(counters *deliveryCounters) {
	s.countersMu.Lock()
	defer s.countersMu.Unlock()

	s.sent += counters.sent.Load()
	s.bytes += counters.bytes.Load()

	delete(s.counters, counters)
}
