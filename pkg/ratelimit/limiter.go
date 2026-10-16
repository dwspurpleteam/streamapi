package ratelimit

import (
	"math"
	"runtime"
	"sync/atomic"
	"time"
)

const cacheIsolationBytes = 128

type Limiter struct {
	shards []bucket
}

type bucket struct {
	rate     float64
	burst    float64
	start    time.Time
	_        [cacheIsolationBytes]byte
	consumed atomic.Uint64
	_        [cacheIsolationBytes]byte
}

func New(rate, burst, quantum int64, now time.Time) *Limiter {
	count := min(int64(runtime.GOMAXPROCS(0)), rate, max(1, burst/quantum))

	b := &Limiter{shards: make([]bucket, count)}
	for i := range b.shards {
		shard := &b.shards[i]
		shard.rate = float64(rate / count)
		shard.burst = float64((burst / quantum / count) * quantum)

		if int64(i) < rate%count {
			shard.rate++
		}

		if int64(i) < (burst/quantum)%count {
			shard.burst += float64(quantum)
		}

		if i == 0 {
			shard.burst += float64(burst % quantum)
		}

		shard.start = now
		shard.consumed.Store(math.Float64bits(-shard.burst))
	}

	return b
}

func (b *Limiter) Allow(units int, now time.Time, index int) bool {
	for range len(b.shards) {
		if b.shards[index].allow(units, now) {
			return true
		}

		index++
		if index == len(b.shards) {
			index = 0
		}
	}

	return false
}

func (b *bucket) allow(units int, now time.Time) bool {
	if units < 0 || float64(units) > b.burst {
		return false
	}

	if units == 0 {
		return true
	}

	available := now.Sub(b.start).Seconds() * b.rate
	floor := available - b.burst

	previous := b.consumed.Load()
	for {
		consumed := math.Float64frombits(previous)
		if consumed > floor && float64(units) > available-consumed {
			return false
		}

		base := max(consumed, floor)

		next := base + float64(units)
		if next-base < float64(units) {
			next = math.Nextafter(next, math.Inf(1))
		}

		if next > available {
			return false
		}

		if b.consumed.CompareAndSwap(previous, math.Float64bits(next)) {
			return true
		}

		previous = b.consumed.Load()
	}
}

func (b *Limiter) Shards() int {
	return len(b.shards)
}
