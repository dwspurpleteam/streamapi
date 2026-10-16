package media

import (
	"math"
	"runtime"
	"sync/atomic"
	"time"
)

const cacheIsolationBytes = 128

type budget struct {
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

func newBudget(rate, burst, packetBytes int64, now time.Time) *budget {
	count := min(int64(runtime.GOMAXPROCS(0)), rate, max(1, burst/packetBytes))

	b := &budget{shards: make([]bucket, count)}
	for i := range b.shards {
		shard := &b.shards[i]
		shard.rate = float64(rate / count)
		shard.burst = float64((burst / packetBytes / count) * packetBytes)

		if int64(i) < rate%count {
			shard.rate++
		}

		if int64(i) < (burst/packetBytes)%count {
			shard.burst += float64(packetBytes)
		}

		if i == 0 {
			shard.burst += float64(burst % packetBytes)
		}

		shard.start = now
		shard.consumed.Store(math.Float64bits(-shard.burst))
	}

	return b
}

func (b *budget) allow(bytes int, now time.Time, index int) bool {
	for range len(b.shards) {
		if b.shards[index].allow(bytes, now) {
			return true
		}

		index++
		if index == len(b.shards) {
			index = 0
		}
	}

	return false
}

func (b *bucket) allow(bytes int, now time.Time) bool {
	if bytes < 0 || float64(bytes) > b.burst {
		return false
	}

	if bytes == 0 {
		return true
	}

	available := now.Sub(b.start).Seconds() * b.rate
	floor := available - b.burst

	previous := b.consumed.Load()
	for {
		consumed := math.Float64frombits(previous)
		if consumed > floor && float64(bytes) > available-consumed {
			return false
		}

		base := max(consumed, floor)

		next := base + float64(bytes)
		if next-base < float64(bytes) {
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
