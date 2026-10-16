package grpc

import (
	"fmt"
	"runtime"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

type histogram struct {
	shards  []prometheus.Histogram
	created time.Time
}

func newHistogram(options prometheus.HistogramOpts) *histogram {
	h := &histogram{
		shards:  make([]prometheus.Histogram, runtime.GOMAXPROCS(0)),
		created: time.Now(),
	}
	for i := range h.shards {
		h.shards[i] = prometheus.NewHistogram(options)
	}

	return h
}

func (h *histogram) observe(index uint64, value float64) {
	h.shards[index%uint64(len(h.shards))].Observe(value)
}

func (h *histogram) Describe(ch chan<- *prometheus.Desc) {
	h.shards[0].Describe(ch)
}

func (h *histogram) Collect(ch chan<- prometheus.Metric) {
	var (
		count  uint64
		sum    float64
		metric dto.Metric
	)

	buckets := make(map[float64]uint64)

	for _, shard := range h.shards {
		metric.Reset()

		if err := shard.Write(&metric); err != nil {
			ch <- prometheus.NewInvalidMetric(shard.Desc(), fmt.Errorf("read histogram shard: %w", err))

			return
		}

		value := metric.GetHistogram()
		count += value.GetSampleCount()
		sum += value.GetSampleSum()

		for _, bucket := range value.GetBucket() {
			buckets[bucket.GetUpperBound()] += bucket.GetCumulativeCount()
		}
	}

	ch <- prometheus.MustNewConstHistogramWithCreatedTimestamp(h.shards[0].Desc(), count, sum, buckets, h.created)
}
