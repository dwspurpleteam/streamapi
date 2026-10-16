package main

import (
	"net/http"
	"net/http/pprof"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func (a *app) newHTTPServer(c httpConfig) *http.Server {
	mux := http.NewServeMux()

	mux.Handle("POST /webhooks/livekit", a.webhook)

	mux.HandleFunc("GET /live", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("GET /ready/media", func(w http.ResponseWriter, _ *http.Request) {
		if a.media.Snapshot().Closed {
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			w.WriteHeader(http.StatusNoContent)
		}
	})

	mux.HandleFunc("GET /ready/events", func(w http.ResponseWriter, _ *http.Request) {
		if a.publisher.Ready() {
			w.WriteHeader(http.StatusNoContent)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})

	registry := prometheus.NewRegistry()
	registry.MustRegister(a.streams)
	registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	a.registerMetrics(registry)

	mux.Handle("GET /metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{
		MaxRequestsInFlight: c.MetricsConcurrency,
		Timeout:             c.MetricsTimeout,
	}))

	if c.Profiling {
		mux.HandleFunc("GET /debug/pprof/", pprof.Index)
		mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
	}

	timeout := c.Timeout

	return &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: timeout,
		ReadTimeout:       timeout,
		WriteTimeout:      timeout,
		IdleTimeout:       timeout,
		MaxHeaderBytes:    c.MaxHeaderBytes,
	}
}

func (a *app) registerMetrics(r *prometheus.Registry) {
	for _, metric := range []struct {
		name  string
		help  string
		value func() float64
	}{
		{
			"callrelay_media_rooms",
			"Active source rooms",
			func() float64 {
				return float64(a.media.Snapshot().Rooms)
			},
		},
		{
			"callrelay_media_subscriptions",
			"Active media subscriptions",
			func() float64 {
				return float64(a.media.Snapshot().Subscriptions)
			},
		},
		{
			"callrelay_media_reserved_bytes",
			"Bytes reserved for subscriber queues",
			func() float64 {
				return float64(a.media.Snapshot().ReservedBytes)
			},
		},
	} {
		r.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: metric.name,
			Help: metric.help,
		}, metric.value))
	}

	for _, metric := range []struct {
		name  string
		help  string
		value func() float64
	}{
		{
			"callrelay_media_received_total",
			"Media packets received from call sources",
			func() float64 {
				return float64(a.media.Snapshot().Packets)
			},
		},
		{
			"callrelay_media_sent_total",
			"Media packets handed to gRPC",
			func() float64 {
				return float64(a.streams.Snapshot().Sent)
			},
		},
		{
			"callrelay_media_bytes_total",
			"Media bytes handed to gRPC",
			func() float64 {
				return float64(a.streams.Snapshot().Bytes)
			},
		},
		{
			"callrelay_media_slow_total",
			"Slow consumers disconnected",
			func() float64 {
				return float64(a.streams.Snapshot().Slow)
			},
		},
		{
			"callrelay_media_rejected_total",
			"Subscriptions rejected at capacity",
			func() float64 {
				return float64(a.media.Snapshot().Rejected)
			},
		},
		{
			"callrelay_events_published_total",
			"Events acknowledged by JetStream",
			func() float64 {
				return float64(a.events.Snapshot().Accepted)
			},
		},
		{
			"callrelay_events_failed_total",
			"Event publication failures",
			func() float64 {
				return float64(a.events.Snapshot().Failed)
			},
		},
		{
			"callrelay_webhooks_rejected_total",
			"Rejected platform webhooks",
			func() float64 {
				return float64(a.webhook.Snapshot().Rejected)
			},
		},
	} {
		r.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: metric.name,
			Help: metric.help,
		}, metric.value))
	}
}
