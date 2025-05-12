package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	IngestTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "di_ingest_total",
		Help: "Total ingest requests by tenant and outcome.",
	}, []string{"tenant", "outcome"})

	IngestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "di_ingest_duration_seconds",
		Help:    "Ingest request duration.",
		Buckets: []float64{.001, .002, .005, .01, .025, .05, .1, .25, .5, 1},
	}, []string{"tenant"})

	StreamPublishTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "di_stream_publish_total",
		Help: "Redis XADD outcomes.",
	}, []string{"tenant", "outcome"})

	RateLimitDropped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "di_rate_limit_dropped_total",
		Help: "Requests dropped by per-tenant rate limiter.",
	}, []string{"tenant"})

	DedupHits = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "di_dedup_hits_total",
		Help: "Replay-guard hits (duplicate event_id).",
	}, []string{"tenant"})
)
