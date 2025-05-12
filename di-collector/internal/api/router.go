package api

import (
	"net/http"

	"github.com/example/di-collector/internal/auth"
	"github.com/example/di-collector/internal/ratelimit"
	"github.com/example/di-collector/internal/redisx"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func NewRouter(rdb *redisx.Client) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", healthz)
	mux.Handle("GET /readyz", readyz(rdb))
	mux.Handle("GET /metrics", promhttp.Handler())

	resolver := auth.StaticResolverFromEnv()
	limiter := ratelimit.NewPerTenant(ratelimit.Config{RPS: 2000, Burst: 4000})

	ingest := http.Handler(handleSignals(rdb))
	ingest = limiter.Middleware(ingest)
	ingest = auth.Middleware(resolver)(ingest)
	ingest = recoverer(ingest)
	ingest = requestLogger(ingest)

	mux.Handle("POST /v1/signals", ingest)
	return mux
}
