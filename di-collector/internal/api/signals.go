package api

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/example/di-collector/internal/auth"
	"github.com/example/di-collector/internal/metrics"
	"github.com/example/di-collector/internal/redisx"
	"github.com/example/di-collector/internal/signal"
	"go.opentelemetry.io/otel"
)

const maxBodyBytes = 64 * 1024

type errorBody struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorBody{Error: msg, Code: code})
}

var tracer = otel.Tracer("di-collector/api")

func handleSignals(rdb *redisx.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, span := tracer.Start(r.Context(), "ingest_signal")
		defer span.End()

		tenant, ok := auth.TenantFrom(ctx)
		if !ok {
			writeErr(w, http.StatusUnauthorized, "no_tenant", "tenant context missing")
			return
		}
		start := time.Now()
		defer func() {
			metrics.IngestDuration.WithLabelValues(tenant).Observe(time.Since(start).Seconds())
		}()

		r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
		defer r.Body.Close()

		var s signal.Signal
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&s); err != nil {
			var maxErr *http.MaxBytesError
			if errors.As(err, &maxErr) {
				metrics.IngestTotal.WithLabelValues(tenant, "payload_too_large").Inc()
				writeErr(w, http.StatusRequestEntityTooLarge, "payload_too_large", "payload exceeds 64KB")
				return
			}
			if errors.Is(err, io.EOF) {
				metrics.IngestTotal.WithLabelValues(tenant, "empty_body").Inc()
				writeErr(w, http.StatusBadRequest, "empty_body", "request body is empty")
				return
			}
			metrics.IngestTotal.WithLabelValues(tenant, "invalid_json").Inc()
			writeErr(w, http.StatusBadRequest, "invalid_json", err.Error())
			return
		}

		// Tenant from token wins over a spoofed body field.
		s.TenantID = tenant
		if s.Timestamp.IsZero() {
			s.Timestamp = time.Now().UTC()
		}

		if err := s.Validate(time.Now()); err != nil {
			metrics.IngestTotal.WithLabelValues(tenant, "invalid_signal").Inc()
			writeErr(w, http.StatusBadRequest, "invalid_signal", err.Error())
			return
		}

		w.Header().Set("X-DI-Schema-Version", "1.0.0")

		written, err := rdb.Publish(ctx, &s)
		if err != nil {
			metrics.StreamPublishTotal.WithLabelValues(tenant, "error").Inc()
			metrics.IngestTotal.WithLabelValues(tenant, "publish_failed").Inc()
			slog.Error("publish failed", "err", err, "tenant", tenant, "event_id", s.EventID)
			writeErr(w, http.StatusBadGateway, "publish_failed", "could not buffer signal")
			return
		}
		if !written {
			metrics.DedupHits.WithLabelValues(tenant).Inc()
			metrics.IngestTotal.WithLabelValues(tenant, "duplicate").Inc()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"status":"duplicate"}`))
			return
		}

		metrics.StreamPublishTotal.WithLabelValues(tenant, "ok").Inc()
		metrics.IngestTotal.WithLabelValues(tenant, "accepted").Inc()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"status":"accepted"}`))
	}
}
