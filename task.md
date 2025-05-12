# Digital Intelligence (DI) Signal Orchestrator — Task Plan

## Project Summary

A high-throughput orchestrator that ingests device-level telemetry and behavioral
events, normalizes them into a Redis stream, and feeds a PySpark feature
engineering pipeline that powers downstream fraud-detection models inside a
distributed RiskOS workflow.

**Stack:** Go (collector + API), Redis Streams (transport/buffer),
PySpark (batch + micro-batch feature engineering), OpenAPI (schema contract).

**Primary goals**
1. Sub-50ms ingestion latency at the API edge.
2. Lossless buffering via Redis Streams with consumer groups.
3. Reusable feature set for fraud models, derived from raw interaction data.
4. Versioned API schema that downstream RiskOS services can integrate against.

---

## Architecture (high level)

```
   ┌──────────────┐     HTTP/gRPC     ┌────────────────┐    XADD     ┌───────────────┐
   │  SDK / Edge  │ ────────────────► │ Go Collector   │ ──────────► │ Redis Streams │
   │  (devices)   │                   │ (di-collector) │             │ di:signals:*  │
   └──────────────┘                   └────────────────┘             └───────┬───────┘
                                                                              │ XREADGROUP
                                                                              ▼
                                                                      ┌──────────────┐
                                                                      │   PySpark    │
                                                                      │  (features)  │
                                                                      └──────┬───────┘
                                                                             │
                                                                             ▼
                                                                   Feature store / RiskOS
```

---

## Phase 1 — Signal Collector (Go + Redis)

**Goal:** receive, validate, and durably stream signals.

- [x] 1.1 Bootstrap Go module `di-collector` with `cmd/server` entrypoint.
- [x] 1.2 Define signal schema (`internal/signal/types.go`) for device telemetry
      (OS, IP, fingerprint, sensors) and behavioral events (clicks, dwell time,
      typing cadence). Include `event_id`, `tenant_id`, `session_id`, `ts`.
- [x] 1.3 HTTP ingestion endpoint `POST /v1/signals` with bearer-token auth
      and per-tenant rate limiting.
- [x] 1.4 Validation layer: required fields, max payload size, replay guard
      (event_id deduped via Redis SET with TTL).
- [x] 1.5 Redis Streams writer: `XADD di:signals:{tenant} *` with capped
      length (`MAXLEN ~`) for backpressure protection.
- [x] 1.6 Graceful shutdown, health/readiness probes, structured JSON logs.
- [x] 1.7 Load test target: 10k req/s sustained on a single pod, p99 < 50ms.

**Deliverables:** `cmd/server`, `internal/signal`, `internal/redis`,
`internal/api`, Dockerfile, k6 load script.

---

## Phase 2 — Feature Engineering (PySpark)

**Goal:** turn raw signals into model-ready features.

- [x] 2.1 PySpark job `di_features/stream_consumer.py` that reads from Redis
      Streams via `XREADGROUP` (consumer group `di-features`) and lands raw
      events to Parquet on object storage, partitioned by `tenant_id` / `dt`.
- [x] 2.2 Schema enforcement with PySpark `StructType` matching the Go signal
      contract. Quarantine malformed rows to `bad_records/`.
- [x] 2.3 Sessionization: window events by `session_id` with watermarking
      (10 min) to compute session-level aggregates.
- [x] 2.4 Feature set v1 (fraud-relevant):
        - velocity counts (events / 1m, 5m, 1h),
        - device-IP-fingerprint cardinality per session,
        - typing cadence variance, mouse entropy,
        - geo distance vs. prior session.
- [x] 2.5 Write features to a feature store table (Delta / Iceberg) keyed by
      `tenant_id`, `session_id`, `feature_window`.
- [x] 2.6 Backfill mode: same job runnable as nightly batch for replay.
- [x] 2.7 Unit tests with `pytest` + `chispa` for transformations.

**Deliverables:** `pyspark/di_features/`, `pyspark/tests/`, Spark job manifests.

---

## Phase 3 — API Schema & RiskOS Integration

**Goal:** make the orchestrator a first-class citizen in RiskOS workflows.

- [x] 3.1 Author OpenAPI 3.1 spec (`api/openapi.yaml`) covering ingestion,
      schema versioning header (`X-DI-Schema-Version`), and error model.
- [x] 3.2 Generate client stubs for RiskOS consumers (Go + Python).
- [x] 3.3 RiskOS workflow adapter: a thin service that subscribes to the
      feature store and emits decision-ready payloads on the RiskOS bus.
- [x] 3.4 Observability: Prometheus metrics (ingest rate, drop rate, lag),
      OTel traces from collector → Redis → Spark job span links.
- [x] 3.5 Runbook + schema-evolution policy (additive-only minor versions,
      breaking changes bump major and require dual-write window).
- [x] 3.6 End-to-end test: synthetic SDK → collector → Redis → Spark →
      feature row → RiskOS adapter ack.
- [x] 3.7 Deployment: Helm charts for collector + adapter, Spark job on
      k8s operator, Redis via managed service.

**Deliverables:** `api/openapi.yaml`, `riskos-adapter/`, `deploy/helm/`,
`docs/runbook.md`.

---

## Out of scope (for now)
- Model training / scoring (consumer of features, not produced here).
- SDK clients on device (assumed to exist).
- Multi-region replication of Redis (single-region MVP).
