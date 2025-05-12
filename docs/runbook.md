# DI Signal Orchestrator — Runbook

## On-call rotation
Pages route to whoever owns the `di-platform` PagerDuty service. Sev-1 SLA:
ack ≤ 15 min, mitigation ≤ 60 min.

## Surface area
- **`di-collector`** — Go ingestion edge. Stateless. Scales horizontally.
- **`di-features-stream-consumer`** — PySpark micro-batch driver. One consumer
  per partition; the consumer group `di-features` distributes Redis pending
  entries.
- **`di-features-backfill`** — Spark batch job (k8s SparkApplication, run on
  demand or nightly via the platform scheduler).
- **`riskos-adapter`** — Go service that publishes feature rows to the RiskOS
  bus (NATS subject `riskos.di.features`).
- **Redis Streams** — `di:signals:{tenant}` per-tenant stream, capped via
  `MAXLEN ~ 1_000_000`.

## Dashboards & alerts
- Collector RED metrics: `di_ingest_total`, `di_ingest_duration_seconds`.
- Stream lag: `di_stream_publish_total{outcome="error"}` and the consumer-group
  `XLEN - XPENDING` on the Redis exporter.
- Adapter throughput: `riskos_adapter_emitted_total` (TBD instrumentation).

Page when:
- p99 ingest latency > 50ms for 5 min on any tenant
- error rate (5xx + publish_failed) > 0.5% for 5 min
- Redis `XLEN` > 500k for any tenant for 10 min (consumer behind)
- Spark consumer pod crash-looping (CrashLoopBackOff > 3)

## Common incidents

### Collector p99 latency spike
1. Check `di_ingest_duration_seconds` by tenant — is it global or one tenant?
2. If single tenant: check rate-limiter drops (`di_rate_limit_dropped_total`).
   A noisy tenant exhausting capacity can starve others on the same pod —
   increase `RPS`/`Burst` in collector deployment or scale replicas.
3. If global: check Redis CPU and `INFO clients`. Slow `XADD` is usually a
   Redis cluster failover or replica lag.

### Redis stream backlog growing
1. Verify consumer pods are healthy (`kubectl get po -l app=di-features`).
2. `XPENDING di:signals:{tenant} di-features` — pending IDs older than 30s
   indicate stuck consumers. `XCLAIM` to reassign or restart pods.
3. If sustained, scale consumer replicas (each new replica picks up a share
   of pending IDs automatically).

### Replay-guard producing too many duplicates
- Look at `di_dedup_hits_total` per tenant. >5% sustained usually means a
  retrying client is reusing `event_id`. Reach out to the tenant; do not
  disable dedup.

### RiskOS adapter falling behind
- Adapter checkpoints to a PVC (`/var/lib/riskos-adapter/checkpoint`). If
  the PVC is unmounted on a pod restart, the adapter restarts at "now" and
  drops in-flight work. Restore from the last good checkpoint backup.

## Rollback procedure
- Collector / adapter: `helm rollback <release> <revision>`. Both services are
  stateless w.r.t. the cluster.
- Spark consumer: roll back the SparkApplication CR. Outstanding Redis
  pending entries are picked up by the new revision.
- Schema breaking change rolled out by mistake: see
  `docs/schema-evolution.md` § "Emergency rollback".

## Escalation
- Redis on-call: managed-data team (#data-platform).
- Spark on k8s: platform team (#platform-infra).
- RiskOS bus: RiskOS oncall.
