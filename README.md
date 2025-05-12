# Digital Intelligence (DI) Signal Orchestrator

Ingests device telemetry and behavioral events, buffers them in Redis Streams,
and produces fraud-detection features via PySpark for downstream RiskOS
workflows.

See [task.md](task.md) for the phased plan.

## Layout

| Path | Phase | Purpose |
| --- | --- | --- |
| [di-collector/](di-collector/) | 1 | Go ingestion service → Redis Streams |
| [pyspark/](pyspark/) | 2 | Feature engineering jobs |
| [api/](api/) | 3 | OpenAPI schema contract |
| [riskos-adapter/](riskos-adapter/) | 3 | Bridge from feature store to RiskOS bus |
| [deploy/helm/](deploy/helm/) | 3 | Kubernetes deployment |
| [loadtest/](loadtest/) | 1 | k6 load scripts |
| [docs/](docs/) | 3 | Runbook + schema policy |

## Quick start (Phase 1)

```bash
cd di-collector
go run ./cmd/server
# in another shell:
curl -X POST localhost:8080/v1/signals \
  -H 'Authorization: Bearer dev-token' \
  -H 'Content-Type: application/json' \
  -d @../testdata/sample_signal.json
```
