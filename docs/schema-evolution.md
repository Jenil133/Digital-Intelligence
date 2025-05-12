# Schema evolution policy

Schema version is independent of API version and is advertised on every
response via `X-DI-Schema-Version`. The wire schema lives in
`api/openapi.yaml` and is mirrored by:
- Go: `di-collector/internal/signal/types.go`
- PySpark: `pyspark/di_features/schema.py`

## Versioning rules (semver)
- **PATCH** (`1.0.0 → 1.0.1`): doc-only or constraint relaxation. No code change.
- **MINOR** (`1.0.x → 1.1.0`): **additive only**.
  - New optional fields.
  - New enum values that consumers must tolerate via "unknown → ignored".
  - New optional behavioral event kinds.
- **MAJOR** (`1.x → 2.0.0`): breaking change.
  - Removing or renaming a field.
  - Tightening a constraint that was previously satisfied by valid producers.
  - Changing field type.
  - Changing the meaning of an existing field.

## Dual-write window for major bumps
Breaking changes require a **dual-write window** of ≥ 14 days during which:
1. Producers send both schemas in parallel (or the collector translates v1→v2).
2. Consumers (PySpark + adapter) read both schemas with a unified projection.
3. Dashboards track per-version traffic share. Cut over only when v1 < 1%.

## Deprecation
1. Mark the field `deprecated: true` in OpenAPI.
2. Emit `Deprecation` and `Sunset` HTTP headers per RFC 8594 / draft-ietf-httpapi-deprecation.
3. Wait at least one minor cycle before removal in a major bump.

## Forbidden transitions
- A previously required field becoming optional in MINOR (it must be MAJOR — old consumers may rely on presence).
- Reusing a removed field name with new semantics within 12 months.

## Emergency rollback
If a schema change causes downstream breakage:
1. Revert the OpenAPI spec (`git revert <commit>`) and redeploy the collector.
2. Bounce the PySpark consumer to pick up the prior `SIGNAL` StructType.
3. Quarantined records (`bad_records/`) accumulated during the bad window
   should be replayed via `di_features.backfill` after the schema is healthy.
