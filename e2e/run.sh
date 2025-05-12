#!/usr/bin/env bash
# End-to-end harness driver.
#
# Brings up redis + collector + spark consumer + nats + adapter, fires synthetic
# signals into the collector, subscribes to the riskos.di.features NATS subject,
# and asserts a feature payload arrives within the deadline.
#
# Requires: docker compose, jq, nats CLI (or Python with nats-py).

set -euo pipefail
cd "$(dirname "$0")"

DEADLINE_SEC="${DEADLINE_SEC:-120}"

cleanup() { docker compose down -v >/dev/null 2>&1 || true; }
trap cleanup EXIT

echo "==> bringing up stack"
docker compose up -d --build redis nats collector features adapter

echo "==> waiting for collector readiness"
for i in $(seq 1 30); do
  if curl -sf localhost:8080/readyz >/dev/null; then break; fi
  sleep 1
done

echo "==> firing 50 synthetic signals"
for i in $(seq 1 50); do
  curl -sS -X POST localhost:8080/v1/signals \
    -H 'Authorization: Bearer tokenA' \
    -H 'Content-Type: application/json' \
    -d "$(python3 ./signal_factory.py "$i")" \
    -o /dev/null
done

echo "==> subscribing to riskos.di.features (deadline ${DEADLINE_SEC}s)"
timeout "${DEADLINE_SEC}" docker run --rm --network e2e_default \
  natsio/nats-box:latest nats sub -s nats://nats:4222 'riskos.di.features' \
  --count 1 || {
    echo "FAIL: no payload arrived within deadline"
    docker compose logs --tail=200
    exit 1
  }

echo "PASS: e2e signal -> collector -> redis -> spark -> feature store -> adapter -> bus"
