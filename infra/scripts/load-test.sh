#!/usr/bin/env bash
#
# KAN-42 / ADR-0021 — ingestion resilience load test.
#
# Brings up the trimmed load stack (infra/load/docker-compose.load.yml), seeds a
# project + API key + schema, runs the k6 spike against POST /collect, then
# reconciles the accepted (202) count against Kafka to prove no accepted data was
# lost. Tears the stack down on exit. Requires: docker compose, node, and k6.
#
# Run locally with `make load-test`; CI runs it in the `load-test` job.
set -euo pipefail

cd "$(dirname "$0")/../.." # repo root

COMPOSE="docker compose -f infra/load/docker-compose.load.yml"

cleanup() {
  echo "== tearing down load stack =="
  $COMPOSE down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "== building + starting the trimmed load stack (waiting for health) =="
$COMPOSE up -d --build --wait --wait-timeout 300

echo "== seeding project + API key + schema =="
API_KEY="$(node infra/load/seed.mjs)"
if [ -z "$API_KEY" ]; then
  echo "seeding failed: no API key returned" >&2
  exit 1
fi

echo "== running k6 ingest spike =="
k6 run -e API_KEY="$API_KEY" -e COLLECTOR_URL="http://localhost:3001" infra/load/ingest-spike.js

echo "== reconciling accepted events against Kafka =="
node infra/load/reconcile.mjs

echo "== load test PASSED =="
