# Runbook: ingestion load test (k6 spike)

Delivers the AC#5 evidence for [ADR-0021](../adr/0021-ingestion-resilience.md) (**KAN-42**): a k6
spike against the Collector's `POST /collect` that proves the write edge stays **responsive** under
load and never loses **accepted** data. It doubles as a CI gate (the `load-test` job in
`.github/workflows/ci.yml`) and a local command (`make load-test`).

> **Scope.** This targets the Collector write edge, so it runs a **trimmed** single-node stack
> (`infra/load/docker-compose.load.yml`): Collector + Kafka + Redis + Project/Schema + Postgres.
> No Cassandra, Ingestion-Processor, Query API, Aggregator or replica — the spike produces to
> `raw-events` and reconciliation reads the topic's offsets directly.

## What it does

`infra/scripts/load-test.sh` (invoked by `make load-test` and CI):

1. builds + starts the trimmed stack and waits for every container to be healthy;
2. seeds a project, an API key and the `level_complete` schema via Project/Schema's REST API
   (`infra/load/seed.mjs`, prints the key);
3. runs the k6 spike (`infra/load/ingest-spike.js`) — ramps to 200 VUs, posting valid events;
4. reconciles accepted events against Kafka (`infra/load/reconcile.mjs`);
5. tears the stack down (an `EXIT` trap, always).

## Run it

```bash
make load-test        # or: bash infra/scripts/load-test.sh
```

Requires Docker, Node, and **k6** on `PATH` (https://k6.io/docs/get-started/installation/). CI
installs a pinned k6 binary in the `load-test` job.

## What it asserts

- **Responsiveness** — k6 thresholds: `http_req_duration` p95 `< 800ms` and `http_req_failed` rate
  `< 1%`. Crucially, `429` (rate-limited) and `503` (backpressure / retries exhausted) are declared
  **expected** statuses, not failures — graceful shedding is the correct behaviour under overload,
  and only 5xx-other / network errors count against the failure threshold.
- **No accepted data lost** — k6 writes the count of `202`s to `infra/load/summary.json`;
  `reconcile.mjs` sums the `raw-events` end offsets and asserts **produced ≥ accepted**:

  | Result             | Meaning                                                                                                          |
  | ------------------ | ---------------------------------------------------------------------------------------------------------------- |
  | produced < accept  | **DATA LOSS** — script exits non-zero, the gate fails.                                                           |
  | produced == accept | Clean.                                                                                                           |
  | produced > accept  | Duplicates from at-least-once produce retry — acceptable (downstream is idempotent, ADR-0016); logged as a note. |

## Watching the resilience paths engage

The load compose sets generous limits (`RATE_LIMIT_*`, `PRODUCE_MAX_INFLIGHT`) so the spike exercises
throughput rather than the limiter. To _watch shedding_, tune them down in
`infra/load/docker-compose.load.yml` and re-run — you'll see `429`/`503` responses climb while the
reconciliation still reports no loss (shed requests were never accepted). The shedding paths
themselves are also covered deterministically by the unit + integration tests
(`services/collector/test/{rate-limit,in-flight-limiter,collector.service}.spec.ts`,
`collect.e2e-spec.ts`).

See [ADR-0021](../adr/0021-ingestion-resilience.md) and the [Collector runbook](collector.md).
