# Runbook: Postgres primary + streaming read replica

Delivers the Postgres half of [ADR-0019 §2](../adr/0019-replication-and-consistency-model.md)
(**KAN-41**). Cascade's Postgres now runs as a **single primary + one streaming (async) read
replica**. All writes go to the primary; the replica is a hot standby that trails by a bounded
lag and serves the Query API's eventually-consistent analytics reads (funnel, retention). This
is a different replication model from Cassandra's masterless quorum replicas (KAN-38/39) — and
feeling both is the point.

> **Honest scope.** Project/Schema is low-volume, so the replica here is about learning the
> pattern (streaming replication + lag as a handled concern), not relieving real load.

## Topology

| Role    | Container                  | Host port | Internal                | Notes                                                     |
| ------- | -------------------------- | --------- | ----------------------- | --------------------------------------------------------- |
| Primary | `cascade-postgres-primary` | `5432`    | `postgres-primary:5432` | all writes; `wal_level=replica`, `max_wal_senders=10`     |
| Replica | `cascade-postgres-replica` | `5433`    | `postgres-replica:5432` | hot standby (`pg_is_in_recovery()=t`); physical streaming |

Hand-rolled on the official `postgres:16-alpine` image (same ethos as the hand-rolled Cassandra
cluster / Kafka KRaft):

- **Primary** — `infra/postgres/primary/10-init-replication.sh` runs once on first boot (initdb
  hook): it creates a `replicator` REPLICATION role and appends a `host replication replicator
0.0.0.0/0 scram-sha-256` rule to `pg_hba.conf`.
- **Replica** — `infra/postgres/replica/entrypoint.sh` runs on every boot: on an empty data dir
  it `pg_basebackup`s from the primary (`-R` writes `standby.signal`), then hands off to the
  stock entrypoint to run as a hot standby. Idempotent — an existing data dir just resumes
  streaming.

## Bring-up & verify

```bash
make up          # starts both instances (replica waits for a healthy primary)
```

Confirm the primary sees a streaming replica:

```bash
docker exec cascade-postgres-primary \
  psql -U cascade -d cascade -c "SELECT application_name, state, sync_state FROM pg_stat_replication;"
#  application_name | state     | sync_state
# ------------------+-----------+------------
#  replica_1        | streaming | async
```

Confirm the replica is a standby:

```bash
docker exec cascade-postgres-replica \
  psql -U cascade -d cascade -tAc "SELECT pg_is_in_recovery();"   # → t
```

## The demo (AC#2–#5)

```bash
make pg-replication-demo         # or: bash infra/scripts/postgres-replication-demo.sh
```

It writes a row on the primary, reads-your-writes on the primary, polls the **replica** until
the row appears (printing the measured lag), and shows the replica **rejecting a write**:

| Step             | Where   | Result                                                           |
| ---------------- | ------- | ---------------------------------------------------------------- |
| write a row      | primary | ok                                                               |
| read-your-writes | primary | immediate                                                        |
| same row visible | replica | yes, after bounded lag (AC#5)                                    |
| attempt a write  | replica | `ERROR: cannot execute INSERT in a read-only transaction` (AC#4) |

The script exits non-zero if the row never replicates.

## Lag policy — route by freshness need (ADR-0019 §2)

Replication is **asynchronous**, so the replica trails the primary. We route reads by the
reader's freshness requirement rather than assuming zero lag:

- **Config-critical / read-your-writes → primary.** Project/Schema key verification and
  per-project schema lookups gate ingest auth (a key just minted or a schema just registered
  must be immediately visible). Project/Schema (Prisma) and the Aggregator (write-only) stay
  **100% on the primary**.
- **Eventually-consistent analytics → replica.** The Query API funnel/retention reads already
  lag ingestion by the Aggregator's processing latency, so serving them from a bounded-lag
  replica is consistent with the guarantee we already make. Wired via
  `DATABASE_REPLICA_URL` (see `services/query-api`); **unset ⇒ reads fall back to the primary**,
  so single-node dev/test and the smoke test run unchanged.

Monitor lag:

```bash
# bytes the replica trails by (0 = caught up)
docker exec cascade-postgres-primary psql -U cascade -d cascade -tAc \
  "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) FROM pg_stat_replication;"
# time the replica trails by
docker exec cascade-postgres-replica psql -U cascade -d cascade -tAc \
  "SELECT now() - pg_last_xact_replay_timestamp();"
```

Synchronous replication is **not** adopted (ADR-0019 §2 / Alternatives): it would tie primary
write latency/availability to replica health for no benefit to these workloads.

## Failover (documented; auto-failover out of scope)

A single-primary Postgres is **CP** — if the primary is lost, **writes stop until a replica is
promoted**; the replica can still serve (stale, bounded-lag) reads throughout. For a learning
project we document the manual path rather than build orchestration:

```bash
# Promote the replica to a writable primary (manual):
docker exec cascade-postgres-replica pg_ctl promote -D /var/lib/postgresql/data
# pg_is_in_recovery() flips to f; the (former) replica now accepts writes.
```

After a real promotion you would re-point writers' `DATABASE_URL` at the new primary and rebuild
the old primary as a fresh standby (`pg_basebackup`). Automatic failover (Patroni / repmgr /
managed Postgres) is deliberately out of scope here.

## Gotchas

- **First boot is slower / heavier:** the replica base-backups the primary before it reports
  healthy (staggered `start_period`). `make up` now runs two Postgres instances — trim Docker
  RAM if needed.
- **The replica is read-only.** Any write against `:5433` errors with _"cannot execute … in a
  read-only transaction"_ — that's the standby working as designed, not a bug.
- **Replica reset:** `make down-v` drops both data volumes; the replica re-runs `pg_basebackup`
  on the next `make up`. A wiped-but-not-recreated replica volume also re-bootstraps (the
  entrypoint keys on an empty data dir).
- **Password persistence:** `pg_basebackup -R` does not embed the replication password in
  `primary_conninfo`; the entrypoint appends a `primary_conninfo` line (with the password) to
  `postgresql.auto.conf` in the data volume so the walreceiver reconnects after restarts.

## Follow-up

- Sync/quorum-commit replication, replica read-consistency SLAs, and automated failover are all
  intentionally excluded (ADR-0019 §2).
- If a future high-volume relational read appears, it is a candidate for replica routing via the
  same `DATABASE_REPLICA_URL` pattern; today only the Query API analytics reads use it.

See [ADR-0019](../adr/0019-replication-and-consistency-model.md),
[`docs/read-models/funnel.md`](../read-models/funnel.md),
[`docs/read-models/retention.md`](../read-models/retention.md).
