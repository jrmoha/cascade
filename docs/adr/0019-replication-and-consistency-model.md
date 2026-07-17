# 0019 — Replication & consistency model (Cassandra + Postgres)

**Status:** Accepted

> The KAN-37 ticket calls this "ADR-0005"; that number was already taken (validate at the
> Collector edge), so the replication/consistency decision is recorded here as ADR-0019.

## Context

KAN-37 opens **Phase 4 (Scaling, Replication & Resilience)**. Everything to date has run on a
single node of each store, which is fine for a walking skeleton but hides the decision that
matters most once there is more than one node: **what happens to a read or a write when a node
is lost.** Before scaling out — multi-node Cassandra (KAN-38), node-loss chaos gate (KAN-39),
Postgres read replica (KAN-41) — the replication topology and per-store consistency levels
must be a **deliberate, recorded choice**, not the driver defaults we happen to inherit.

**Current state (single-node, dev-grade):**

- **Cassandra** — the dev keyspace is created `SimpleStrategy` **RF=1** by the in-app migrators
  (`services/ingestion-processor/src/cassandra/migrator.ts`,
  `services/aggregator/src/cassandra/migrator.ts`), which already note that real environments
  provision the keyspace with `NetworkTopologyStrategy` out-of-band ([ADR-0007](0007-cassandra-raw-events-model.md)
  §consequences). **No client sets a consistency level**, so every read/write runs at the
  cassandra-driver default **`LOCAL_ONE`**. Datacenter is `datacenter1` (default `SimpleSnitch`).
- **Postgres** — a single instance (`infra/docker-compose.yml`) shared by Project/Schema
  (Prisma, [ADR-0011](0011-project-schema-service.md)) and the Aggregator / Query API (raw `pg`,
  [ADR-0017](0017-funnel-and-retention-views.md)). No replica; every read hits the primary.

With RF=1 / `LOCAL_ONE`, losing one node loses data and fails reads — the opposite of what
Phase 4 must prove. The charter names "replication and consistency tradeoffs" and "replicas
teach replication firsthand" as explicit goals (`docs/00-charter.md`), so the model is decided
here and implemented by the later Phase-4 tickets.

This ADR **decides and records**; it changes no `src/` or `infra/` code. KAN-38/41 apply it.

## Decision

### 0. The quorum rule, stated once — `R + W > RF ⇒ strong`

For a quorum-replicated store, if the number of replicas a **read** waits for (`R`) plus the
number a **write** waits for (`W`) exceeds the replication factor (`RF`), then the read and
write replica sets are guaranteed to **overlap on at least one replica** — so a read always
observes the most recent acknowledged write (strong / read-your-writes consistency). Tuning
`R` and `W` against `RF` **is** the CAP knob: raise them for consistency, lower them for
availability and latency. This rule drives both store decisions below.

### 1. Cassandra — `NetworkTopologyStrategy`, RF=3, read/write `LOCAL_QUORUM`

- **Replication:** `NetworkTopologyStrategy` with **RF=3** per datacenter. NTS is datacenter-
  and rack-aware, so replicas are spread across racks (fault domains) and the topology extends
  to multi-region by adding a DC to the map — no re-modelling. RF=3 is the smallest factor that
  keeps a **quorum (2)** available while **one replica is down**, which is exactly the KAN-39
  "survive node loss" gate.
- **Consistency:** reads and writes at **`LOCAL_QUORUM`** → `R=2`, `W=2`, `RF=3`, so
  `R + W = 4 > 3` ⇒ **strong consistency within the datacenter**, tolerant of one node down.
  `LOCAL_QUORUM` (quorum within the local DC only) is chosen **even single-region** so that
  going multi-region is a config change, not a rewrite: the alternative `QUORUM` would block on
  cross-region replicas and pay WAN latency on every request the moment a second DC exists.
- **Per-workload nuance (different data, different needs).** Raw-event **ingest** is an
  idempotent, append-only upsert keyed by `event_id` ([ADR-0007](0007-cassandra-raw-events-model.md)),
  and the durable answers live in the Aggregator read models ([ADR-0001](0001-overall-architecture.md)) —
  so raw ingest could tolerate `LOCAL_ONE`. But the **derived read models** (event counts,
  leaderboard, funnel, retention) are read-after-write sensitive: a dashboard reading a counter
  it just incremented must not see a stale replica. We therefore standardise on **`LOCAL_QUORUM`
  for both reads and writes** across the keyspace for correctness and one mental model, and call
  out the **one sanctioned relaxation**: the bounded raw **retrieval** path (`GET /query`, replay
  /audit/debug — [ADR-0008](0008-raw-event-time-range-read.md)) may read at `LOCAL_ONE`, because
  it is explicitly a best-effort audit read, not an analytics guarantee. Any such relaxation is a
  per-query, documented exception — never the default.
- **Dev keyspace stays `SimpleStrategy` RF=1 / `LOCAL_ONE`.** The in-app migrators keep creating
  a single-node throwaway keyspace for local/test; production provisions the real keyspace
  (`NetworkTopologyStrategy`, RF=3) **out-of-band** (infra/Terraform, KAN-38), as ADR-0007
  already anticipates.

**CAP / PACELC.** Cassandra is an **AP** store — **PA/EL** in PACELC: under a partition it stays
available (**PA**), and in normal operation it favours **latency (EL)**. `LOCAL_QUORUM` deliberately
trades some of that latency/availability back for **strong consistency within a DC**, which suits
this workload: high-volume, append-heavy events and their aggregates, where per-DC quorum is cheap
and correctness of the read models matters.

### 2. Postgres — primary + streaming read-replica, freshness-based read routing

- **Topology:** one **primary** (all writes) plus one or more **streaming (async) read
  replicas** (KAN-41). Physical/streaming replication, not logical.
- **Read routing by freshness need, not blindly:**
  - **Read-after-write / config-critical reads → primary.** Project/Schema key verification and
    per-project event-schema lookups ([ADR-0011](0011-project-schema-service.md), [ADR-0013](0013-collector-ingest-auth-validation-caching.md))
    gate ingest auth; a key just minted or a schema just registered must be immediately visible,
    so these read the **primary** (the short Redis cache in front of them already absorbs most
    load). Writes are the primary's job by definition.
  - **Already-eventually-consistent analytics reads → replica.** The funnel/retention tables the
    Query API reads are **Aggregator-owned, eventually-consistent by construction**
    ([ADR-0017](0017-funnel-and-retention-views.md), [ADR-0018](0018-enforce-cqrs-read-boundary.md)) —
    a read already lags ingestion by the Aggregator's processing latency. Serving them from a
    replica that lags the primary by a bounded amount is consistent with the guarantee we already
    make, and it scales read load off the primary.
- **Lag policy:** replication is asynchronous, so the replica trails the primary. Monitor
  replica lag (e.g. `pg_stat_replication` / `pg_last_xact_replay_timestamp`); route by the
  reader's freshness requirement (above) rather than assuming zero lag; and if a specific read
  ever needs read-your-writes it goes to the primary. Synchronous replication is **not** adopted —
  it would couple write latency/availability to replica health for no benefit to these workloads.

**CAP / PACELC.** A single-primary Postgres is a **CP** store — **PC/EC** in PACELC: it favours
**consistency** over availability under a partition (a lost primary means no writes until
failover), and **consistency over latency** in normal operation. The **replica** adds AP-style
read scaling with **bounded lag**. This split matches the data: relational config wants ACID and
read-your-writes on the primary; derived analytical views tolerate lag on a replica.

### 3. Why this is the right shape for this system

Two stores, two consistency postures, each matched to its data (`R+W>RF` gives Cassandra strong
per-DC reads; single-primary gives Postgres ACID config), and both able to **survive one node
loss** — the property Phase 4 exists to prove. `LOCAL_QUORUM` / per-DC replicas mean multi-region
is later a configuration exercise, not a redesign.

## Alternatives considered

- **RF=1 and/or `CL=ONE` everywhere (today's dev default in production).** No fault tolerance:
  one node loss = data loss and failed reads for the partitions it held. Directly fails the
  KAN-39 node-loss gate. Rejected for any real environment — kept only as the local/test keyspace.
- **`CL=ALL` (or RF-wide quorum) for "maximum safety."** Every replica must respond, so a single
  slow/down node fails the operation — worst-case availability and latency, and it defeats the
  point of replication. Rejected.
- **Plain `QUORUM` instead of `LOCAL_QUORUM`.** Correct single-region, but the day a second DC is
  added every read/write blocks on cross-region replicas and pays WAN latency. `LOCAL_QUORUM`
  gives the same single-region guarantee and makes multi-region config-only. Rejected in favour
  of `LOCAL_QUORUM`.
- **`SimpleStrategy` at scale.** Not rack/DC aware — places replicas without fault-domain or
  region awareness, wrong for multi-node and multi-region. Fine only for the single-node dev
  keyspace. Rejected for production.
- **Single Postgres, no replica.** No read scaling and no read-path survivability if the instance
  is lost; every analytics read competes with config writes on one box. Rejected — the replica is
  the point of KAN-41 (and "replicas teach replication firsthand", per the charter).
- **Synchronous Postgres replication.** Ties primary write latency and availability to replica
  health; unnecessary because config reads that need freshness already go to the primary and
  analytics reads already tolerate lag. Rejected.

## Consequences

- The target topology and levels are now fixed and citable: **Cassandra** `NetworkTopologyStrategy`
  RF=3, read/write `LOCAL_QUORUM` (with `LOCAL_ONE` allowed only for bounded raw retrieval);
  **Postgres** primary + async read replica with freshness-based routing.
- **Implementation is deferred and now has a spec to build to:** KAN-38 provisions the multi-node
  keyspace and sets explicit client consistency levels; KAN-39 proves node-loss survival against
  these levels; KAN-41 stands up the Postgres replica and read routing. Each cites this ADR.
- **Node-loss survival is now proven, not assumed** (delivered in **KAN-39**): under continuous
  `cassandra-stress` load at `LOCAL_QUORUM`, killing one of the three nodes causes **0 errors**
  (quorum = 2 of 3 stays reachable); the node rejoins and catches up via hinted handoff / repair;
  and a **second** node down deterministically returns `Unavailable` — the chosen CAP boundary
  (§1), not a defect. Repeatable via `infra/scripts/node-down-chaos.sh`; see
  `docs/runbooks/cassandra-node-down.md`. The Postgres replica half (§2) remains KAN-41.
- **Clients set consistency explicitly** (delivered in **KAN-38**): all three Cassandra clients now
  set `queryOptions.consistency` from `CASSANDRA_CONSISTENCY` (`local_quorum`) — never the driver
  default `LOCAL_ONE` — and the keyspace is created `NetworkTopologyStrategy` with RF from
  `CASSANDRA_REPLICATION_FACTOR`. The level/RF names are the shared `cassandraConsistencySchema`
  contract in `@cascade/contracts`. See `docs/runbooks/cassandra-cluster.md`.
- **The Postgres replica half (§2) is now delivered** (in **KAN-41**): `infra/docker-compose.yml`
  runs a **primary** (`cascade-postgres-primary`, `:5432`) + one **streaming async read replica**
  (`cascade-postgres-replica`, `:5433`), hand-rolled on the official image (a primary initdb hook
  creates the `replicator` role + pg_hba rule; the replica bootstraps via `pg_basebackup -R` and
  runs as a hot standby). Read routing is by freshness: **Project/Schema (Prisma) and the
  Aggregator stay 100% on the primary**; the **Query API funnel/retention** analytics reads route
  to the replica via a second `pg` pool (`DATABASE_REPLICA_URL`, `PostgresService.replicaQuery`) —
  **unset ⇒ falls back to the primary**, so single-node dev/test and the smoke test are unchanged.
  Replication (write-on-primary → visible-on-replica, lag measurement, and the read-only-standby
  failure boundary) is proven by `infra/scripts/postgres-replication-demo.sh`; manual failover is
  documented (not automated). See `docs/runbooks/postgres-replication.md`.
- No behaviour changes in the original ticket — docs only. `docs/blueprint.md` gains a Replication &
  consistency pointer and `CLAUDE.md` records the policy so Phase-4 work honours it.

This ADR builds on [ADR-0001](0001-overall-architecture.md) (topology), [ADR-0007](0007-cassandra-raw-events-model.md)
(Cassandra model), [ADR-0011](0011-project-schema-service.md) (Postgres/Project-Schema), and
[ADR-0015](0015-read-model-aggregation-strategy.md) / [ADR-0017](0017-funnel-and-retention-views.md)
(read-model stores); it decides their replication/consistency posture without changing their
schemas.
