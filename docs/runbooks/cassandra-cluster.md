# Runbook: Cassandra cluster (replication & consistency)

The write/read store runs as a **3-node cluster** (KAN-38) implementing the replication &
consistency model of [ADR-0019](../adr/0019-replication-and-consistency-model.md). This page
covers bringing it up, verifying the ring, the consistency knob, and the ONE-vs-QUORUM demo.

## Topology

- **3 nodes** in one datacenter (`datacenter1`), defined in
  [`infra/docker-compose.yml`](../../infra/docker-compose.yml) as `cassandra-1` (seed),
  `cassandra-2`, `cassandra-3`. `GossipingPropertyFileSnitch` so the DC/rack are honoured.
- **Keyspace `cascade`:** `NetworkTopologyStrategy`, **RF=3** (`{datacenter1: 3}`). Created by
  the app migrators from `CASSANDRA_LOCAL_DC` + `CASSANDRA_REPLICATION_FACTOR`
  (`services/{ingestion-processor,aggregator}/src/cassandra/migrator.ts`).
- **Consistency:** clients set **`LOCAL_QUORUM`** explicitly (`CASSANDRA_CONSISTENCY`), never the
  driver default `LOCAL_ONE`. With RF=3, `QUORUM = floor(3/2)+1 = 2`, so `R + W = 2 + 2 > 3` ⇒
  **strong consistency** while tolerating **one node down**.

## Bring it up & verify (AC#5)

```bash
make up   # starts the 3 nodes (2 & 3 wait for the seed to be healthy)

# All three Up/Normal with even ownership (~33% each):
docker exec cascade-cassandra-1 nodetool status
# Datacenter: datacenter1
# --  Address     Load    Tokens  Owns (effective)  Host ID  Rack
# UN  172.x.x.2   ...     256     ~33%              ...      rack1
# UN  172.x.x.3   ...     256     ~33%              ...      rack1
# UN  172.x.x.4   ...     256     ~33%              ...      rack1

# Keyspace uses NetworkTopologyStrategy RF=3 (AC#2):
docker exec cascade-cassandra-1 cqlsh -e "DESCRIBE KEYSPACE cascade" | head -3
```

`UN` = Up/Normal. `nodetool ring` shows the token distribution in detail. Bootstrapping three
nodes takes a few minutes on first `up` (each waits for the prior to be healthy so the ring forms
cleanly); watch with `docker compose -f infra/docker-compose.yml logs -f cassandra-2`.

## The consistency knob (AC#3)

Consistency is `CASSANDRA_CONSISTENCY` per service (validated by `cassandraConsistencySchema` in
`@cascade/contracts`), applied as the driver client's default `queryOptions.consistency` — so every
`execute()` runs at that level. `LOCAL_QUORUM` in the cluster; flip it (e.g. `one`) to trade
consistency for latency/availability. `R`/`W` can also be overridden per statement (the demo and
`cqlsh CONSISTENCY <level>;` do exactly that).

The rule: **`R + W > RF ⇒ a read always sees the latest write`** (quorum overlap). Tuning `R`/`W`
against `RF` is the CAP knob — see ADR-0019 for the full CAP/PACELC positioning.

## ONE vs QUORUM demo (AC#4)

```bash
./infra/scripts/consistency-demo.sh
```

The script writes `demo=v1` at QUORUM, then **diverges one replica**: it stops node 3, writes
`demo=v2` at QUORUM (nodes 1 & 2 only) and discards the stored hints so hinted handoff can't heal
node 3 (`read_repair='NONE'` already blocks the read-repair path). Node 3 returns holding the old
`v1`; nodes 1 & 2 hold `v2`. It then reads the same row under three conditions:

| Condition                           | Level    | Result           | Why                                                                |
| ----------------------------------- | -------- | ---------------- | ------------------------------------------------------------------ |
| all nodes up                        | `QUORUM` | `v2` (**fresh**) | 2 of 3 replicas agree on the latest write (`R+W>RF`)               |
| nodes 1 & 2 down, only stale node 3 | `ONE`    | `v1` (**stale**) | the one reachable replica answers — available, but behind          |
| nodes 1 & 2 down, only stale node 3 | `QUORUM` | **UNAVAILABLE**  | needs 2 replicas, only 1 reachable → refuses (consistency > avail) |

Isolating the stale node (stopping 1 & 2) is what makes the stale read **deterministic** — with all
nodes up, the dynamic snitch would route a `ONE` read to a fresh replica. The script also shows
`CONSISTENCY ALL` failing while a node is down, prints a QUORUM read's `TRACING` (it contacts 2
replicas), restores all nodes, and — via an EXIT trap that always runs — re-enables handoff,
repairs, and drops the demo keyspace. The takeaway: `ONE` trades consistency for
availability/latency; `QUORUM` stays consistent because `R+W>RF`, at the cost of needing a quorum
reachable.

## Losing a node

With RF=3 + `LOCAL_QUORUM`, the cluster **keeps serving** reads and writes with one node down
(quorum = 2 of 3). That survival is **proven end-to-end in [KAN-39](https://dash34.atlassian.net/browse/KAN-39)**:
`./infra/scripts/node-down-chaos.sh` drives continuous `cassandra-stress` load, kills a node
mid-run (0 errors), then shows it rejoin and catch up (hinted handoff / repair) and exactly where
tolerance ends (a second node down = `Unavailable`). See the
[**cassandra-node-down.md**](./cassandra-node-down.md) runbook.

## Notes

- **Memory:** three Cassandra nodes plus Kafka/Postgres/Redis is heavy; each node's heap is trimmed
  (`MAX_HEAP_SIZE=512M`). If Docker is memory-constrained, raise its limit or stop the `apps`
  profile while running infra only.
- **Single-node tests:** the Testcontainers integration suites and the smoke test run **one** node
  with `CASSANDRA_REPLICATION_FACTOR=1` / `CASSANDRA_CONSISTENCY=local_quorum` (quorum of RF=1 is 1)
  — the multi-node behaviour is exercised here via compose + the demo, not in CI.
- **Reset:** `docker compose -f infra/docker-compose.yml down -v` drops the per-node volumes
  (`cassandra-{1,2,3}-data`) for a clean ring.
