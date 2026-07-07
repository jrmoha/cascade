# Runbook: Cassandra node down (the chaos gate)

Kill a Cassandra node under load and watch the cluster keep serving — the proof, not the
assumption, that the replication model of [ADR-0019](../adr/0019-replication-and-consistency-model.md)
delivers the availability we designed for. This is **KAN-39**, the signature moment of Phase 4:
_"I ran load, killed a node, and it didn't blink — RF=3 and `LOCAL_QUORUM` tolerates one loss."_

Assumes the 3-node cluster from [`cassandra-cluster.md`](./cassandra-cluster.md) (`make up`).

## What this proves

The `cascade` keyspace is `NetworkTopologyStrategy` **RF=3**; clients read and write at
**`LOCAL_QUORUM`** (`R=W=2`). Because `R + W = 2 + 2 > 3 = RF`, the read and write replica sets
always overlap, so:

- **One node down → still strongly consistent and available.** A quorum (2 of 3) is still
  reachable, so every read and write succeeds and still sees the latest acknowledged write.
- **Two nodes down → `Unavailable`.** A quorum of 2 can't be formed from 1 reachable replica, so
  the coordinator refuses the operation. This is **not a bug** — it's the CAP boundary we chose
  (consistency over availability past one loss). See [The consistency edge](#the-consistency-edge-a-second-node-down).

## Run the experiment (AC#5)

```bash
./infra/scripts/node-down-chaos.sh
```

Self-contained and repeatable: it needs only a healthy cluster (`make up`), no app stack. It drives
load with **`cassandra-stress`** (bundled in the `cassandra:4.1` image) at `cl=LOCAL_QUORUM` against
its own RF=3 keyspace, and restores the cluster on exit (an `EXIT` trap restarts any stopped nodes
and drops the demo keyspaces). Phases:

| #   | Phase                 | What it shows                                                                                                                             |
| --- | --------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| 0   | Preflight             | cluster is `3× UN` and `cassandra-stress` is present, else it bails                                                                       |
| 1   | Seed                  | `cassandra-stress write` seeds ~20k rows (NTS RF=3) at `LOCAL_QUORUM`                                                                     |
| 2   | **Chaos (AC#1)**      | continuous `mixed` read+write load; `docker stop` node 3 mid-run; assert **`Total errors == 0`**                                          |
| 3   | Sentinel              | write one row at `LOCAL_QUORUM` **while node 3 is down** (coordinator stores a hint for it)                                               |
| 4   | Rejoin                | restart node 3; wait for `3× UN`; give hinted handoff time to replay                                                                      |
| 5   | **Isolate (AC#2/#3)** | stop nodes 1 & 2; read the sentinel from node 3's own replica (catch-up), and show `LOCAL_QUORUM` now returns `Unavailable` (quorum lost) |
| 6   | **Repair**            | restore 1 & 2; `nodetool repair` both keyspaces; confirm node 3 holds the sentinel                                                        |
| 7   | Result                | the summary table below                                                                                                                   |

The one **hard gate** is phase 2: if the load reports any errors the script exits non-zero. Expected
tail:

```
## 7. Result
   1 node down under continuous load (LOCAL_QUORUM) 0 errors  (survives)
   node 3 rejoins                                 ✔ caught up via nodetool repair
   2 nodes down (LOCAL_QUORUM)                    Unavailable (needs 2 replicas, 1 reachable) — the designed CAP limit
   R + W > RF  ⇒  2 + 2 > 3  ⇒  a quorum read/write survives exactly one replica loss.

PASS: one node loss at LOCAL_QUORUM served continuous load with 0 errors. 🎯
```

## What to watch

- **`nodetool status`** — the ring view from a node. `UN` = Up/Normal, `DN` = Down/Normal. After a
  kill, the survivors mark the node `DN` within ~5–20s (see the caveat below). During the run:

  ```bash
  watch -n2 'docker exec cascade-cassandra-1 nodetool status | grep -E "^(UN|DN)"'
  ```

- **Failure detection is not instant.** `docker stop` sends `SIGTERM`, then `SIGKILL` after the 10s
  grace period (Cassandra rarely drains a loaded node in time — you'll see exit code `137`). A
  hard-killed node does **not** gossip "I'm leaving," so peers only mark it `DN` via the
  phi-accrual **failure detector** ~10–20s later. This is why the script gates every assertion on
  _observed_ ring state (`nodetool status` showing the expected `UN`/`DN` counts) rather than on
  `docker stop` returning — do the same when reasoning about a live incident.
- **Hinted handoff** — while a replica is down, each coordinator stores the writes it missed as
  **hints** and replays them when it comes back (`nodetool statushandoff` shows Active/Paused; hints
  live under `/var/lib/cassandra/hints`). Handoff heals **short** outages automatically; replay
  timing varies (in the dev cluster a single hint can lag tens of seconds), so it is best-effort,
  not a guarantee.
- **Read repair** — a `QUORUM`+ read that sees divergent replicas repairs them inline. It only helps
  rows that are actually read.
- **`nodetool repair`** — the anti-entropy backstop that reconciles **everything**, for outages
  longer than the hint window (`max_hint_window_in_ms`, default 3h) or whenever you want certainty.
  The script runs `nodetool repair -full <keyspace>` after the node rejoins and confirms node 3 then
  holds the sentinel — the deterministic catch-up proof (AC#2).
- **`nodetool netstats`** — streaming/repair progress while a repair runs.

## Recovery procedure (kill → observe → recover)

1. **Observe** the loss: `nodetool status` shows the node `DN`. Reads/writes at `LOCAL_QUORUM`
   keep succeeding on the surviving quorum — confirm your error rate stays flat.
2. **Bring the node back**: `docker start cascade-cassandra-<n>` (in production: restart the process
   / instance). Wait for it to rejoin: `nodetool status` returns it to `UN`.
3. **Let it catch up**:
   - **Short outage (< hint window):** hinted handoff replays the missed writes automatically; no
     action needed. Verify with a read.
   - **Longer outage (> hint window, or after any doubt):** run
     `nodetool repair -full <keyspace>` on the recovered node and watch `nodetool netstats`.
4. **Confirm full health**: `nodetool status` shows `3× UN` with balanced ownership.

## The consistency edge: a second node down (AC#3)

With RF=3 + `LOCAL_QUORUM`, a quorum is **2** replicas. Lose one and you still have two — fine.
Lose a **second** and only one replica is reachable, so no quorum can form:

```
Unavailable: Cannot achieve consistency level LOCAL_QUORUM
  info={'consistency': 'LOCAL_QUORUM', 'required_replicas': 2, 'alive_replicas': 1}
```

This is the **designed limit**, not a failure of the design. Cassandra is an **AP / PA-EL** store
that we deliberately tuned to per-DC strong consistency; the trade is that once you drop below a
quorum you keep **consistency** and give up **availability** for that data. Knowing exactly where
tolerance ends — RF=3 tolerates **one** node loss at `LOCAL_QUORUM` — is the point of running this.
To trade the other way in an emergency (availability over consistency), a specific read can be
dropped to `CL=ONE` (see [`cassandra-cluster.md`](./cassandra-cluster.md) → the ONE-vs-QUORUM demo,
and ADR-0019's sanctioned `LOCAL_ONE` relaxation for bounded raw retrieval).

## End-to-end: does the app blink?

The chaos script proves the property at the **Cassandra layer**. The same guarantee carries through
the running stack (`make stack-up`): the **Ingestion-Processor** writes and the **Query API** reads
at `LOCAL_QUORUM` (`services/*/src/cassandra/cassandra.service.ts`, `CASSANDRA_CONSISTENCY`), so
killing one Cassandra node leaves both serving uninterrupted — writes still meet quorum on the other
two replicas. And because ingest is decoupled by Kafka, even a brief full-Cassandra stall wouldn't
**drop** events: they wait durably in `raw-events` until the Ingestion-Processor drains them (offsets
commit only after the durable write — ADR-0016). To watch it live: `make stack-up`, drive a little
traffic (POST `/collect`, GET `/counts`), `docker stop cascade-cassandra-3`, and confirm the
Collector keeps returning `2xx` and the Query API keeps answering.

## Follow-up

- **Toxiproxy** can simulate a **slow / partitioned** node (latency, timeouts) instead of a clean
  kill — a more realistic and instructive failure mode. Deferred from KAN-39; a good next chaos
  experiment to add in front of one node's `9042`.
