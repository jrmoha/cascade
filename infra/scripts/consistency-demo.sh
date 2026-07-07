#!/usr/bin/env bash
#
# consistency-demo.sh — KAN-38 / ADR-0019
#
# Demonstrates, against the 3-node Cassandra cluster in infra/docker-compose.yml,
# how the SAME read behaves differently at consistency ONE vs QUORUM: latency
# (how many replicas the coordinator waits for) and STALENESS (whether a lagging
# replica can serve an old value). Also prints `nodetool status` (AC#5).
#
# The staleness result is made DETERMINISTIC (not a timing race):
#   1. write v1 at QUORUM with all nodes up,
#   2. disable hinted handoff on nodes 1 & 2 and stop node 3,
#   3. update to v2 at QUORUM (nodes 1 & 2 only — node 3 misses it and, with
#      handoff off, is never sent the hint),
#   4. restart node 3 (still holding v1 — the demo table sets read_repair='NONE'
#      so reads never silently heal it),
#   5. read at ONE *coordinated by node 3* (it answers from its own stale replica)
#      vs read at QUORUM (2/3 agree on v2, fresh).
# Finally it repairs the keyspace and re-enables handoff.
#
# Prereqs: `make up` (the cluster is healthy) and Docker. Usage: ./consistency-demo.sh
set -euo pipefail

KS=consistency_demo
N1=cascade-cassandra-1
N2=cascade-cassandra-2
N3=cascade-cassandra-3

# Run a CQL statement, coordinated by the given node's container.
cql() { docker exec -i "$1" cqlsh -e "$2"; }
# Portable epoch-millis (macOS `date` has no %N; python3 works on both).
now_ms() { python3 -c 'import time; print(int(time.time()*1000))'; }
# Time a CQL statement (wall-clock ms, incl. cqlsh startup — the delta between
# ONE and QUORUM is the signal, not the absolute number). TRACING shows the
# replica count precisely.
timed_cql() {
  local node="$1" stmt="$2" start end
  start=$(now_ms)
  docker exec -i "$node" cqlsh -e "$stmt" >/tmp/cql_out.$$ 2>&1 || true
  end=$(now_ms)
  echo "$((end - start))"
}

# Always leave the cluster as we found it, even if a step fails midway:
# re-enable handoff, repair, drop the demo keyspace.
cleanup() {
  docker start "$N1" "$N2" "$N3" >/dev/null 2>&1 || true
  wait_cql "$N1" || true   # cqlsh must be up before we can drop the keyspace
  docker exec "$N1" nodetool enablehandoff >/dev/null 2>&1 || true
  docker exec "$N2" nodetool enablehandoff >/dev/null 2>&1 || true
  docker exec "$N1" nodetool repair -full "${KS}" >/dev/null 2>&1 || true
  docker exec -i "$N1" cqlsh -e "DROP KEYSPACE IF EXISTS ${KS};" >/dev/null 2>&1 || true
  rm -f /tmp/cql_out.$$
}
trap cleanup EXIT

wait_ring() { # wait until node 1 sees all 3 nodes UN
  local i n
  for i in $(seq 1 60); do
    n=$(docker exec "$N1" nodetool status 2>/dev/null | grep -cE '^UN' || echo 0)
    [ "$n" = "3" ] && return 0
    sleep 3
  done
}

wait_cql() { # wait until the node's native transport (cqlsh) accepts queries
  local node="$1" i
  for i in $(seq 1 60); do
    docker exec -i "$node" cqlsh -e "SELECT now() FROM system.local;" >/dev/null 2>&1 && return 0
    sleep 3
  done
}

echo "=================================================================="
echo " Cassandra consistency demo (ONE vs QUORUM) — ADR-0019"
echo "=================================================================="

echo -e "\n## 1. Cluster status (AC#5): expect 3x UN, ~even ownership"
docker exec "$N1" nodetool status

echo -e "\n## 2. Seed: keyspace RF=3 (NetworkTopologyStrategy), read_repair='NONE'"
cql "$N1" "CREATE KEYSPACE IF NOT EXISTS ${KS} WITH replication =
             {'class':'NetworkTopologyStrategy','datacenter1':3};
           CREATE TABLE IF NOT EXISTS ${KS}.kv (k text PRIMARY KEY, v text)
             WITH read_repair='NONE';
           INSERT INTO ${KS}.kv (k,v) VALUES ('demo','v1');"
echo "wrote demo=v1"

echo -e "\n## 3. Diverge one replica: stop node 3, write v2, discard its hints"
docker exec "$N1" nodetool disablehandoff >/dev/null; docker exec "$N2" nodetool disablehandoff >/dev/null
docker stop "$N3" >/dev/null
cql "$N1" "CONSISTENCY QUORUM; UPDATE ${KS}.kv SET v='v2' WHERE k='demo';"
echo "wrote demo=v2 at QUORUM (nodes 1 & 2 have v2; node 3 is down, still v1)"
# Discard hints the coordinators stored for the down node so hinted handoff can't
# heal node 3 when it returns (read_repair='NONE' already blocks the read path).
sleep 2
docker exec "$N1" nodetool truncatehints >/dev/null 2>&1 || true
docker exec "$N2" nodetool truncatehints >/dev/null 2>&1 || true

echo -e "\n   Availability cost of the strongest level — with node 3 down, CONSISTENCY ALL FAILS:"
cql "$N1" "CONSISTENCY ALL; SELECT v FROM ${KS}.kv WHERE k='demo';" 2>&1 \
  | grep -iE "Unavailable|required_replicas" | head -1 || true

echo -e "\n## 4. Bring node 3 back (it holds v1; nodes 1 & 2 hold v2)"
docker start "$N3" >/dev/null
wait_ring; wait_cql "$N3"; sleep 2

echo -e "\n## 5. With ALL nodes up, CONSISTENCY QUORUM is FRESH (2 of 3 agree on v2)"
val_quorum=$(cql "$N1" "CONSISTENCY QUORUM; SELECT v FROM ${KS}.kv WHERE k='demo';" 2>/dev/null \
  | grep -E "^\s+v[0-9]" | tr -d ' ' | head -1 || true)
echo "   QUORUM read → ${val_quorum:-?}   (read_repair='NONE', so node 3 stays stale)"
echo -e "   TRACING — a QUORUM read contacts 2 replicas:"
cql "$N1" "CONSISTENCY QUORUM; TRACING ON; SELECT v FROM ${KS}.kv WHERE k='demo';" 2>&1 \
  | grep -iE "Processing response from|Request complete" | head -3 || true

echo -e "\n## 6. Isolate the stale replica: stop nodes 1 & 2 (only node 3 is reachable)"
docker stop "$N1" "$N2" >/dev/null
wait_cql "$N3"
echo -e "\n   (a) CONSISTENCY ONE via node 3 → answered from its only (STALE) replica:"
val_one=$(cql "$N3" "CONSISTENCY ONE; SELECT v FROM ${KS}.kv WHERE k='demo';" 2>/dev/null \
  | grep -E "^\s+v[0-9]" | tr -d ' ' | head -1 || true)
echo "   ONE read → ${val_one:-?}   (available, but stale)"
echo -e "\n   (b) CONSISTENCY QUORUM via node 3 → FAILS (needs 2 replicas, only 1 reachable):"
cql "$N3" "CONSISTENCY QUORUM; SELECT v FROM ${KS}.kv WHERE k='demo';" 2>&1 \
  | grep -iE "Unavailable|required_replicas" | head -1 || echo "   (unavailable)"

echo -e "\n## 7. Result"
printf "   %-42s %s\n" "all nodes up · QUORUM (2/3 agree)"          "${val_quorum:-?}  (fresh)"
printf "   %-42s %s\n" "nodes 1&2 down · ONE via stale node 3"      "${val_one:-?}  (stale, but available)"
printf "   %-42s %s\n" "nodes 1&2 down · QUORUM via node 3"         "UNAVAILABLE (chooses consistency over availability)"
echo "   R + W > RF  ⇒  2 + 2 > 3  ⇒  strong: a QUORUM read always overlaps the QUORUM write."
echo "   ONE trades consistency for availability/latency; QUORUM trades availability for consistency."

echo -e "\n## 8. Restore nodes 1 & 2; cleanup (repair + drop ${KS}) runs on the EXIT trap."
docker start "$N1" "$N2" >/dev/null; wait_ring
echo "done."
