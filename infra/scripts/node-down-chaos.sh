#!/usr/bin/env bash
#
# node-down-chaos.sh — KAN-39 / ADR-0019 (the chaos gate)
#
# Proves the property KAN-38 built and ADR-0019 decided: with the keyspace at
# NetworkTopologyStrategy RF=3 and clients at LOCAL_QUORUM (R=W=2, R+W>RF), the
# 3-node cluster KEEPS SERVING reads and writes while ONE node is down — and
# fails deterministically only when a SECOND node dies (quorum lost = the chosen
# CAP boundary).
#
# Experiment (kill -> observe -> recover), all coordinated so it's repeatable:
#   1. Seed data (cassandra-stress write, RF=3, cl=LOCAL_QUORUM).
#   2. AC#1: run continuous mixed read+write load; mid-run stop node 3 (docker
#      stop -> SIGKILL after grace). Assert the load saw ZERO errors -> one loss
#      at LOCAL_QUORUM doesn't blink.
#   3. Write a sentinel row while node 3 is down (coordinators store a HINT for it).
#   4. Restart node 3; give hinted handoff time to replay the missed write.
#   5. Isolate node 3 (stop 1 & 2, waiting until node 3 OBSERVES the isolation):
#        AC#2 — read the sentinel from node 3's own replica (did it catch up?);
#        AC#3 — a LOCAL_QUORUM read now FAILS (Unavailable) — quorum lost.
#   6. Restore, `nodetool repair` (the "after a longer outage" heal) and, if
#      handoff hadn't replayed, prove repair reconciled node 3. End at 3x UN.
#
# The EXIT trap always restores the cluster (restart nodes, drop the demo keyspaces).
#
# Prereqs: `make up` (3-node cluster healthy) + Docker. Usage: ./node-down-chaos.sh
# Docs: docs/runbooks/cassandra-node-down.md
set -euo pipefail

# Shared helpers (cql, wait_ring, wait_cql) + the N1/N2/N3 container names.
# shellcheck source=infra/scripts/cassandra-lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/cassandra-lib.sh"

STRESS=/opt/cassandra/tools/bin/cassandra-stress
STRESS_KS=chaos_stress      # cassandra-stress standard1 table lives here (RF=3)
SENTINEL_KS=chaos_sentinel  # tiny kv table for the hinted-handoff proof
ROWS=20000                  # rows to seed / read range
KILL_AFTER=12               # seconds of load before we stop node 3
DURATION=40                 # total seconds of mixed load (spans the kill)
HINT_WAIT=30                # seconds after rejoin for hinted handoff to replay
STRESS_OUT=/tmp/chaos_stress.$$

# Result placeholders (filled as phases run; printed in the summary).
stress_errors="?"; handoff_result="?"; quorum_lost_result="?"

cleanup() {
  docker start "$N1" "$N2" "$N3" >/dev/null 2>&1 || true
  wait_cql "$N1" || true
  docker exec "$N1" nodetool enablehandoff >/dev/null 2>&1 || true
  docker exec "$N1" cqlsh -e "DROP KEYSPACE IF EXISTS ${STRESS_KS};" >/dev/null 2>&1 || true
  docker exec "$N1" cqlsh -e "DROP KEYSPACE IF EXISTS ${SENTINEL_KS};" >/dev/null 2>&1 || true
  rm -f "$STRESS_OUT"
}
trap cleanup EXIT

# cassandra-stress, coordinated inside node 1, contacting nodes 1 & 2 (which stay
# up) — the driver discovers node 3 from the ring and reroutes when it dies.
stress() { docker exec "$N1" "$STRESS" "$@" -node "$N1,$N2" -rate threads=8; }

# Wait until the given node OBSERVES exactly $2 peers Up/Normal in its own ring
# view. A SIGKILL'd node (docker stop exceeding the grace period) doesn't gossip
# "leaving", so peers only mark it down via the failure detector ~10-20s later —
# gate assertions on this, not on `docker stop` returning.
wait_down() {
  local node="$1" want="$2" up
  for _ in $(seq 1 30); do
    up=$(docker exec "$node" nodetool status 2>/dev/null | grep -cE '^UN' || echo 0)
    [ "$up" = "$want" ] && return 0
    sleep 2
  done
  return 1
}

# Read the sentinel from node 3's OWN replica (CL=ONE). Meaningful only while
# nodes 1 & 2 are down, so the single reachable replica answering IS node 3.
# Echoes the value, or empty if node 3 doesn't hold it. Polls a few times.
read_sentinel_local() {
  local v=""
  for _ in $(seq 1 5); do
    v=$(cql "$N3" "CONSISTENCY ONE; SELECT v FROM ${SENTINEL_KS}.kv WHERE k='sentinel';" 2>/dev/null \
      | grep -E 'written-while' | tr -d ' ' || true)
    [ -n "$v" ] && break
    sleep 3
  done
  echo "$v"
}

echo "=================================================================="
echo " Cassandra node-loss chaos gate (RF=3, LOCAL_QUORUM) — ADR-0019/KAN-39"
echo "=================================================================="

echo -e "\n## 0. Preflight: cluster healthy (3x UN) and cassandra-stress present"
if ! docker exec "$N1" "$STRESS" version >/dev/null 2>&1; then
  echo "ERROR: cassandra-stress not found at $STRESS in $N1. Is the cluster up (\`make up\`)?" >&2
  exit 1
fi
if ! wait_ring "$N1"; then
  echo "ERROR: cluster is not 3x UN. Bring it up with \`make up\` and retry" >&2
  docker exec "$N1" nodetool status || true
  exit 1
fi
docker exec "$N1" nodetool status

echo -e "\n## 1. Seed data at LOCAL_QUORUM (keyspace ${STRESS_KS}, NTS RF=3)"
stress write "n=${ROWS}" cl=LOCAL_QUORUM -pop "seq=1..${ROWS}" \
  -schema "replication(strategy=NetworkTopologyStrategy,datacenter1=3)" "keyspace=${STRESS_KS}" \
  >/dev/null
cql "$N1" "CREATE KEYSPACE IF NOT EXISTS ${SENTINEL_KS} WITH replication =
             {'class':'NetworkTopologyStrategy','datacenter1':3};
           CREATE TABLE IF NOT EXISTS ${SENTINEL_KS}.kv (k text PRIMARY KEY, v text);"
echo "seeded ${ROWS} rows; sentinel keyspace ready"

echo -e "\n## 2. Continuous mixed load; stop node 3 mid-run (AC#1: expect 0 errors)"
stress mixed "ratio(write=1,read=1)" "duration=${DURATION}s" cl=LOCAL_QUORUM \
  -pop "dist=uniform(1..${ROWS})" -schema "keyspace=${STRESS_KS}" >"$STRESS_OUT" 2>&1 &
stress_pid=$!
sleep "$KILL_AFTER"
echo "   -> stopping ${N3} (docker stop: SIGTERM, then SIGKILL after the grace period)"
docker stop "$N3" >/dev/null
echo "   waiting for the surviving nodes to mark ${N3} DOWN..."
wait_down "$N1" 2 || true
echo "   nodetool status mid-outage (expect ${N3} = DN):"
docker exec "$N1" nodetool status | grep -E '^(UN|DN)' || true
if wait "$stress_pid"; then :; fi   # let the load run to completion
# Parse the "Total errors" summary line in one awk pass (no `grep | head`, which
# would SIGPIPE-fail under `set -o pipefail`).
stress_errors=$(awk '/Total errors/{for(i=1;i<=NF;i++) if($i ~ /^[0-9]+$/){print $i; exit}}' "$STRESS_OUT" || true)
[ -z "$stress_errors" ] && stress_errors="?"
echo "   load finished with node 3 down. Total errors reported: ${stress_errors}"

echo -e "\n## 3. Write a sentinel at LOCAL_QUORUM while node 3 is down (coordinator hints it)"
cql "$N1" "CONSISTENCY LOCAL_QUORUM;
           INSERT INTO ${SENTINEL_KS}.kv (k,v) VALUES ('sentinel','written-while-node3-down');"
echo "   wrote sentinel (nodes 1 & 2 hold it; node 3 owes a hint)"

echo -e "\n## 4. Bring node 3 back; give hinted handoff time to replay (${HINT_WAIT}s)"
docker start "$N3" >/dev/null
wait_ring "$N1"; wait_cql "$N3"
sleep "$HINT_WAIT"
docker exec "$N1" nodetool status | grep -E '^(UN|DN)' || true

echo -e "\n## 5. Isolate node 3 (stop 1 & 2) — prove catch-up AND that quorum is lost"
docker stop "$N1" "$N2" >/dev/null
# Gate on OBSERVED state: wait until node 3 sees only itself up (2 down), so the
# assertions below are deterministic and not racing the failure detector.
if wait_down "$N3" 1; then
  echo "   node 3 now sees only itself Up (2 nodes DOWN):"
else
  echo "   WARNING: node 3 still sees peers Up after waiting — results may be unreliable"
fi
docker exec "$N3" nodetool status | grep -E '^(UN|DN)' || true
# (a) AC#2 — read the sentinel from node 3's OWN replica (CL=ONE, only node 3 up).
#     If present, hinted handoff already replayed the missed write to node 3.
sentinel_val=$(read_sentinel_local)
need_repair_proof=0
if [ -n "$sentinel_val" ]; then
  handoff_result="✔ caught up via hinted handoff (held the missed write)"
  echo "   (a) node 3 local read -> '${sentinel_val}'  ✔ hinted handoff healed it"
else
  need_repair_proof=1
  echo "   (a) node 3 local read empty — handoff not replayed yet; repair (step 6) will heal it"
fi
# (b) AC#3 — a LOCAL_QUORUM read now needs 2 replicas but only node 3 is up.
# Capture first: cqlsh exits non-zero on the Unavailable error, and under
# `set -o pipefail` a `cql ... | grep` pipeline would report cqlsh's status, not
# grep's match — so the assertion must run on the captured text.
lq_read=$(cql "$N3" "CONSISTENCY LOCAL_QUORUM; SELECT v FROM ${SENTINEL_KS}.kv WHERE k='sentinel';" 2>&1 || true)
if echo "$lq_read" | grep -iqE "Unavailable|Cannot achieve consistency"; then
  quorum_lost_result="Unavailable (needs 2 replicas, 1 reachable) — the designed CAP limit"
  echo "   (b) LOCAL_QUORUM read -> Unavailable  ✔ quorum lost = designed limit (2nd node down)"
else
  quorum_lost_result="unexpected: read did not fail with only 1 node up"
  echo "   (b) WARNING: expected Unavailable with only node 3 up"
fi
docker start "$N1" "$N2" >/dev/null
wait_ring "$N1"

echo -e "\n## 6. Repair (the 'after a longer outage' heal path) + confirm catch-up"
docker exec "$N1" nodetool repair -full "${SENTINEL_KS}" >/dev/null 2>&1 || true
docker exec "$N1" nodetool repair -full "${STRESS_KS}" >/dev/null 2>&1 || true
echo "   nodetool repair done on both keyspaces"
if [ "$need_repair_proof" = "1" ]; then
  # Handoff hadn't replayed in time; prove repair reconciled node 3 by isolating
  # it once more and reading its own replica.
  docker stop "$N1" "$N2" >/dev/null
  wait_down "$N3" 1 || true
  if [ -n "$(read_sentinel_local)" ]; then
    handoff_result="✔ caught up via nodetool repair"
    echo "   post-repair node 3 local read -> present  ✔ repair reconciled the missed write"
  else
    handoff_result="⚠ node 3 still missing the write after repair (investigate)"
    echo "   WARNING: node 3 still lacks the sentinel after repair"
  fi
  docker start "$N1" "$N2" >/dev/null
  wait_ring "$N1"
fi
echo "   cluster status:"
docker exec "$N1" nodetool status | grep -E '^(UN|DN)' || true

echo -e "\n## 7. Result"
printf "   %-46s %s\n" "1 node down under continuous load (LOCAL_QUORUM)" "${stress_errors} errors  ($([ "${stress_errors}" = "0" ] && echo 'survives' || echo 'see log'))"
printf "   %-46s %s\n" "node 3 rejoins" "${handoff_result}"
printf "   %-46s %s\n" "2 nodes down (LOCAL_QUORUM)" "${quorum_lost_result}"
echo "   R + W > RF  ⇒  2 + 2 > 3  ⇒  a quorum read/write survives exactly one replica loss."
echo "   Cleanup (restart nodes + drop demo keyspaces) runs on the EXIT trap."

# The one hard gate: continuous load through a single node loss must not error.
if [ "${stress_errors}" != "0" ]; then
  echo -e "\nFAIL: expected 0 errors under load with one node down, got '${stress_errors}'." >&2
  echo "Inspect ${STRESS_OUT}. A tiny non-zero count can appear if in-flight requests were" >&2
  echo "routed to node 3 at the instant it was killed; LOCAL_QUORUM on the surviving 2/3" >&2
  echo "replicas should still let the driver retry them successfully." >&2
  exit 1
fi
echo -e "\nPASS: one node loss at LOCAL_QUORUM served continuous load with 0 errors. 🎯"
