#!/usr/bin/env bash
#
# kafka-scaling-demo.sh — KAN-40 / ADR-0020
#
# Shows, against the 3-broker cluster + 6-partition `raw-events`, how a scaled
# consumer group behaves:
#   1. topic layout (6 partitions, RF=3, leaders spread across brokers)
#   2. per-key ordering — a single sessionId's events all land on ONE partition
#   3. THROUGHPUT rises with instances: time the Ingestion-Processor group draining
#      a fixed backlog at 1 instance vs 3 (drain rate ~ scales up to partition count)
#   4. REBALANCE — scaling the group reassigns partitions and processing continues
#
# Drives the real `cascade-ingestion-processor` group (NestJS ServerKafka appends
# `-server` to the broker-side group id). Load is produced by kafka-load.mjs
# straight to `raw-events` (bypassing the Collector). Prereqs: `make up` (cluster
# healthy) + Docker + node (kafkajs at repo root). Docs: docs/runbooks/kafka-scaling.md
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE=(docker compose -f "$ROOT/infra/docker-compose.yml")
K1=cascade-kafka-1
GROUP=cascade-ingestion-processor-server   # ServerKafka postfixes '-server'
BACKLOG=${BACKLOG:-40000}                   # events per timed drain
SESSIONS=${SESSIONS:-2000}
HI=${HI:-3}                                 # scale-up target (≤ 6 partitions)

kbin() { local s="$1"; shift; docker exec "$K1" "/opt/kafka/bin/$s" "$@" --bootstrap-server localhost:29092; }
load() { COUNT="$1" SESSIONS="${2:-$SESSIONS}" node "$ROOT/infra/scripts/kafka-load.mjs"; }

# Sum LAG across raw-events partitions for the group (columns: GROUP TOPIC PART CUR END LAG ...).
# A partition with no committed offset yet shows LAG '-'; the group may also not be
# assigned raw-events at all yet. Both mean "not drained", so return a large sentinel
# rather than 0 — otherwise wait_drained would report success before consumption began.
group_lag() {
  kbin kafka-consumer-groups.sh --group "$GROUP" --describe 2>/dev/null \
    | awk '$2=="raw-events" { rows++; if ($6 ~ /^[0-9]+$/) s+=$6; else s+=1000000 }
           END { if (rows==0) print 1000000; else print s+0 }'
}
# Number of live members in the group. `--members` is a modifier of `--describe`.
# Count only data rows (first field == the group name), never the header/blank line.
member_count() {
  kbin kafka-consumer-groups.sh --describe --members --group "$GROUP" 2>/dev/null \
    | awk -v g="$GROUP" '$1==g {c++} END{print c+0}'
}
# Per-partition end offsets as "<partition> <offset>" lines, sorted by partition.
part_offsets() {
  docker exec "$K1" /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:29092 \
    --topic raw-events 2>/dev/null | awk -F: '{print $2, $3}' | sort -n
}

scale_to() { # scale the ingestion-processor group to N replicas (no rebuild, no other
  # apps). NOT --build: scaling 1->N then ADDS containers (a real rebalance) instead
  # of recreating them, and boot time never contaminates a drain measurement. Build
  # the image first with `make stack-build` (or `make stack-scale`).
  "${COMPOSE[@]}" --profile apps up -d --no-deps --scale "ingestion-processor=$1" \
    ingestion-processor >/dev/null 2>&1
}

wait_members() { # wait until the group has exactly $1 members (generous: first boot
  # runs the NestJS app + Cassandra migrator + Kafka join)
  local want="$1" m
  for _ in $(seq 1 90); do
    m=$(member_count)
    [ "$m" = "$want" ] && return 0
    sleep 2
  done
  return 1
}

# Drain the group to lag 0, echo elapsed seconds. $1 = timeout seconds.
wait_drained() {
  local timeout="$1" start now lag
  start=$(date +%s)
  while :; do
    lag=$(group_lag)
    [ "${lag:-0}" -eq 0 ] && break
    now=$(date +%s)
    [ $((now - start)) -ge "$timeout" ] && break
    sleep 1
  done
  now=$(date +%s); echo $((now - start))
}

echo "=================================================================="
echo " Kafka partitioning + consumer-group scaling demo — ADR-0020/KAN-40"
echo "=================================================================="

echo -e "\n## 0. Preflight: 3 brokers + raw-events with 6 partitions"
parts=$(kbin kafka-topics.sh --describe --topic raw-events 2>/dev/null | awk -F'PartitionCount: ' 'NF>1{print $2+0}' | head -1)
[ "${parts:-0}" = "6" ] || { echo "ERROR: raw-events not found with 6 partitions (got '${parts:-none}'). Run \`make up\`." >&2; exit 1; }

echo -e "\n## 1. Topic layout — 6 partitions, RF=3, leaders spread across brokers"
kbin kafka-topics.sh --describe --topic raw-events

echo -e "\n## 2. Per-key ordering: 500 events for a SINGLE session land on ONE partition"
before=$(part_offsets)
load 500 1 >/dev/null   # SESSIONS=1 → every event keyed 'sess-0'
after=$(part_offsets)
echo "   partitions whose end-offset advanced (expect exactly one, by +500):"
join <(echo "$before") <(echo "$after") | awk '$3>$2{printf "     partition %s: +%d\n", $1, $3-$2}'

echo -e "\n## 3. Start the Ingestion-Processor group at 1 instance; drain any backlog"
scale_to 1
wait_members 1 || echo "   (warning: group did not reach 1 member in time)"
echo "   member(s): $(member_count); draining existing backlog to lag 0..."
wait_drained 300 >/dev/null

echo -e "\n## 4. THROUGHPUT @ 1 instance: produce ${BACKLOG}, time the drain"
load "$BACKLOG" >/dev/null
t1=$(wait_drained 600)
r1=$(( BACKLOG / (t1 > 0 ? t1 : 1) ))
echo "   drained ${BACKLOG} in ${t1}s  (~${r1} events/s, 1 consumer owns all 6 partitions)"

echo -e "\n## 5. Scale to ${HI} instances → REBALANCE (partitions reassign, processing continues)"
scale_to "$HI"
wait_members "$HI" || echo "   (warning: group did not reach ${HI} members in time)"
sleep 5   # let the rebalance's sync phase finalise partition assignment before we read it
echo "   members now: $(member_count) — assignment (#PARTITIONS per member):"
kbin kafka-consumer-groups.sh --describe --members --group "$GROUP" 2>/dev/null | awk 'NF>0'

echo -e "\n## 6. THROUGHPUT @ ${HI} instances: produce ${BACKLOG}, time the drain"
load "$BACKLOG" >/dev/null
t3=$(wait_drained 600)
r3=$(( BACKLOG / (t3 > 0 ? t3 : 1) ))
echo "   drained ${BACKLOG} in ${t3}s  (~${r3} events/s, ${HI} consumers share 6 partitions)"

echo -e "\n## 7. Result"
printf "   %-30s %s\n" "throughput @ 1 instance" "~${r1} events/s (drain ${t1}s)"
printf "   %-30s %s\n" "throughput @ ${HI} instances" "~${r3} events/s (drain ${t3}s)"
if [ "$r3" -gt "$r1" ]; then
  echo "   ✔ throughput rose with instances (scales up to the 6-partition cap)."
else
  echo "   ⚠ no rise observed — the bottleneck may be downstream (Cassandra) not consumers;"
  echo "     try a larger BACKLOG or check Cassandra load."
fi
echo "   Partition key = sessionId ⇒ a session stays ordered on one partition (step 2)."
echo -e "\n   Leaving the group at ${HI} instances. Scale back with:"
echo "     make stack-scale IP=1 AGG=1     (or docker compose ... --scale ingestion-processor=1)"
