#!/usr/bin/env bash
#
# postgres-replication-demo.sh — KAN-41 / ADR-0019 §2
#
# Demonstrates, against the Postgres primary + streaming read replica in
# infra/docker-compose.yml, the single-primary streaming-replication model and
# the replication lag the read path must tolerate:
#
#   1. confirm the replica is a streaming standby (pg_stat_replication);
#   2. write a row on the PRIMARY (all writes go to the primary);
#   3. read-your-writes on the primary — visible immediately;
#   4. poll the REPLICA until the row appears, and print the measured lag
#      (this is AC#5: a write on the primary becomes visible on the replica);
#   5. failure boundary — a write attempted on the replica is REJECTED
#      (it is read-only / in recovery), the chosen CP posture, not a bug.
#
# Prereqs: `make up` (primary + replica healthy) and Docker.
# Usage: ./postgres-replication-demo.sh
set -euo pipefail

# Shared helpers (psqlp/psqlr, wait_replica_streaming, replica_lag_*) + the
# PRIMARY/REPLICA container names.
# shellcheck source=infra/scripts/postgres-lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/postgres-lib.sh"

TBL=replication_demo
TOKEN="v-$(date +%s)"

# Always drop the demo table (on the primary; it replicates to the replica).
cleanup() {
  psqlp "DROP TABLE IF EXISTS ${TBL};" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "=================================================================="
echo " Postgres primary + read-replica demo — ADR-0019 §2"
echo "=================================================================="

echo -e "\n## 1. Replica is a streaming standby (AC#1)"
wait_replica_streaming || { echo "   ERROR: no replica in state='streaming'"; exit 1; }
psqlp "SELECT application_name, state, sync_state FROM pg_stat_replication;" \
  | sed 's/^/   pg_stat_replication: /'
echo "   pg_is_in_recovery() on replica → $(psqlr 'SELECT pg_is_in_recovery();')  (t = standby)"

echo -e "\n## 2. Write on the PRIMARY (all writes go to the primary) (AC#2)"
psqlp "CREATE TABLE IF NOT EXISTS ${TBL} (id int PRIMARY KEY, token text);" >/dev/null
psqlp "INSERT INTO ${TBL}(id, token) VALUES (1, '${TOKEN}')
       ON CONFLICT (id) DO UPDATE SET token = EXCLUDED.token;" >/dev/null
echo "   wrote ${TBL}(1) = ${TOKEN}"

echo -e "\n## 3. Read-your-writes on the PRIMARY — visible immediately (AC#3)"
echo "   primary read → $(psqlp "SELECT token FROM ${TBL} WHERE id=1;")"

echo -e "\n## 4. Poll the REPLICA until the row appears + measure lag (AC#5)"
if wait_scalar_on_replica "SELECT token FROM ${TBL} WHERE id=1;" "${TOKEN}"; then
  echo "   replica read → $(psqlr "SELECT token FROM ${TBL} WHERE id=1;")  (replicated)"
  echo "   lag: $(replica_lag_bytes) bytes · $(replica_lag_seconds) s behind primary"
else
  echo "   ERROR: row never replicated to the replica"
  exit 1
fi

echo -e "\n## 5. Failure boundary — a write on the REPLICA is REJECTED (AC#4)"
if psqlr "INSERT INTO ${TBL}(id, token) VALUES (2, 'nope');" 2>/tmp/pg_ro.$$; then
  echo "   ERROR: replica accepted a write — it should be read-only"; rm -f /tmp/pg_ro.$$; exit 1
fi
grep -iE 'read-only transaction' /tmp/pg_ro.$$ | sed 's/^/   /' | head -1 || true
rm -f /tmp/pg_ro.$$

echo -e "\n## 6. Result"
printf "   %-46s %s\n" "write on primary"                 "${TOKEN}"
printf "   %-46s %s\n" "read-your-writes on primary"       "immediate"
printf "   %-46s %s\n" "same row visible on replica"        "yes (bounded lag)"
printf "   %-46s %s\n" "write attempted on replica"         "REJECTED (read-only standby)"
echo "   Single-primary streaming replication: writes → primary; the replica"
echo "   serves eventually-consistent analytics reads, trailing by a bounded lag."
echo -e "\n## 7. Cleanup (DROP ${TBL}) runs on the EXIT trap."
echo "done."
