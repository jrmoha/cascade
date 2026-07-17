#!/usr/bin/env bash
#
# postgres-lib.sh — shared helpers for the Postgres primary/replica scripts
# (postgres-replication-demo.sh). Source it *after* `set -euo pipefail`:
#
#   source "$(dirname "${BASH_SOURCE[0]}")/postgres-lib.sh"
#
# Container names of the primary + replica in infra/docker-compose.yml.
# (Consumed by the sourcing scripts, hence "unused" here.)
# shellcheck disable=SC2034
PRIMARY=cascade-postgres-primary
# shellcheck disable=SC2034
REPLICA=cascade-postgres-replica

# Run SQL and print the single scalar result (tuples-only, unaligned).
#   psqlp "<sql>"   → on the primary
#   psqlr "<sql>"   → on the replica
psqlp() { docker exec -i "$PRIMARY" psql -U cascade -d cascade -tAc "$1"; }
psqlr() { docker exec -i "$REPLICA" psql -U cascade -d cascade -tAc "$1"; }

# Wait until the primary reports at least one replica in state='streaming'.
#   wait_replica_streaming
wait_replica_streaming() {
  local n
  for _ in $(seq 1 40); do
    n=$(psqlp "SELECT count(*) FROM pg_stat_replication WHERE state='streaming';" 2>/dev/null || echo 0)
    [ "${n:-0}" -ge 1 ] && return 0
    sleep 1
  done
  return 1
}

# Bytes the replica trails the primary by (0 when fully caught up).
#   replica_lag_bytes
replica_lag_bytes() {
  psqlp "SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn),0)::bigint
         FROM pg_stat_replication ORDER BY replay_lsn LIMIT 1;" 2>/dev/null || echo '?'
}

# Seconds the replica's last replayed transaction trails now() (time-based lag).
#   replica_lag_seconds
replica_lag_seconds() {
  psqlr "SELECT COALESCE(round(EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp()))::numeric,3),0);" \
    2>/dev/null || echo '?'
}

# Poll the replica until the given SQL scalar equals the expected value.
#   wait_scalar_on_replica "<sql>" "<expected>"
wait_scalar_on_replica() {
  local sql="$1" expected="$2" got
  for _ in $(seq 1 40); do
    got=$(psqlr "$sql" 2>/dev/null || echo '')
    [ "$got" = "$expected" ] && return 0
    sleep 0.5
  done
  return 1
}
