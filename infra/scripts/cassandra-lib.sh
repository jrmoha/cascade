#!/usr/bin/env bash
#
# cassandra-lib.sh — shared helpers for the Cassandra cluster scripts
# (consistency-demo.sh, node-down-chaos.sh). Source it; it defines no state
# beyond the functions below and must be sourced *after* `set -euo pipefail`.
#
#   source "$(dirname "${BASH_SOURCE[0]}")/cassandra-lib.sh"
#
# Container names of the 3-node cluster in infra/docker-compose.yml.
# (Consumed by the sourcing scripts, hence "unused" here.)
# shellcheck disable=SC2034
N1=cascade-cassandra-1
# shellcheck disable=SC2034
N2=cascade-cassandra-2
# shellcheck disable=SC2034
N3=cascade-cassandra-3

# Run a CQL statement, coordinated by the given node's container.
#   cql <container> "<cql>"
cql() { docker exec -i "$1" cqlsh -e "$2"; }

# Wait until the given node sees all 3 nodes Up/Normal (UN). Coordinated by the
# node passed in so it works even when another node is the one being restarted.
#   wait_ring <container>
wait_ring() {
  local node="$1" n
  for _ in $(seq 1 60); do
    n=$(docker exec "$node" nodetool status 2>/dev/null | grep -cE '^UN' || echo 0)
    [ "$n" = "3" ] && return 0
    sleep 3
  done
  return 1
}

# Wait until the node's native transport (cqlsh) accepts queries.
#   wait_cql <container>
wait_cql() {
  local node="$1"
  for _ in $(seq 1 60); do
    docker exec -i "$node" cqlsh -e "SELECT now() FROM system.local;" >/dev/null 2>&1 && return 0
    sleep 3
  done
  return 1
}
