#!/usr/bin/env bash
#
# 10-init-replication.sh — KAN-41 / ADR-0019 §2
#
# initdb hook (runs once, inside /docker-entrypoint-initdb.d, on the PRIMARY's
# first boot). Creates the streaming-replication role the read replica uses for
# pg_basebackup + the walreceiver connection, and opens a pg_hba rule for it.
# The stock entrypoint runs this after initdb, before the real server start, so
# the pg_hba change is picked up by the server that goes on to serve traffic.
set -euo pipefail

: "${POSTGRES_REPLICATION_PASSWORD:?must be set (see docker-compose)}"

# REPLICATION login role. Password is stored scram-sha-256 (PG16 default
# password_encryption), matching the pg_hba method below.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-SQL
	CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD '${POSTGRES_REPLICATION_PASSWORD}';
SQL

# Allow the replicator role to open replication connections from anywhere on the
# compose network (dev-scoped 0.0.0.0/0). Appended to the data-dir pg_hba.conf so
# it persists in the volume.
echo "host replication replicator 0.0.0.0/0 scram-sha-256" >> "$PGDATA/pg_hba.conf"

echo "10-init-replication: created 'replicator' role and opened pg_hba replication rule"
