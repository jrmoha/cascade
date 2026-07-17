#!/usr/bin/env bash
#
# entrypoint.sh — KAN-41 / ADR-0019 §2
#
# Replica bootstrap for the hand-rolled Postgres streaming standby. On first boot
# (empty data dir) it takes a base backup from the primary and configures itself
# as a hot standby; on every subsequent boot it just resumes streaming. Then it
# hands off to the stock docker-entrypoint.sh so the container runs a normal
# `postgres` (which starts in recovery because standby.signal is present).
set -euo pipefail

PGDATA="${PGDATA:-/var/lib/postgresql/data}"
PRIMARY_HOST="${PRIMARY_HOST:-postgres-primary}"
PRIMARY_PORT="${PRIMARY_PORT:-5432}"

# The stock image starts entrypoints as root and later drops to `postgres`. We
# must own the data dir and run pg_basebackup AS postgres so the copied files are
# owned correctly (postgres refuses to start on a root-owned data dir). Re-exec
# this script as postgres, then continue below.
if [ "$(id -u)" = '0' ]; then
	mkdir -p "$PGDATA"
	chown -R postgres:postgres "$PGDATA"
	chmod 0700 "$PGDATA"
	exec gosu postgres "$0" "$@"
fi

# ---- from here on we are the postgres user ----

if [ ! -s "$PGDATA/PG_VERSION" ]; then
	echo "replica: empty data dir — streaming base backup from ${PRIMARY_HOST}:${PRIMARY_PORT}"
	# Clear any partial state from an aborted earlier attempt.
	find "$PGDATA" -mindepth 1 -delete 2>/dev/null || true

	export PGPASSWORD="${POSTGRES_REPLICATION_PASSWORD:?must be set}"
	# -R writes standby.signal + a primary_conninfo line; -Xs streams WAL during
	# the backup so the standby is consistent; -P prints progress; -w never prompts.
	until pg_basebackup -h "$PRIMARY_HOST" -p "$PRIMARY_PORT" -U replicator \
		-D "$PGDATA" -Fp -Xs -R -P -w; do
		echo "replica: primary not ready for base backup — retrying in 3s"
		sleep 3
	done
	chmod 0700 "$PGDATA"

	# pg_basebackup -R does NOT embed the password in primary_conninfo. Append our
	# own setting (Postgres uses the last occurrence) so the walreceiver can
	# reconnect after any restart — this file lives in the data volume, so it
	# persists. Dev-scoped: a plaintext replication password is acceptable here.
	cat >> "$PGDATA/postgresql.auto.conf" <<-CONF
		primary_conninfo = 'host=${PRIMARY_HOST} port=${PRIMARY_PORT} user=replicator password=${POSTGRES_REPLICATION_PASSWORD} application_name=replica_1'
	CONF
	echo "replica: base backup complete; configured as hot standby"
else
	echo "replica: existing data dir — resuming standby"
fi

exec docker-entrypoint.sh postgres
