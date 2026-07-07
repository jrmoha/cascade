import { readdirSync, readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { Logger } from '@nestjs/common';
import type { Client } from 'cassandra-driver';

export const KEYSPACE = 'cascade';

/**
 * Migrations live at the package root (sibling of `src`/`dist`), so resolving
 * relative to this module's directory works under ts-node, the compiled `dist`,
 * and Vitest alike — independent of the process CWD (the e2e smoke harness boots
 * this service from another workspace).
 */
const MIGRATIONS_DIR = resolve(__dirname, '../../migrations');

/** Replication topology for keyspace creation (KAN-38, ADR-0019). */
export interface KeyspaceReplication {
  /** The local datacenter name (`CASSANDRA_LOCAL_DC`). */
  localDc: string;
  /** Replication factor for that DC (`CASSANDRA_REPLICATION_FACTOR`). */
  replicationFactor: number;
}

// Keyspace bootstrap uses NetworkTopologyStrategy with the DC + RF from config
// (ADR-0019): production-correct (rack/DC-aware, multi-region-ready) over
// SimpleStrategy, and the same DDL on the single-node dev/test keyspace (RF=1).
// This is bootstrap, not a versioned schema migration.
function createKeyspaceCql({ localDc, replicationFactor }: KeyspaceReplication): string {
  return `
    CREATE KEYSPACE IF NOT EXISTS ${KEYSPACE}
    WITH replication = {'class': 'NetworkTopologyStrategy', '${localDc}': ${replicationFactor}}`;
}

const CREATE_MIGRATIONS_TABLE = `
  CREATE TABLE IF NOT EXISTS ${KEYSPACE}.schema_migrations (
    id text PRIMARY KEY,
    applied_at timestamp
  )`;

/**
 * A small, dependency-free schema migration runner over the cassandra-driver
 * (KAN-24). It bootstraps the keyspace and a `schema_migrations` tracking table,
 * then applies the ordered `*.cql` files in {@link MIGRATIONS_DIR} exactly once
 * each — so running it repeatedly (on every startup, or via `npm run migrate`)
 * is idempotent. The committed `.cql` files are the single source of truth for
 * the schema; nothing is created ad-hoc.
 */
export class Migrator {
  private readonly logger = new Logger(Migrator.name);

  constructor(
    private readonly client: Client,
    private readonly replication: KeyspaceReplication,
    private readonly migrationsDir: string = MIGRATIONS_DIR,
  ) {}

  async run(): Promise<void> {
    await this.client.execute(createKeyspaceCql(this.replication));
    await this.client.execute(CREATE_MIGRATIONS_TABLE);

    const applied = await this.appliedIds();
    const pending = this.migrationFiles().filter((id) => !applied.has(id));

    if (pending.length === 0) {
      this.logger.log('Schema up to date; no migrations to apply');
      return;
    }

    for (const id of pending) {
      await this.apply(id);
    }
    this.logger.log(`Applied ${pending.length} migration(s): ${pending.join(', ')}`);
  }

  private async appliedIds(): Promise<Set<string>> {
    const rs = await this.client.execute(`SELECT id FROM ${KEYSPACE}.schema_migrations`);
    return new Set(rs.rows.map((row) => row.get('id') as string));
  }

  /** Ordered list of migration ids (filenames), e.g. `0001_create_raw_events`. */
  private migrationFiles(): string[] {
    return readdirSync(this.migrationsDir)
      .filter((f) => f.endsWith('.cql'))
      .sort()
      .map((f) => f.replace(/\.cql$/, ''));
  }

  private async apply(id: string): Promise<void> {
    const cql = readFileSync(resolve(this.migrationsDir, `${id}.cql`), 'utf-8');
    // Strip full-line `--` comments FIRST (a comment may itself contain ';'),
    // then split into statements on ';'. Migrations use full-line comments only.
    const statements = cql
      .split('\n')
      .filter((line) => !line.trim().startsWith('--'))
      .join('\n')
      .split(';')
      .map((s) => s.trim())
      .filter((s) => s.length > 0);

    for (const statement of statements) {
      await this.client.execute(statement);
    }

    await this.client.execute(
      `INSERT INTO ${KEYSPACE}.schema_migrations (id, applied_at) VALUES (?, ?)`,
      [id, new Date()],
      { prepare: true },
    );
    this.logger.log(`Applied migration ${id}`);
  }
}
