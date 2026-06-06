import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { Logger } from '@nestjs/common';
import type { Pool } from 'pg';

/**
 * Postgres migrations live at `<package>/migrations/postgres` (sibling of
 * `src`/`dist`), resolved relative to this module's directory so it works under
 * ts-node, compiled `dist`, and Vitest alike — independent of the process CWD.
 */
const MIGRATIONS_DIR = resolve(__dirname, '../../migrations/postgres');

/**
 * A small, dependency-free SQL migration runner over a `pg` Pool — the same
 * shape as the Cassandra `Migrator` (ADR-0007), reused here for the Aggregator's
 * relational read models (funnel/retention summaries, ADR-0015). It bootstraps a
 * tracking table, then applies the ordered `*.sql` files in {@link MIGRATIONS_DIR}
 * exactly once each, each wrapped in a transaction, so running it repeatedly
 * (on boot or via a CLI) is idempotent. The committed `.sql` files are the
 * single source of truth; nothing is created ad-hoc.
 *
 * The Aggregator owns its own tables in the shared Postgres (raw `pg`, separate
 * from the Project/Schema service's Prisma-managed schema — ADR-0011/ADR-0015),
 * so the tracking table is namespaced `aggregator_schema_migrations` to avoid any
 * collision in the same database. Phase 1 skeleton (KAN-31): the `postgres/` dir
 * starts empty (no summary tables yet); the runner simply reports "up to date".
 */
export class PgMigrator {
  private readonly logger = new Logger(PgMigrator.name);

  constructor(
    private readonly pool: Pool,
    private readonly migrationsDir: string = MIGRATIONS_DIR,
  ) {}

  async run(): Promise<void> {
    await this.pool.query(
      `CREATE TABLE IF NOT EXISTS aggregator_schema_migrations (
         id text PRIMARY KEY,
         applied_at timestamptz NOT NULL DEFAULT now()
       )`,
    );

    const applied = await this.appliedIds();
    const pending = this.migrationFiles().filter((id) => !applied.has(id));

    if (pending.length === 0) {
      this.logger.log('Postgres schema up to date; no migrations to apply');
      return;
    }

    for (const id of pending) {
      await this.apply(id);
    }
    this.logger.log(`Applied ${pending.length} Postgres migration(s): ${pending.join(', ')}`);
  }

  private async appliedIds(): Promise<Set<string>> {
    const { rows } = await this.pool.query<{ id: string }>(
      'SELECT id FROM aggregator_schema_migrations',
    );
    return new Set(rows.map((row) => row.id));
  }

  /** Ordered list of migration ids (filenames), e.g. `0001_create_funnel_summaries`. */
  private migrationFiles(): string[] {
    if (!existsSync(this.migrationsDir)) return [];
    return readdirSync(this.migrationsDir)
      .filter((f) => f.endsWith('.sql'))
      .sort()
      .map((f) => f.replace(/\.sql$/, ''));
  }

  private async apply(id: string): Promise<void> {
    const sql = readFileSync(resolve(this.migrationsDir, `${id}.sql`), 'utf-8');
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(sql);
      await client.query('INSERT INTO aggregator_schema_migrations (id) VALUES ($1)', [id]);
      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
    this.logger.log(`Applied Postgres migration ${id}`);
  }
}
