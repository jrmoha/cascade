import { GenericContainer, type StartedTestContainer, Wait } from 'testcontainers';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import Redis from 'ioredis';
import { HealthIndicatorService } from '@nestjs/terminus';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { AggregatorConfig } from '../src/config/env.schema';
import { CassandraService, KEYSPACE } from '../src/cassandra/cassandra.service';
import { CassandraHealthIndicator } from '../src/cassandra/cassandra.health';
import { PostgresService } from '../src/postgres/postgres.service';
import { PostgresHealthIndicator } from '../src/postgres/postgres.health';
import { RedisHealthIndicator } from '../src/redis/redis.health';

// Integration test against real Cassandra, Postgres, and Redis (no mocking the
// stores, per CLAUDE.md). It proves the Aggregator skeleton's store wiring —
// each client connects, the Cassandra + Postgres migrators bootstrap their
// (namespaced) tracking tables and report "no pending" against the empty
// migration dirs, and the readiness indicators report healthy. The Kafka
// consume/DLQ path is covered by the unit test and the e2e smoke.
// Set SKIP_INTEGRATION=1 to skip where Docker is unavailable.
describe.skipIf(process.env.SKIP_INTEGRATION === '1')('Aggregator stores (integration)', () => {
  let cassandraContainer: StartedTestContainer;
  let redisContainer: StartedTestContainer;
  let postgresContainer: StartedPostgreSqlContainer;

  let cassandra: CassandraService;
  let postgres: PostgresService;
  let redis: Redis;

  beforeAll(async () => {
    [cassandraContainer, redisContainer, postgresContainer] = await Promise.all([
      new GenericContainer('cassandra:4.1')
        .withExposedPorts(9042)
        .withStartupTimeout(180_000)
        .withWaitStrategy(Wait.forLogMessage(/Starting listening for CQL clients/))
        .start(),
      new GenericContainer('redis:7.2-alpine')
        .withExposedPorts(6379)
        .withWaitStrategy(Wait.forLogMessage(/Ready to accept connections/))
        .start(),
      new PostgreSqlContainer('postgres:16-alpine').start(),
    ]);

    const config = {
      CASSANDRA_CONTACT_POINTS: [cassandraContainer.getHost()],
      CASSANDRA_PORT: cassandraContainer.getMappedPort(9042),
      CASSANDRA_LOCAL_DC: 'datacenter1',
      REDIS_HOST: redisContainer.getHost(),
      REDIS_PORT: redisContainer.getMappedPort(6379),
      DATABASE_URL: postgresContainer.getConnectionUri(),
      KAFKA_BOOTSTRAP_SERVERS: ['localhost:9092'],
      AGGREGATOR_DEDUP_TTL_SECONDS: 86400,
      AGGREGATOR_LEADERBOARD_DAILY_TTL_SECONDS: 172800,
      PORT: 3005,
    } satisfies AggregatorConfig;

    cassandra = new CassandraService(config);
    await cassandra.onApplicationBootstrap(); // connect + run Cassandra migrator

    postgres = new PostgresService(config);
    await postgres.onApplicationBootstrap(); // connect + run pg migrator

    redis = new Redis({
      host: config.REDIS_HOST,
      port: config.REDIS_PORT,
      lazyConnect: true,
      maxRetriesPerRequest: 1,
    });
    await redis.connect();
  }, 240_000);

  afterAll(async () => {
    await cassandra?.onModuleDestroy();
    await postgres?.onModuleDestroy();
    await redis?.quit().catch(() => undefined);
    await Promise.all([
      cassandraContainer?.stop(),
      redisContainer?.stop(),
      postgresContainer?.stop(),
    ]);
  });

  it('Cassandra migrator bootstraps the keyspace and records applied migrations', async () => {
    const rs = await cassandra.execute(`SELECT id FROM ${KEYSPACE}.aggregator_schema_migrations`);
    const ids = rs.rows.map((r) => r.get('id') as string);
    // The event-counts counter tables migration (KAN-32) is applied and tracked.
    expect(ids).toContain('0001_create_event_counts');
  });

  it('Postgres migrator bootstraps its own tracking table (separate from Prisma)', async () => {
    const { rows } = await postgres.query<{ id: string }>(
      'SELECT id FROM aggregator_schema_migrations',
    );
    expect(rows).toHaveLength(0);
  });

  it('reports all three stores healthy via the readiness indicators', async () => {
    const hi = new HealthIndicatorService();
    const cassandraHealth = new CassandraHealthIndicator(cassandra, hi);
    const postgresHealth = new PostgresHealthIndicator(postgres, hi);
    const redisHealth = new RedisHealthIndicator(redis, hi);

    await expect(cassandraHealth.isHealthy('cassandra')).resolves.toMatchObject({
      cassandra: { status: 'up' },
    });
    await expect(postgresHealth.isHealthy('postgres')).resolves.toMatchObject({
      postgres: { status: 'up' },
    });
    await expect(redisHealth.isHealthy('redis')).resolves.toMatchObject({
      redis: { status: 'up' },
    });
  });
});
