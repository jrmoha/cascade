import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { RawEvent } from '@cascade/contracts';
import type { PostgresService } from '../src/postgres/postgres.service';
import { FunnelRepository } from '../src/aggregation/funnel.repository';
import { RetentionRepository } from '../src/aggregation/retention.repository';

const base: RawEvent = {
  eventId: 'e1',
  projectId: 'rpg',
  schemaVersion: 1,
  type: 'game_start',
  occurredAt: '2026-05-01T23:30:00.000Z',
  receivedAt: '2026-05-01T23:30:00.000Z',
  payload: {},
};

describe('FunnelRepository / RetentionRepository (unit)', () => {
  let query: ReturnType<typeof vi.fn>;
  let postgres: PostgresService;
  let funnel: FunnelRepository;
  let retention: RetentionRepository;

  beforeEach(() => {
    query = vi.fn().mockResolvedValue({ rows: [] });
    postgres = { query } as unknown as PostgresService;
    funnel = new FunnelRepository(postgres);
    retention = new RetentionRepository(postgres);
  });

  it('funnel upserts (project, actorId, type, occurredAt)', async () => {
    await funnel.apply({ ...base, actorId: 'a1' });
    expect(query).toHaveBeenCalledTimes(1);
    expect(query.mock.calls[0][1]).toEqual(['rpg', 'a1', 'game_start', base.occurredAt]);
  });

  it('funnel falls back to sessionId when actorId is absent', async () => {
    await funnel.apply({ ...base, sessionId: 's9' });
    expect(query.mock.calls[0][1]).toEqual(['rpg', 's9', 'game_start', base.occurredAt]);
  });

  it('funnel skips an event with neither actorId nor sessionId', async () => {
    await funnel.apply(base);
    expect(query).not.toHaveBeenCalled();
  });

  it('retention inserts the UTC day of occurredAt (actorId)', async () => {
    await retention.apply({ ...base, actorId: 'a1' });
    expect(query).toHaveBeenCalledTimes(1);
    // 2026-05-01T23:30Z is still the 1st in UTC.
    expect(query.mock.calls[0][1]).toEqual(['rpg', 'a1', '2026-05-01']);
  });

  it('retention falls back to sessionId, and skips when no actor identity', async () => {
    await retention.apply({ ...base, sessionId: 's9' });
    expect(query.mock.calls[0][1]).toEqual(['rpg', 's9', '2026-05-01']);

    query.mockClear();
    await retention.apply(base);
    expect(query).not.toHaveBeenCalled();
  });
});
