import { Injectable } from '@nestjs/common';
import { actorKey, dailyLeaderboardPeriod, RawEvent } from '@cascade/contracts';
import { PostgresService } from '../postgres/postgres.service';

/**
 * Record that an actor was active on a UTC day. Set membership — re-inserting an
 * existing (project, actor, day) is a no-op (ADR-0016 §1).
 */
const INSERT_ACTIVITY = `
  INSERT INTO retention_actor_activity (project_id, actor_id, active_period)
  VALUES ($1, $2, $3)
  ON CONFLICT DO NOTHING`;

/**
 * Maintains the **retention** read model in Postgres (ADR-0015 §2 / ADR-0017):
 * the set of UTC days each actor was active. The Query API derives each actor's
 * cohort as its earliest active day and counts distinct returning actors per day
 * offset, producing the cohort triangle.
 *
 * The day is bucketed by **event time** (`occurredAt`) via the shared
 * {@link dailyLeaderboardPeriod} helper, so a late/out-of-order event lands on
 * the day it happened (ADR-0015 §3) — the same helper the daily leaderboard uses.
 *
 * **Naturally idempotent**: `INSERT … ON CONFLICT DO NOTHING` makes the write a
 * set insert, and the read-time cohort is `MIN(active_period)` (commutative), so
 * re-delivery or a full replay from offset 0 reproduce the same rows with no
 * dedup gate (ADR-0016 §1).
 *
 * An event with neither `actorId` nor `sessionId` is skipped (no error).
 */
@Injectable()
export class RetentionRepository {
  constructor(private readonly postgres: PostgresService) {}

  /** Record that this event's actor was active on the UTC day of `occurredAt`. */
  async apply(event: RawEvent): Promise<void> {
    const actor = actorKey(event);
    if (actor === null) return;

    const day = dailyLeaderboardPeriod(event.occurredAt); // 'YYYY-MM-DD' (UTC)
    await this.postgres.query(INSERT_ACTIVITY, [event.projectId, actor, day]);
  }
}
