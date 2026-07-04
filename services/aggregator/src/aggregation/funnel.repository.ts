import { Injectable } from '@nestjs/common';
import { actorKey, RawEvent } from '@cascade/contracts';
import { PostgresService } from '../postgres/postgres.service';

/**
 * Upsert the earliest event-time at which an actor performed an event type.
 * `LEAST` keeps the minimum, so the row converges to the true first-seen
 * regardless of arrival order or replay (ADR-0016 §1).
 */
const UPSERT_STEP = `
  INSERT INTO funnel_actor_steps (project_id, actor_id, event_type, first_seen_at)
  VALUES ($1, $2, $3, $4)
  ON CONFLICT (project_id, actor_id, event_type)
  DO UPDATE SET first_seen_at = LEAST(funnel_actor_steps.first_seen_at, EXCLUDED.first_seen_at)`;

/**
 * Maintains the **funnel** read model in Postgres (ADR-0015 §2 / ADR-0017): a
 * generic per-`(project, actor, eventType)` first-seen table the Query API
 * pivots into an ordered funnel at read time. The "right tool" contrast to the
 * Cassandra counters and Redis leaderboard — relational state we later slice and
 * group.
 *
 * **Naturally idempotent**: the upsert keeps `LEAST(existing, new)` first-seen,
 * which is commutative and idempotent, so re-applying the same event (Kafka
 * at-least-once, or a full replay from offset 0) is a no-op. Like the
 * leaderboard's `ZADD GT`, this view needs no dedup gate of its own; riding the
 * controller's shared per-`eventId` gate is a harmless superset (ADR-0016 §1).
 *
 * An event with neither `actorId` nor `sessionId` has no actor to attribute and
 * is skipped (no error) — mirroring how the leaderboard skips non-score events.
 */
@Injectable()
export class FunnelRepository {
  constructor(private readonly postgres: PostgresService) {}

  /** Record that this event's actor performed `event.type` at `occurredAt`. */
  async apply(event: RawEvent): Promise<void> {
    const actor = actorKey(event);
    if (actor === null) return;

    await this.postgres.query(UPSERT_STEP, [event.projectId, actor, event.type, event.occurredAt]);
  }
}
