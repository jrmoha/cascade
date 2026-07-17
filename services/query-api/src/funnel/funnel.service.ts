import { Injectable } from '@nestjs/common';
import type { FunnelResponse, FunnelStepResult, FunnelSteps } from '@cascade/contracts';
import { PostgresService } from '../postgres/postgres.service';

export interface FunnelQuery {
  projectId: string;
  steps: FunnelSteps;
  from: string;
  to: string;
}

/**
 * Computes an **ordered funnel** from the Aggregator's per-actor step table
 * (`funnel_actor_steps`, ADR-0017). There is no stored funnel object: the step
 * sequence is supplied per request, so this builds the query dynamically and
 * pivots the table per actor — exactly the relational slicing Postgres is the
 * right tool for (ADR-0015).
 *
 * Per actor we take, within the window, the earliest time each step's event type
 * occurred (`t1..tn`). An actor counts at step *k* only if it has `t1..tk` with
 * **non-decreasing** timestamps (`t2 >= t1`, …, `tk >= t(k-1)`) — i.e. it moved
 * through the steps in order. `conversionRate` is cumulative from step 1.
 */
@Injectable()
export class FunnelService {
  constructor(private readonly postgres: PostgresService) {}

  async compute(query: FunnelQuery): Promise<FunnelResponse> {
    const { projectId, steps, from, to } = query;
    const counts = await this.stepActorCounts(projectId, steps, from, to);

    const entry = counts[0] ?? 0;
    const resultSteps: FunnelStepResult[] = steps.map((eventType, i) => ({
      step: i + 1,
      eventType,
      actors: counts[i] ?? 0,
      conversionRate: entry > 0 ? round4((counts[i] ?? 0) / entry) : 0,
    }));

    return { projectId, from, to, steps: resultSteps };
  }

  /**
   * Returns `[actorsReachedStep1, …, actorsReachedStepN]`. Builds a per-actor
   * pivot (`min(first_seen_at) FILTER (WHERE event_type = $k)` as `t{k}`) and
   * counts actors satisfying the cumulative ordered condition for each step.
   */
  private async stepActorCounts(
    projectId: string,
    steps: FunnelSteps,
    from: string,
    to: string,
  ): Promise<number[]> {
    // $1 projectId · $2..$(n+1) step event types · $(n+2) from · $(n+3) to
    const stepParams = steps.map((_, i) => `$${i + 2}`);
    const fromParam = `$${steps.length + 2}`;
    const toParam = `$${steps.length + 3}`;

    const pivots = steps.map(
      (_, i) => `min(first_seen_at) FILTER (WHERE event_type = ${stepParams[i]}) AS t${i + 1}`,
    );

    const stepCounts = steps.map((_, i) => {
      const conds: string[] = ['t1 IS NOT NULL'];
      for (let j = 1; j <= i; j++) conds.push(`t${j + 1} >= t${j}`);
      return `count(*) FILTER (WHERE ${conds.join(' AND ')})::int AS step_${i + 1}`;
    });

    const sql = `
      WITH actor_steps AS (
        SELECT actor_id, ${pivots.join(', ')}
        FROM funnel_actor_steps
        WHERE project_id = $1
          AND event_type IN (${stepParams.join(', ')})
          AND first_seen_at >= ${fromParam} AND first_seen_at <= ${toParam}
        GROUP BY actor_id
      )
      SELECT ${stepCounts.join(', ')} FROM actor_steps`;

    const params = [projectId, ...steps, from, to];
    // Eventually-consistent analytics read → replica (ADR-0019 §2).
    const { rows } = await this.postgres.replicaQuery<Record<string, number>>(sql, params);
    const row = rows[0] ?? {};
    return steps.map((_, i) => Number(row[`step_${i + 1}`] ?? 0));
  }
}

/** Round a 0–1 rate to 4 decimal places to avoid floating-point noise. */
function round4(n: number): number {
  return Math.round(n * 10_000) / 10_000;
}
