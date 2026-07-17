import { Injectable } from '@nestjs/common';
import {
  RETENTION_GRANULARITY,
  type RetentionCohort,
  type RetentionResponse,
} from '@cascade/contracts';
import { PostgresService } from '../postgres/postgres.service';

export interface RetentionQuery {
  projectId: string;
  from: string;
  to: string;
  maxOffset: number;
}

interface MatrixRow {
  cohort: string;
  day_offset: number;
  actors: number;
}

/**
 * Computes a **retention cohort matrix** from the Aggregator's per-actor
 * active-day set (`retention_actor_activity`, ADR-0017). Each actor's cohort is
 * its earliest active day (`MIN(active_period)`); retention at day offset *N*
 * counts the distinct actors of that cohort active on cohort-day + *N*. Offset 0
 * is the cohort size. Pure relational group/self-join over the summary table —
 * never a raw scan (ADR-0015).
 */
@Injectable()
export class RetentionService {
  constructor(private readonly postgres: PostgresService) {}

  async compute(query: RetentionQuery): Promise<RetentionResponse> {
    const rows = await this.matrix(query);

    // Group the (cohort, offset, actors) rows into one entry per cohort day,
    // preserving the SQL's cohort-then-offset ordering.
    const byCohort = new Map<string, RetentionCohort>();
    for (const row of rows) {
      let cohort = byCohort.get(row.cohort);
      if (!cohort) {
        cohort = { cohort: row.cohort, cohortSize: 0, offsets: [] };
        byCohort.set(row.cohort, cohort);
      }
      cohort.offsets.push({ offset: row.day_offset, actors: row.actors });
      if (row.day_offset === 0) cohort.cohortSize = row.actors;
    }

    return {
      projectId: query.projectId,
      granularity: RETENTION_GRANULARITY,
      cohorts: [...byCohort.values()],
    };
  }

  private async matrix(query: RetentionQuery): Promise<MatrixRow[]> {
    const { projectId, from, to, maxOffset } = query;
    const sql = `
      WITH first_seen AS (
        SELECT actor_id, min(active_period) AS cohort
        FROM retention_actor_activity
        WHERE project_id = $1
        GROUP BY actor_id
      ),
      matrix AS (
        SELECT f.cohort, (a.active_period - f.cohort) AS day_offset, a.actor_id
        FROM retention_actor_activity a
        JOIN first_seen f ON f.actor_id = a.actor_id
        WHERE a.project_id = $1
          AND f.cohort >= $2::date AND f.cohort <= $3::date
      )
      SELECT to_char(cohort, 'YYYY-MM-DD') AS cohort,
             day_offset,
             count(DISTINCT actor_id)::int AS actors
      FROM matrix
      WHERE day_offset >= 0 AND day_offset <= $4
      GROUP BY cohort, day_offset
      ORDER BY cohort, day_offset`;

    // Eventually-consistent analytics read → replica (ADR-0019 §2).
    const { rows } = await this.postgres.replicaQuery<MatrixRow>(sql, [
      projectId,
      from,
      to,
      maxOffset,
    ]);
    return rows;
  }
}
