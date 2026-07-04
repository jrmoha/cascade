import { z } from 'zod';

/**
 * Retention contract (KAN-35, ADR-0015 §2 / ADR-0017). Retention measures, for
 * each **cohort** (actors first seen on a given UTC day), how many return on
 * subsequent days. The Aggregator records, idempotently, the set of UTC days
 * each actor was active; the Query API derives each actor's cohort as the
 * earliest active day (`MIN`) and counts distinct returning actors per day
 * offset, producing the classic cohort triangle.
 *
 * Granularity is the UTC calendar **day** (consistent with the leaderboard's
 * daily period). These schemas are the single source of truth for the response
 * shape, shared by the Query API and its tests.
 */

/** Retention is bucketed by UTC calendar day. */
export const RETENTION_GRANULARITY = 'day' as const;

/** Default day offset depth (cohort day 0 … N) when the request omits it. */
export const RETENTION_DEFAULT_MAX_OFFSET = 7;
/** Hard cap on day-offset depth, bounding the cohort triangle width. */
export const RETENTION_MAX_OFFSET = 90;
/** Hard cap on the cohort-day range `[from, to]` (inclusive), in days. */
export const RETENTION_MAX_COHORT_DAYS = 92;

/** Distinct returning actors at a given day offset from the cohort day. */
export const retentionOffsetSchema = z.object({
  /** Days since the cohort day; `0` is the cohort day itself. */
  offset: z.number().int().nonnegative(),
  actors: z.number().int().nonnegative(),
});
export type RetentionOffset = z.infer<typeof retentionOffsetSchema>;

/** One cohort row: its day, its size (offset-0 actors), and its retention curve. */
export const retentionCohortSchema = z.object({
  /** Cohort day, UTC `YYYY-MM-DD`. */
  cohort: z.string(),
  cohortSize: z.number().int().nonnegative(),
  offsets: z.array(retentionOffsetSchema),
});
export type RetentionCohort = z.infer<typeof retentionCohortSchema>;

/** `GET /retention` — cohort matrix for a `(projectId, [from,to] cohort range)`. */
export const retentionResponseSchema = z.object({
  projectId: z.string(),
  granularity: z.literal(RETENTION_GRANULARITY),
  cohorts: z.array(retentionCohortSchema),
});
export type RetentionResponse = z.infer<typeof retentionResponseSchema>;
