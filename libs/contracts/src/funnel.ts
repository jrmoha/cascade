import { z } from 'zod';

/**
 * Funnel contract (KAN-35, ADR-0015 §2 / ADR-0017). A funnel measures how many
 * **actors** progress through an *ordered* sequence of event types within a time
 * window. Unlike counters/leaderboards there is no predefined funnel object: the
 * Aggregator maintains a generic per-`(project, actor, eventType)` first-seen
 * table and the Query API computes conversions for the step list passed on the
 * request, so any funnel is ad-hoc. These schemas are the single source of truth
 * for the request params and the response shape, shared by the Query API and its
 * tests.
 *
 * "Ordered" means an actor counts at step *k* only if it performed steps
 * `1..k` with **non-decreasing** event-time timestamps — i.e. it actually moved
 * through them in sequence, not merely performed them in any order.
 */

/** A funnel must have at least an entry + one conversion step. */
export const FUNNEL_MIN_STEPS = 2;
/** Upper bound on steps — bounds the per-step pivot the funnel query builds. */
export const FUNNEL_MAX_STEPS = 10;

/**
 * The ordered list of event types that define a funnel: 2–10 **distinct**,
 * non-empty event types. The Query API parses `?steps=a,b,c` into this.
 */
export const funnelStepsSchema = z
  .array(z.string().min(1))
  .min(FUNNEL_MIN_STEPS)
  .max(FUNNEL_MAX_STEPS)
  .refine((steps) => new Set(steps).size === steps.length, {
    message: 'funnel steps must be distinct event types',
  });
export type FunnelSteps = z.infer<typeof funnelStepsSchema>;

/**
 * One step's result. `step` is **1-based** (1 = entry). `actors` is the number
 * of distinct actors that reached this step in order within the window.
 * `conversionRate` is `actors / actors-at-step-1` (so step 1 is always `1`),
 * i.e. cumulative conversion from the top of the funnel.
 */
export const funnelStepResultSchema = z.object({
  step: z.number().int().positive(),
  eventType: z.string(),
  actors: z.number().int().nonnegative(),
  conversionRate: z.number().min(0).max(1),
});
export type FunnelStepResult = z.infer<typeof funnelStepResultSchema>;

/** `GET /funnel` — ordered conversion for a `(projectId, steps, [from,to])`. */
export const funnelResponseSchema = z.object({
  projectId: z.string(),
  from: z.string(),
  to: z.string(),
  steps: z.array(funnelStepResultSchema),
});
export type FunnelResponse = z.infer<typeof funnelResponseSchema>;
