/**
 * A bounded, non-blocking in-flight counter (KAN-42, ADR-0021).
 *
 * The Collector's backpressure mechanism: rather than let concurrent produces
 * queue up unboundedly (which just moves the failure to OOM), we cap how many
 * may be in flight at once. {@link tryAcquire} returns `false` the moment the
 * cap is reached — the caller then sheds load with a `503` instead of buffering.
 * There is deliberately **no** waiting queue: shedding fast is the point.
 */
export class InFlightLimiter {
  private inFlight = 0;

  constructor(private readonly max: number) {}

  /** Reserve a slot; `false` if already at capacity (caller should shed with 503). */
  tryAcquire(): boolean {
    if (this.inFlight >= this.max) {
      return false;
    }
    this.inFlight += 1;
    return true;
  }

  /** Release a previously acquired slot. */
  release(): void {
    if (this.inFlight > 0) {
      this.inFlight -= 1;
    }
  }

  /** Slots currently held — for observability/tests. */
  get current(): number {
    return this.inFlight;
  }
}
