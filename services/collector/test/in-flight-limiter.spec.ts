import { describe, expect, it } from 'vitest';
import { InFlightLimiter } from '../src/collector/in-flight-limiter';

describe('InFlightLimiter', () => {
  it('grants slots up to the cap, then refuses', () => {
    const limiter = new InFlightLimiter(2);
    expect(limiter.tryAcquire()).toBe(true);
    expect(limiter.tryAcquire()).toBe(true);
    expect(limiter.tryAcquire()).toBe(false); // at capacity
    expect(limiter.current).toBe(2);
  });

  it('frees a slot on release so a later acquire succeeds', () => {
    const limiter = new InFlightLimiter(1);
    expect(limiter.tryAcquire()).toBe(true);
    expect(limiter.tryAcquire()).toBe(false);
    limiter.release();
    expect(limiter.current).toBe(0);
    expect(limiter.tryAcquire()).toBe(true);
  });

  it('never drops below zero on an over-release', () => {
    const limiter = new InFlightLimiter(1);
    limiter.release();
    limiter.release();
    expect(limiter.current).toBe(0);
  });
});
