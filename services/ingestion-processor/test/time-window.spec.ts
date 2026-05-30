import { describe, expect, it, vi, afterEach } from 'vitest';
import { toHourlyWindow } from '../src/processor/time-window';

describe('toHourlyWindow', () => {
  afterEach(() => vi.useRealTimers());

  it('truncates an ISO timestamp to the UTC hour', () => {
    expect(toHourlyWindow('2026-05-30T15:16:50.165Z')).toBe('2026-05-30T15');
  });

  it('normalizes a non-UTC offset to UTC before bucketing', () => {
    // 2026-05-30T01:30+03:00 === 2026-05-29T22:30Z -> hour 22 on the 29th
    expect(toHourlyWindow('2026-05-30T01:30:00.000+03:00')).toBe('2026-05-29T22');
  });

  it('keeps events in the same bucket within the hour and splits across the boundary', () => {
    expect(toHourlyWindow('2026-05-30T15:00:00.000Z')).toBe('2026-05-30T15');
    expect(toHourlyWindow('2026-05-30T15:59:59.999Z')).toBe('2026-05-30T15');
    expect(toHourlyWindow('2026-05-30T16:00:00.000Z')).toBe('2026-05-30T16');
  });

  it('falls back to the current hour for missing or unparseable input', () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-01-02T09:45:00.000Z'));
    expect(toHourlyWindow(undefined)).toBe('2026-01-02T09');
    expect(toHourlyWindow('not-a-date')).toBe('2026-01-02T09');
  });
});
