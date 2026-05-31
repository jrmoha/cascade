import { describe, expect, it, vi, afterEach } from 'vitest';
import { recentHourlyBuckets, toHourlyBucket } from '../src/time-window';

describe('toHourlyBucket', () => {
  afterEach(() => vi.useRealTimers());

  it('truncates an ISO timestamp to the UTC hour', () => {
    expect(toHourlyBucket('2026-05-30T15:16:50.165Z')).toBe('2026-05-30T15');
  });

  it('normalizes a non-UTC offset to UTC before bucketing', () => {
    // 2026-05-30T01:30+03:00 === 2026-05-29T22:30Z -> hour 22 on the 29th
    expect(toHourlyBucket('2026-05-30T01:30:00.000+03:00')).toBe('2026-05-29T22');
  });

  it('keeps events in the same bucket within the hour and splits across the boundary', () => {
    expect(toHourlyBucket('2026-05-30T15:00:00.000Z')).toBe('2026-05-30T15');
    expect(toHourlyBucket('2026-05-30T15:59:59.999Z')).toBe('2026-05-30T15');
    expect(toHourlyBucket('2026-05-30T16:00:00.000Z')).toBe('2026-05-30T16');
  });

  it('falls back to the current hour for missing or unparseable input', () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-01-02T09:45:00.000Z'));
    expect(toHourlyBucket(undefined)).toBe('2026-01-02T09');
    expect(toHourlyBucket('not-a-date')).toBe('2026-01-02T09');
  });
});

describe('recentHourlyBuckets', () => {
  afterEach(() => vi.useRealTimers());

  it('returns just the current bucket for hours = 1', () => {
    expect(recentHourlyBuckets('2026-05-30T15:16:50.165Z', 1)).toEqual(['2026-05-30T15']);
  });

  it('walks back, most-recent first, across day and month boundaries', () => {
    expect(recentHourlyBuckets('2026-05-30T01:00:00.000Z', 3)).toEqual([
      '2026-05-30T01',
      '2026-05-30T00',
      '2026-05-29T23',
    ]);
  });

  it('clamps hours to at least 1', () => {
    expect(recentHourlyBuckets('2026-05-30T15:00:00.000Z', 0)).toEqual(['2026-05-30T15']);
    expect(recentHourlyBuckets('2026-05-30T15:00:00.000Z', -5)).toEqual(['2026-05-30T15']);
  });

  it('defaults the anchor to now when not given', () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-01-02T09:45:00.000Z'));
    expect(recentHourlyBuckets(undefined, 2)).toEqual(['2026-01-02T09', '2026-01-02T08']);
  });
});
