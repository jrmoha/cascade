import { describe, expect, it, vi, afterEach } from 'vitest';
import { hourlyBucketRange, toHourlyBucket } from '../src/time-window';

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

describe('hourlyBucketRange', () => {
  it('returns the single bucket when from and to fall in the same hour', () => {
    expect(hourlyBucketRange('2026-05-30T15:05:00.000Z', '2026-05-30T15:55:00.000Z')).toEqual([
      '2026-05-30T15',
    ]);
  });

  it('includes both endpoint hours, newest first', () => {
    expect(hourlyBucketRange('2026-05-30T13:30:00.000Z', '2026-05-30T15:10:00.000Z')).toEqual([
      '2026-05-30T15',
      '2026-05-30T14',
      '2026-05-30T13',
    ]);
  });

  it('floors each endpoint to its UTC hour', () => {
    // 13:59 and 15:00 still yield buckets 13, 14, 15.
    expect(hourlyBucketRange('2026-05-30T13:59:59.999Z', '2026-05-30T15:00:00.000Z')).toEqual([
      '2026-05-30T15',
      '2026-05-30T14',
      '2026-05-30T13',
    ]);
  });

  it('spans day and month boundaries', () => {
    expect(hourlyBucketRange('2026-05-29T23:00:00.000Z', '2026-05-30T01:00:00.000Z')).toEqual([
      '2026-05-30T01',
      '2026-05-30T00',
      '2026-05-29T23',
    ]);
  });

  it('normalizes non-UTC offsets before bucketing', () => {
    // 01:30+03:00 === 22:30Z on the 29th.
    expect(hourlyBucketRange('2026-05-30T01:30:00.000+03:00', '2026-05-30T00:10:00.000Z')).toEqual([
      '2026-05-30T00',
      '2026-05-29T23',
      '2026-05-29T22',
    ]);
  });

  it('returns an empty array when from is after to', () => {
    expect(hourlyBucketRange('2026-05-30T15:00:00.000Z', '2026-05-30T13:00:00.000Z')).toEqual([]);
  });

  it('returns an empty array for unparseable input', () => {
    expect(hourlyBucketRange('not-a-date', '2026-05-30T15:00:00.000Z')).toEqual([]);
    expect(hourlyBucketRange('2026-05-30T15:00:00.000Z', 'not-a-date')).toEqual([]);
  });
});
