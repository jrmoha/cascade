// KAN-42 / ADR-0021 — ingestion resilience load test.
//
// A traffic spike against the Collector's POST /collect. It proves the two
// things the ticket asks for under load: the service stays *responsive* (p95
// latency threshold) and it never loses *accepted* data — every 202 is later
// reconciled against the Kafka topic by reconcile.mjs.
//
// Graceful shedding is expected, not a failure: a 429 (rate-limited) or 503
// (backpressure / produce retries exhausted) means the system degraded
// gracefully rather than falling over. Only 5xx-other and network errors count
// as real failures. The trimmed load stack sets generous limits so the spike
// mostly lands as 202s; the shedding paths themselves are covered by the unit +
// integration tests.
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const COLLECTOR_URL = __ENV.COLLECTOR_URL || 'http://localhost:3001';
const API_KEY = __ENV.API_KEY;

const accepted = new Counter('accepted_events');

// 202 (accepted) and the graceful-shed statuses (429/503) are all "expected", so
// they do not count towards http_req_failed. Anything else (5xx, network) does.
http.setResponseCallback(http.expectedStatuses(202, 429, 503));

export const options = {
  scenarios: {
    spike: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '10s', target: 50 }, // warm up
        { duration: '20s', target: 200 }, // the spike
        { duration: '10s', target: 0 }, // ramp down
      ],
      gracefulStop: '10s',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'], // ~no unexpected failures under the spike
    http_req_duration: ['p(95)<800'], // stays responsive
  },
};

export default function () {
  if (!API_KEY) {
    throw new Error('API_KEY env is required (run via infra/scripts/load-test.sh)');
  }
  const res = http.post(
    `${COLLECTOR_URL}/collect`,
    JSON.stringify({
      type: 'level_complete',
      occurredAt: new Date().toISOString(),
      payload: {
        level: Math.floor(Math.random() * 100) + 1,
        score: Math.floor(Math.random() * 10000),
      },
      sessionId: `sess-${__VU}`,
      actorId: `player-${__VU}-${__ITER}`,
    }),
    { headers: { 'Content-Type': 'application/json', 'x-api-key': API_KEY } },
  );

  check(res, {
    'accepted or gracefully shed (202/429/503)': (r) => [202, 429, 503].includes(r.status),
  });
  if (res.status === 202) {
    accepted.add(1);
  }
}

// Write the accepted (202) count where reconcile.mjs can pick it up, plus a
// short human summary on stdout (no remote jslib import).
export function handleSummary(data) {
  const acceptedCount = data.metrics.accepted_events?.values?.count ?? 0;
  const p95 = data.metrics.http_req_duration?.values?.['p(95)'] ?? null;
  const failedRate = data.metrics.http_req_failed?.values?.rate ?? null;
  const total = data.metrics.http_reqs?.values?.count ?? 0;

  const summary = { acceptedEvents: acceptedCount, totalRequests: total, p95Ms: p95, failedRate };
  const line =
    `\n=== ingest spike ===\n` +
    `total requests : ${total}\n` +
    `accepted (202) : ${acceptedCount}\n` +
    `p95 latency    : ${p95 === null ? 'n/a' : `${p95.toFixed(1)}ms`}\n` +
    `failed rate    : ${failedRate === null ? 'n/a' : failedRate.toFixed(4)}\n`;

  return {
    stdout: line,
    'infra/load/summary.json': JSON.stringify(summary, null, 2),
  };
}
