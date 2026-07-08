/* eslint-disable no-undef */
// kafka-load.mjs — KAN-40 / ADR-0020
//
// Synthetic load generator for the partitioning / scaling demo. Produces valid
// `RawEvent` envelopes straight to `raw-events` (bypassing the Collector — no API
// key needed; the Ingestion-Processor validates and persists them the same way),
// keyed exactly like the Collector (`sessionId ?? actorId ?? eventId`) so a busy
// project spreads across all partitions while a session stays ordered on one.
//
// Env:
//   BROKERS   comma-separated host brokers (default localhost:9092,9094,9095)
//   COUNT     number of events to produce   (default 20000)
//   SESSIONS  distinct sessionIds to cycle  (default 2000 — controls spread)
//   PROJECT   projectId stamped on events   (default demo-project — one hot tenant)
//   ACKS      producer acks: -1=all,1,0     (default -1, matches prod durability)
//
// Usage: node infra/scripts/kafka-load.mjs   (from repo root, kafkajs resolves)
import { randomUUID } from 'node:crypto';
import { Kafka, Partitioners, logLevel } from 'kafkajs';

const BROKERS = (process.env.BROKERS ?? 'localhost:9092,localhost:9094,localhost:9095').split(',');
const COUNT = Number(process.env.COUNT ?? 20000);
const SESSIONS = Number(process.env.SESSIONS ?? 2000);
const PROJECT = process.env.PROJECT ?? 'demo-project';
const ACKS = Number(process.env.ACKS ?? -1);
const TOPIC = 'raw-events'; // = @cascade/contracts RAW_EVENTS_TOPIC (shell/mjs can't import TS)
const BATCH = 1000;

const kafka = new Kafka({
  clientId: 'cascade-kafka-load',
  brokers: BROKERS,
  logLevel: logLevel.ERROR,
});
// DefaultPartitioner = Java-compatible murmur2(key) — same as the Collector/ADR-0002.
const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });

function event(i) {
  const now = new Date().toISOString();
  const sessionId = `sess-${i % SESSIONS}`;
  return {
    eventId: randomUUID(),
    projectId: PROJECT,
    schemaVersion: 1,
    type: 'level_complete',
    occurredAt: now,
    receivedAt: now,
    payload: { level: (i % 20) + 1, score: i % 1000, playerId: `player-${i % SESSIONS}` },
    sessionId,
    actorId: `player-${i % SESSIONS}`,
    source: 'kafka-load@1.0.0',
  };
}

async function main() {
  const started = Date.now();
  await producer.connect();
  console.log(
    `producing ${COUNT} events to ${TOPIC} across ${SESSIONS} sessions ` +
      `(project=${PROJECT}, acks=${ACKS}) via ${BROKERS.join(',')}`,
  );
  let sent = 0;
  for (let base = 0; base < COUNT; base += BATCH) {
    const n = Math.min(BATCH, COUNT - base);
    const messages = Array.from({ length: n }, (_, j) => {
      const e = event(base + j);
      // Key = sessionId ?? actorId ?? eventId (here sessionId is always set).
      return { key: e.sessionId ?? e.actorId ?? e.eventId, value: JSON.stringify(e) };
    });
    await producer.send({ topic: TOPIC, acks: ACKS, messages });
    sent += n;
    if (sent % 5000 === 0 || sent === COUNT) process.stdout.write(`  sent ${sent}/${COUNT}\r`);
  }
  await producer.disconnect();
  const secs = (Date.now() - started) / 1000;
  console.log(`\ndone: ${sent} events in ${secs.toFixed(1)}s (${Math.round(sent / secs)}/s)`);
}

main().catch((err) => {
  console.error('kafka-load failed:', err.message);
  process.exit(1);
});
