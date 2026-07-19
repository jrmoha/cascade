// KAN-42 — reconcile accepted events against Kafka after the k6 spike.
//
// The AC#5 assertion: every event the Collector *accepted* (202) must be present
// on `raw-events`. We sum the topic's end offsets (fresh topic ⇒ total messages
// produced) and compare to the accepted count k6 wrote to summary.json.
//
//   produced <  accepted  → DATA LOSS  → exit 1 (the load test fails)
//   produced >  accepted  → duplicates from at-least-once produce retry — an
//                           accepted trade-off (downstream is idempotent, ADR-0016)
//   produced == accepted  → clean
import { readFileSync } from 'node:fs';
import { Kafka } from 'kafkajs';

const BROKER = process.env.KAFKA_BROKER || 'localhost:9192';
const TOPIC = process.env.RAW_EVENTS_TOPIC || 'raw-events';
const SUMMARY = process.env.SUMMARY_PATH || 'infra/load/summary.json';

const { acceptedEvents } = JSON.parse(readFileSync(SUMMARY, 'utf8'));

const kafka = new Kafka({ clientId: 'load-reconcile', brokers: [BROKER] });
const admin = kafka.admin();
await admin.connect();
const offsets = await admin.fetchTopicOffsets(TOPIC);
await admin.disconnect();

// `offset` is the high watermark (next offset). On a fresh topic (low = 0) the
// sum across partitions is the total number of messages produced.
const produced = offsets.reduce((sum, p) => sum + Number(p.offset), 0);

console.log(`accepted (202)          : ${acceptedEvents}`);
console.log(`kafka raw-events records: ${produced}`);

if (produced < acceptedEvents) {
  console.error(
    `FAIL — DATA LOSS: ${acceptedEvents - produced} accepted event(s) missing from Kafka`,
  );
  process.exit(1);
}
if (produced > acceptedEvents) {
  console.warn(
    `note: ${produced - acceptedEvents} duplicate(s) from at-least-once produce retry (acceptable; downstream is idempotent)`,
  );
}
console.log('PASS — no accepted data lost');
