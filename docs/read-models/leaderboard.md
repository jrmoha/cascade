# Read model — live leaderboard (Redis sorted sets)

The Aggregator's second derived view (KAN-34): a per-project **live leaderboard** of
top players that updates as score events arrive, served from a **Redis sorted set
(ZSET)**. This is the deliberate "right tool" contrast to the Cassandra event counters
([event-counts.md](event-counts.md)) — Redis gives sub-millisecond ranked reads
(`ZREVRANGE`/`ZREVRANK`, all O(log n)) that Cassandra's query-first model isn't built
for. It is a worked example of the [ADR-0015](../adr/0015-read-model-aggregation-strategy.md)
strategy (§2 store choice, §4 idempotency, §5 rebuild).

> **Strategy vs. implementation.** The _why_ lives in
> [ADR-0015](../adr/0015-read-model-aggregation-strategy.md); this page is the _what_
> for the leaderboard view.

## What it serves

Two Query API endpoints, both read from Redis (never raw Cassandra):

- `GET /leaderboard?projectId=&period=&limit=` → **top-N** entries, highest score first
  (`ZREVRANGE … WITHSCORES`), each with a **1-based** `rank`.
- `GET /leaderboard/rank?projectId=&period=&playerId=` → one player's **rank + score**
  (`ZREVRANK` + `ZSCORE`); `404` when the player isn't on the board.

`period` is `alltime` (default) or a UTC calendar day `YYYY-MM-DD`; `limit` defaults to
100 (max 1000).

## Store & key scheme (Redis)

One sorted set per `(project, period)`, member = `playerId`, score = the player's score:

```
lb:{projectId}:{period}       period ∈ { alltime, YYYY-MM-DD }
  e.g.  lb:game-1:alltime      lb:game-1:2026-05-30
```

The key is built by the shared helper `leaderboardKey(projectId, period)` in
[`libs/contracts/src/leaderboard.ts`](../../libs/contracts/src/leaderboard.ts), imported
by **both** the Aggregator (writer) and the Query API (reader) so the scheme can never
drift — the same discipline `time-window.ts` applies to the Cassandra bucket keys.

**Two time scopes:**

- **All-time** (`alltime`) — never expires.
- **Daily** (`YYYY-MM-DD`) — bucketed by the event's **`occurredAt`** (event time, so a
  late event lands on the day it happened — ADR-0015 §3), and given/refreshed a TTL on
  each write (`AGGREGATOR_LEADERBOARD_DAILY_TTL_SECONDS`) so idle days self-expire.

## What counts as a score event

Any event whose `payload` carries a **non-empty string `playerId`** and a **finite
numeric `score`** updates the boards. Events without both (most event types) simply don't
touch a leaderboard — no error. `actorId` is not used for this view; the player id is
read from the payload.

## Idempotency — best score via `ZADD GT`

The Aggregator applies each score with `ZADD {key} GT {score} {playerId}`: a member's
score only ever moves **up** (`GT` = greater-than). This is **naturally idempotent and
replay-safe** — re-applying the same event (Kafka at-least-once, or a full replay from
offset 0) is a no-op, and a later _lower_ score never lowers a player. So unlike the
additive counters, the leaderboard needs **no dedup gate of its own** (ADR-0016 §1); it
rides the controller's shared per-`eventId` gate only as a harmless superset.

The write is applied in the Aggregator's valid-event branch alongside the counter
increment, in its **own** bounded retry (so a Redis hiccup never re-runs the additive
counter `+1`); if it ultimately fails, the event is dead-lettered like any other
persistence failure (ADR-0006).

## Rebuild

Every board is a pure, deterministic function of the log. To rebuild: `DEL` the board
keys (`lb:{projectId}:*`) and replay `raw-events` from offset 0 with the Aggregator.
Because `ZADD GT` is idempotent, **no dedup flush is required** for this view (in
contrast to the counters — ADR-0016 §3). The replay reproduces each board exactly.

## Config

- `AGGREGATOR_LEADERBOARD_DAILY_TTL_SECONDS` (Aggregator, required) — retention for the
  daily boards; bounded above by the raw-events 30-day TTL.
- `REDIS_HOST` / `REDIS_PORT` (Query API, required) — the Query API now reads this Redis
  read model, so Redis is one of its readiness deps.

## Tests

- **Unit** —
  [`aggregator/test/leaderboard.repository.spec.ts`](../../services/aggregator/test/leaderboard.repository.spec.ts):
  eligible event → `ZADD GT` on the all-time + daily keys with the daily TTL; ineligible
  (no `playerId` / non-numeric `score`) → no-op; daily bucket derives from `occurredAt`.
  [`query-api/test/leaderboard.service.spec.ts`](../../services/query-api/test/leaderboard.service.spec.ts):
  `ZREVRANGE`→1-based entries, `ZREVRANK`+`ZSCORE`→rank/score, absent player → 404.
- **Integration** —
  [`aggregator/test/leaderboard.e2e-spec.ts`](../../services/aggregator/test/leaderboard.e2e-spec.ts)
  (real Kafka + Cassandra + Redis): a sequence of score events → exact top-N order, a
  player's rank, best-score (a later lower score doesn't lower the player), and both the
  all-time and daily boards populated with the daily TTL set.
  [`query-api/test/leaderboard.redis.e2e-spec.ts`](../../services/query-api/test/leaderboard.redis.e2e-spec.ts)
  (real Redis): `GET /leaderboard` ordering + limit, `GET /leaderboard/rank` rank/score,
  the 404 and the malformed-`period` 400.
