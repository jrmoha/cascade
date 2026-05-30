# Cascade — Project Charter & Overview

### Real-Time Event Analytics Platform

> _Name is a placeholder — rename freely. This document is the "why": the business and product framing, the goals, what success looks like, and the technologies and the reasoning behind them. The detailed engineering roadmap lives in the separate blueprint document._

---

## 1. One-liner

A self-hostable, real-time event analytics platform that ingests high-volume events and turns them into live metrics, funnels, retention cohorts, and leaderboards — demonstrated on game telemetry.

---

## 2. Background & problem

Almost every modern application and game generates a constant stream of behavioral events: a player starts a match, a user opens a screen, a level is completed, a purchase is attempted. That stream is the richest signal a team has about what people actually do — yet most teams either don't capture it at all, or pipe it into an expensive third-party black box they don't control and can't extend.

There is room for a lean, self-hostable engine that an app or game team can run themselves to capture events at scale and get real-time behavioral insight, without surrendering their data or their bill to a SaaS vendor.

Underneath the product framing sits the engineering problem that makes this worth building: **the write path and the read path want opposite things.** Ingestion is write-heavy, append-only, and bursty. Dashboards are read-heavy and want pre-shaped answers fast. A naive single-database design fails at both ends. Designing a system that serves both well is the core challenge.

---

## 3. Purpose

This project has two honest, stacked purposes:

**Product purpose.** Give a team a way to capture behavioral events at scale and see real-time insight from them, on infrastructure they control.

**Personal purpose (the real driver).** This platform was deliberately chosen as a learning vehicle. Its workload forces hands-on mastery of the distributed-systems skills that mark the step from mid-level to senior backend engineer: write-optimized data modeling, event-driven architecture, microservice decomposition, replication and consistency tradeoffs, cloud deployment, and operating a system under load. The product is real, but the growth is the point — and that is stated openly rather than pretended away.

---

## 4. Vision — what it becomes

A backend platform that can:

- Accept a high, bursty volume of events through a thin, fast collector.
- Durably store every raw event, append-only, for replay and audit.
- Compute real-time aggregations (counters, funnels, retention, leaderboards) continuously as events arrive.
- Serve those aggregations to dashboards instantly, without ever scanning raw data live.
- Scale each part horizontally and survive the loss of a node without losing data or going dark.

The frontend dashboards are explicitly a later concern; the backend, deployment, and scaling are the focus.

---

## 5. Goals

**Product / business goals**

- Ingest events at high throughput with low rejection of valid data.
- Store raw events durably and queryably.
- Serve real-time aggregations fast enough to feel live.
- Scale horizontally on both the write and read sides.
- Run entirely on infrastructure the operator controls.

**Learning goals (the actual target)**

- Model data query-first in Cassandra and run it replicated across nodes.
- Build a genuinely event-driven system with Kafka as the backbone.
- Decompose the system into real, independently deployable microservices.
- Deploy to AWS using infrastructure as code.
- Prove the system under load and chaos, with full observability.
- Capture the reasoning behind every major decision in writing (ADRs).

---

## 6. Who it's for

The hypothetical operator is a small product or game team that wants behavioral analytics without a heavyweight SaaS dependency — technically capable enough to self-host, cost- or control-conscious enough to want to. The demo domain throughout is **game telemetry**: player sessions, match events, and live leaderboards, which keeps the work concrete and gives every feature a tangible face.

---

## 7. Value proposition

- **Ownership** — the operator keeps their event data and controls their costs.
- **Real-time** — insight as events arrive, not batched hours later.
- **Extensible** — self-hosted and open, so it can be shaped to the operator's needs.
- **Right-sized** — lean enough for a small team to actually run.

---

## 8. Scope

**In scope**

- Backend services: ingestion, raw storage, stream aggregation, query API, project/schema management.
- Deployment and horizontal scaling on AWS.
- Testing and validation: schema validation on ingest, integration, load, and chaos testing.
- Observability: metrics, dashboards, distributed tracing.

**Out of scope (for now)**

- The frontend/dashboard UI (built later).
- Billing and payment flows.
- Heavy multi-tenant SaaS productization.
- Anything e-commerce.

---

## 9. What's expected — deliverables & success criteria

**Deliverables**

- A running multi-service backend, deployed to AWS via infrastructure as code.
- A documentation set: this charter, the architecture overview, and a series of ADRs.
- A test suite spanning unit, integration, load, and chaos levels.
- Observability dashboards and at least two operational runbooks.

**Success criteria — when this counts as a win**

- The pipeline runs end-to-end on AWS: event in → stored → aggregated → queried out.
- It survives a load test _and_ a deliberately killed Cassandra node without data loss or downtime.
- Every significant architectural decision has an ADR behind it.
- The author can sit in a design review and defend each tradeoff with the workload and consistency model in hand.

That last criterion is the real bar. Seniority is not knowing the most tools — it is being able to justify _why_, with the workload and the tradeoffs in hand.

---

## 10. Technology stack & rationale

| Area                       | Choice                                | Why                                                                                              |
| -------------------------- | ------------------------------------- | ------------------------------------------------------------------------------------------------ |
| Services                   | NestJS / TypeScript                   | Build on existing strength; spend the learning budget on the new concepts                        |
| Event backbone             | Kafka                                 | A replayable, partitioned log with many independent consumers — the heart of event-driven design |
| Event store                | Cassandra                             | Write-optimized, time-series, query-first — the exact workload it was built for                  |
| Metadata store             | PostgreSQL                            | Relational config (projects, schemas, keys); replicas teach replication firsthand                |
| Cache / live counters      | Redis                                 | Leaderboards (sorted sets), rate limiting, live counters, pub/sub fan-out                        |
| Inter-service comms        | Kafka (async) + gRPC/REST (sync)      | The right transport for each interaction style                                                   |
| Containers & orchestration | Docker + Kubernetes (EKS)             | Industry-standard packaging and scaling                                                          |
| Infrastructure as code     | Terraform                             | Reproducible infra that documents itself                                                         |
| Observability              | OpenTelemetry + Prometheus + Grafana  | Tracing and metrics across service boundaries                                                    |
| Testing                    | Vitest, Testcontainers, k6, Toxiproxy | Unit → integration → load → chaos                                                                |

The selection method behind these picks: for each component, judge the workload shape, the consistency-vs-availability need, whether an existing tool already fits, the operational cost, and — because this is a learning project — what it teaches. Each of these will be recorded as an ADR as it's adopted.

---

## 11. Guiding principle

**Build a walking skeleton first** — the thinnest slice that touches every part of the system, deployed end-to-end — then flesh it out phase by phase. Breadth before depth, so integration risk dies early rather than at the end.
