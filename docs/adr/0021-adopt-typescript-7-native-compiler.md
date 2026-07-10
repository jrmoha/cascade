# 0021 — Adopt TypeScript 7 (native Go compiler) as the build compiler

**Status:** Accepted

## Context

[TypeScript 7.0](https://devblogs.microsoft.com/typescript/announcing-typescript-7-0/) — the
compiler and language service **rewritten in Go** (formerly "Project Corsa" / `tsgo`) — reached
**general availability on 2026-07-08**. Same language, no new syntax; the win is a **~10× faster
type-checker and build** (Microsoft quotes 8–12× on full builds) from native code + shared-memory
parallelism.

We evaluated it hands-on against this repo:

- **Our code is already compatible** — the whole graph types **clean (0 errors)** under TS7.
- **NestJS DI survives.** TS7 emits `emitDecoratorMetadata` (`__metadata("design:paramtypes", …)`),
  and the emitted CommonJS **loads and runs** — verified by requiring the built `@cascade/contracts`.
- **Only config migration was needed** (below); **no source changes**.

The blocker to a naive "just bump `typescript` to 7" is **the lint toolchain, and we proved it
empirically.** Upgrading `typescript` → 7.0.2 **and** `typescript-eslint` → its latest (8.63.0) makes
`npm run lint` **crash**:

```
TypeError: Cannot read properties of undefined (reading 'Cjs')
    at @typescript-eslint/typescript-estree/.../create-program/shared.js
```

`typescript-eslint` drives TS's **programmatic compiler API**, which the Go rewrite changed and does
**not stabilize until TS 7.1** ("several months out"). So no released `typescript-eslint` can run on
the TS7 compiler today. Since `npm run lint` is a CI gate step, a **single-compiler** TS7 setup is
not viable yet.

## Decision

Adopt TS7 as the **real build/gate/prod compiler now**, and keep TS5 alive **only** as the parser
engine `typescript-eslint` needs — two compilers, cleanly separated by role.

### 1. Modernize the `tsconfig`s (required by TS7)

Across `libs/contracts` + all five services + `e2e`: `moduleResolution: "node"` → `"nodenext"`,
`module: "commonjs"` → `"nodenext"` (still emits CommonJS — no package sets `"type": "module"`),
remove the removed `baseUrl`, and add `types: ["node"]` to `libs/contracts` (`grpc.ts` uses
`__dirname`/`node:path`). TS7 _removed_ (not just deprecated) `moduleResolution: "node"` and
`baseUrl`.

### 2. TypeScript 7 is the compiler; TypeScript 5.5 exists only for lint

- **`"typescript-7": "npm:typescript@7.0.2"`** (exact) in root `devDependencies` is the compiler.
  `npm run build`, `npm run typecheck`, and every workspace `build` script (hence the Dockerfiles,
  which build via `npm run build -w …`) invoke it. The `verify` CI job's `build` step is therefore
  gated on TS7.
- **`"typescript": "5.5.4"`** stays **only** because `typescript-eslint`'s parser `require`s the
  `typescript` package and supports `>=4.7.4 <5.6.0`. Nothing else references it. It is a lint
  implementation detail, not our compiler.

### 3. Path-qualify every `tsc` invocation

Both packages ship a binary named `tsc`, so they collide on `node_modules/.bin/tsc` with a
non-deterministic winner. Every `tsc` call is therefore an explicit path, never `.bin`:

- build / typecheck / workspace builds → `node …/node_modules/typescript-7/bin/tsc`
- lint resolves the `typescript` **package** (5.5.4) via node resolution, not `.bin` — unaffected.

## Alternatives considered

- **Single `typescript@7` for everything** (drop the alias). Simplest, but **empirically breaks
  lint** — `typescript-eslint` (even latest) can't run on the TS7 compiler API until 7.1. Rejected on
  evidence, not theory.
- **Keep TS5 as the gate, TS7 as a non-gating `typecheck:native` canary.** The conservative option
  (and what an earlier revision of this ADR chose). Rejected: it leaves builds and prod artifacts on
  the old compiler for no reason, since TS7 compiles our code clean and its emit loads and runs. We
  chose to actually _use_ TS7.
- **`@typescript/native-preview` (the `tsgo` nightly) instead of the stable alias.** Ships a
  distinct `tsgo` binary (no `.bin` collision, no path-qualification), but publishes **only
  dev/nightly** builds. Rejected in favour of pinning **GA 7.0.2**; we pay the path-qualification
  cost for a stable pin.

## Consequences

- **The gate and production artifacts are now compiled by TS7.** `npm run build` (root + per-service,
  incl. Docker) runs the native compiler; lint runs on TS5. The gate compiler and the lint compiler
  differ — an accepted divergence: both accept our code (verified 0 errors each), and they check
  complementary things (types vs. lint rules).
- **Accepted risk:** prod is emitted by a compiler that is days-old at GA. Mitigations in place —
  clean compile, decorator-metadata emit verified, and TS7-emitted CJS confirmed to load at runtime.
  **Recommended follow-up:** exercise a TS7-built Docker image end-to-end before a production cut
  (the in-process smoke test transpiles via SWC, so it does _not_ cover TS7 emit).
- **`nodenext` is the module mode** for services + contracts (was `commonjs`/`node`); emit stays
  CommonJS — a stricter _resolution_ mode, not an ESM migration.
- **Every `tsc` call is path-qualified** (root + six workspace `build` scripts) so the two
  `tsc`-named binaries can't collide. Dockerfiles are unchanged (they call `npm run build -w …`).
- **Collapse trigger:** when `typescript-eslint` ships TS7 support (tracking **TS 7.1**), delete the
  `typescript` 5.5 package, the `typescript-7` alias, and all path-qualification, collapsing to a
  single `typescript@7` with bare `tsc`. This ADR's split is a **transitional** state with a clear
  exit.
- **Discovered, out of scope:** the root `tsconfig.json` `references` list omits the `aggregator`
  service, so the root gate build doesn't cover it (it builds via its own workspace config). Noted
  for a follow-up; not changed here.

Complements [ADR-0012](0012-inter-service-contract-versioning.md) (the CI build gate) and
[ADR-0014](0014-nestjs-11-prisma-7-upgrade.md) (toolchain upgrades). Ticket: KAN-43.
