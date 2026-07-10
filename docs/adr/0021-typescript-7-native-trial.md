# 0021 — Trial TypeScript 7 (native compiler) as a non-gating typecheck

**Status:** Accepted

## Context

[TypeScript 7.0](https://devblogs.microsoft.com/typescript/announcing-typescript-7-0/) — the
compiler and language service **rewritten in Go** (formerly "Project Corsa" / `tsgo`) — reached
**general availability on 2026-07-08**. It is the same language, not new syntax: the win is a
**~10× faster type-checker and build** (Microsoft quotes 8–12× on full builds), driven by native
code and shared-memory parallelism. The question this ADR settles is whether Cascade should adopt
it, and how far.

We evaluated the native compiler (`@typescript/native-preview`, `tsgo`) hands-on against this repo:

- **Our code is already compatible.** With configs modernized (below), `tsgo --build` types the
  whole graph with **0 errors** across all services + `@cascade/contracts`.
- **NestJS DI survives.** The native compiler **emits `emitDecoratorMetadata`**
  (`__metadata("design:paramtypes", …)`) — the feared "decorators break under tsgo" blocker
  **does not exist** for us. Runtime DI is unaffected (proven by the smoke test).
- **It is faster even here.** On this small repo the native `--build` runs in ~0.5–0.6s wall vs
  `tsc`'s ~1.9s (~3×, at 300–400% CPU) — a gap that widens as the codebase grows.

Two frictions keep it from being a drop-in replacement **today**:

1. **Removed config options.** TS7 _removed_ (not just deprecated) `moduleResolution: "node"` and
   `baseUrl`, both of which every service `tsconfig` used. `tsgo` refuses to run until they're gone.
2. **No stable programmatic compiler API until TS 7.1.** TS 7.0 ships **without** the stable API,
   which lands in 7.1 ("several months out"). Tooling that drives the compiler API —
   `typescript-eslint`'s parser, `ts-morph`, custom transformers — can't run on the TS7 compiler
   yet. Our lint is **syntactic** (`tseslint.configs.recommended`, no `parserOptions.project`), so
   the coupling is lighter than a type-checked config, but the parser still needs a `typescript`
   peer it supports — so we **cannot make TS7 the sole `typescript` install** while lint depends on
   the 5.x/6.x API.

## Decision

Adopt the **low-risk half now**; defer the cutover until the ecosystem catches up.

### 1. Modernize the `tsconfig`s (accepted by both `tsc` 5.5 and `tsgo`)

`nodenext` is fully supported by our current `tsc` 5.5, so this is a **one-time cleanup that keeps
the existing gate green** while also unblocking the native compiler. Across `libs/contracts` and all
five service configs (`collector`, `ingestion-processor`, `project-schema`, `query-api`,
`aggregator`) + `e2e`:

- `"moduleResolution": "node"` → `"nodenext"`
- `"module": "commonjs"` → `"nodenext"` (still emits CommonJS — no service package sets
  `"type": "module"`, so `nodenext` resolves each service as CJS)
- remove `"baseUrl": "."` (nothing depended on it; imports are relative or the `@cascade/contracts`
  workspace package)
- `libs/contracts`: add `"types": ["node"]` (`grpc.ts` uses `__dirname` / `node:path`)

### 2. Add `tsgo` as a **non-gating** fast typecheck

- `@typescript/native-preview` pinned in root `devDependencies` (exact dev build).
- Root script `"typecheck:native": "tsgo --build"` alongside `build` / `typecheck`.
- A **separate CI job** `typecheck-native` (`.github/workflows/ci.yml`) runs it with
  **`continue-on-error: true`** — informational, **does not block merge**. The existing `verify`
  job (`tsc --build`) stays the real gate.

`tsc` remains the **source of truth** for the build and CI gate. The native job is a fast-feedback
canary for TS7 drift, adopted with zero source changes and trivially reversible.

## Alternatives considered

- **Full cutover now** (make `tsgo` the gating typecheck; add `@typescript/typescript6`'s `tsc6`
  side-by-side to keep `typescript-eslint` working). More moving parts — a second compiler package
  and a split between "compile with 7, lint with 6" — for a Phase-0 skeleton where `tsc` is not a
  bottleneck. Rejected until TS 7.1 gives `typescript-eslint` a stable API to target.
- **Config modernization only** (drop `node`/`baseUrl`, no `tsgo`). Leaves the speed win and the
  drift-canary on the table for no extra risk. Rejected in favour of also wiring the non-gating job.
- **Do nothing.** Fine, but `moduleResolution: "node"` / `baseUrl` are already removed in the
  toolchain we'll eventually move to; modernizing now is free under `tsc` 5.5 and de-risks the
  eventual switch. Rejected.
- **Make `tsgo` emit production `dist`** (replace `tsc -b`). Out of scope — emit parity isn't the
  blocker (metadata emit works), but there's no reason to move the emitter before the checker is the
  gate. Deferred.

## Consequences

- **The `tsc` gate is unchanged and still green:** `npm run build`, `npm run lint`,
  `npm run format:check`, `npm run test:ci`, and — critically — **`npm run smoke` (2/2 passing)**
  all pass under `nodenext`, proving the `commonjs`→`nodenext` + `baseUrl` removal did **not** change
  runtime module resolution or NestJS DI.
- **New non-gating signal:** the `typecheck-native` CI job surfaces TS7 incompatibilities early
  without blocking anyone. Locally, `npm run typecheck:native` gives a ~3× faster whole-graph check.
- **`nodenext` is now the module mode** for services + contracts (was `commonjs`/`node`). Emit is
  still CommonJS; this is a stricter _resolution_ mode, not an ESM migration.
- **Revisit trigger:** when `typescript-eslint` ships TS7-native support (tracking **TS 7.1**),
  re-evaluate making `tsgo` the primary/gating typecheck and dropping `tsc` — the code/emit side is
  already proven, leaving only the lint-toolchain dependency.
- **Discovered, out of scope:** the root `tsconfig.json` `references` list does not include the
  `aggregator` service, so neither `tsc --build` nor `tsgo --build` covers it from the root graph
  (it is built via its own workspace config). Noted for a follow-up; not changed here.

This ADR complements [ADR-0012](0012-inter-service-contract-versioning.md) (the `tsc --build` CI
gate) and [ADR-0014](0014-nestjs-11-prisma-7-upgrade.md) (toolchain upgrades). Ticket: KAN-43.
