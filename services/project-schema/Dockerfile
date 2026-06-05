# syntax=docker/dockerfile:1
# Multi-stage build for the Project/Schema service (HTTP API over Postgres).
# Build context is the REPO ROOT (docker-compose sets `context: ..`) so the
# @cascade/contracts workspace dependency resolves.
# Run from repo root:
#   docker build -f services/project-schema/Dockerfile -t cascade-project-schema .

# ---- builder: install everything, generate Prisma client, build, drop dev deps ----
FROM node:22-slim AS builder
WORKDIR /app
COPY . .
RUN npm ci
# `build` runs `prisma generate` then `tsc -b`, emitting the Prisma client into
# node_modules so it survives the prune below.
RUN npm run build -w @cascade/contracts && npm run build -w @cascade/project-schema
RUN npm prune --omit=dev

# ---- runtime: slim image with only prod deps + built dist ----
FROM node:22-slim AS runtime
ENV NODE_ENV=production
WORKDIR /app
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/package.json ./package.json
COPY --from=builder /app/libs/contracts/package.json ./libs/contracts/package.json
COPY --from=builder /app/libs/contracts/dist ./libs/contracts/dist
# The gRPC server loads project_schema.proto at boot via @grpc/proto-loader;
# ship it alongside dist (PROJECT_SCHEMA_PROTO_PATH resolves dist/../proto).
COPY --from=builder /app/libs/contracts/proto ./libs/contracts/proto
COPY --from=builder /app/services/project-schema/package.json ./services/project-schema/package.json
COPY --from=builder /app/services/project-schema/dist ./services/project-schema/dist
# Ship the Prisma schema + migrations and the Prisma 7 config: the service
# applies `migrate deploy --config prisma.config.ts` on boot (it carries the
# Migrate datasource URL + schema/migrations paths), and they back
# `npm run migrate` too. `prisma` is a runtime dependency (kept past the prune)
# so the migrate CLI is available in the container.
COPY --from=builder /app/services/project-schema/prisma ./services/project-schema/prisma
COPY --from=builder /app/services/project-schema/prisma.config.ts ./services/project-schema/prisma.config.ts
USER node
CMD ["node", "services/project-schema/dist/main.js"]
