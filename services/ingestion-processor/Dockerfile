# syntax=docker/dockerfile:1
# Multi-stage build for the Ingestion-Processor (hybrid Kafka consumer + HTTP
# health server). Build context is the REPO ROOT (docker-compose sets
# `context: ..`) so the @cascade/contracts workspace dependency resolves.
# Run from repo root:
#   docker build -f services/ingestion-processor/Dockerfile -t cascade-ingestion-processor .

# ---- builder: install everything, build contracts + this service, drop dev deps ----
FROM node:22-slim AS builder
WORKDIR /app
COPY . .
RUN npm ci
RUN npm run build -w @cascade/contracts && npm run build -w @cascade/ingestion-processor
RUN npm prune --omit=dev

# ---- runtime: slim image with only prod deps + built dist ----
FROM node:22-slim AS runtime
ENV NODE_ENV=production
WORKDIR /app
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/package.json ./package.json
COPY --from=builder /app/libs/contracts/package.json ./libs/contracts/package.json
COPY --from=builder /app/libs/contracts/dist ./libs/contracts/dist
COPY --from=builder /app/services/ingestion-processor/package.json ./services/ingestion-processor/package.json
COPY --from=builder /app/services/ingestion-processor/dist ./services/ingestion-processor/dist
# Migrations are applied by the service on bootstrap; ship them for `npm run migrate` too.
COPY --from=builder /app/services/ingestion-processor/migrations ./services/ingestion-processor/migrations
USER node
CMD ["node", "services/ingestion-processor/dist/main.js"]
