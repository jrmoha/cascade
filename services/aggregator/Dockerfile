# syntax=docker/dockerfile:1
# Multi-stage build for the Aggregator (hybrid Kafka consumer + HTTP health
# server). Build context is the REPO ROOT (docker-compose sets `context: ..`)
# so the @cascade/contracts workspace dependency resolves.
# Run from repo root:
#   docker build -f services/aggregator/Dockerfile -t cascade-aggregator .

# ---- builder: install everything, build contracts + this service, drop dev deps ----
FROM node:22-slim AS builder
WORKDIR /app
COPY . .
RUN npm ci
RUN npm run build -w @cascade/contracts && npm run build -w @cascade/aggregator
RUN npm prune --omit=dev

# ---- runtime: slim image with only prod deps + built dist ----
FROM node:22-slim AS runtime
ENV NODE_ENV=production
WORKDIR /app
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/package.json ./package.json
COPY --from=builder /app/libs/contracts/package.json ./libs/contracts/package.json
COPY --from=builder /app/libs/contracts/dist ./libs/contracts/dist
COPY --from=builder /app/services/aggregator/package.json ./services/aggregator/package.json
COPY --from=builder /app/services/aggregator/dist ./services/aggregator/dist
# Migrations are applied by the service on bootstrap; ship them for `npm run migrate` too.
COPY --from=builder /app/services/aggregator/migrations ./services/aggregator/migrations
USER node
CMD ["node", "services/aggregator/dist/main.js"]
