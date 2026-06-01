# syntax=docker/dockerfile:1
# Multi-stage build for the Collector. Build context is the REPO ROOT
# (docker-compose sets `context: ..`) so the @cascade/contracts workspace
# dependency resolves. Run from repo root:
#   docker build -f services/collector/Dockerfile -t cascade-collector .

# ---- builder: install everything, build contracts + this service, drop dev deps ----
FROM node:22-slim AS builder
WORKDIR /app
COPY . .
RUN npm ci
RUN npm run build -w @cascade/contracts && npm run build -w @cascade/collector
RUN npm prune --omit=dev

# ---- runtime: slim image with only prod deps + built dist ----
FROM node:22-slim AS runtime
ENV NODE_ENV=production
WORKDIR /app
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/package.json ./package.json
COPY --from=builder /app/libs/contracts/package.json ./libs/contracts/package.json
COPY --from=builder /app/libs/contracts/dist ./libs/contracts/dist
COPY --from=builder /app/services/collector/package.json ./services/collector/package.json
COPY --from=builder /app/services/collector/dist ./services/collector/dist
USER node
CMD ["node", "services/collector/dist/main.js"]
