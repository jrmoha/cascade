.PHONY: up down down-v logs ps stack-up stack-down stack-build stack-scale

# Infra only (backing stores) — the workflow tests/smoke rely on.
up:
	docker compose -f infra/docker-compose.yml up -d

down:
	docker compose -f infra/docker-compose.yml down

down-v:
	docker compose -f infra/docker-compose.yml down -v

logs:
	docker compose -f infra/docker-compose.yml logs -f

ps:
	docker compose -f infra/docker-compose.yml ps

# Full stack: infra + the three app services (collector, ingestion-processor,
# query-api) via the `apps` profile.
stack-build:
	docker compose -f infra/docker-compose.yml --profile apps build

stack-up:
	docker compose -f infra/docker-compose.yml --profile apps up -d --build

# Scale the two consumer groups horizontally (KAN-40): 3 Ingestion-Processor and
# 2 Aggregator replicas share their consumer group; `raw-events` partitions
# distribute across them. Override counts inline, e.g.
#   make stack-scale IP=6 AGG=3
IP ?= 3
AGG ?= 2
stack-scale:
	docker compose -f infra/docker-compose.yml --profile apps up -d --build \
		--scale ingestion-processor=$(IP) --scale aggregator=$(AGG)

stack-down:
	docker compose -f infra/docker-compose.yml --profile apps down
