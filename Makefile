.PHONY: up down down-v logs ps stack-up stack-down stack-build

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

stack-down:
	docker compose -f infra/docker-compose.yml --profile apps down
