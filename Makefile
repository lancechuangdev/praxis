USER_COUNT ?= 10000
AVAILABLE_ATOMIC ?= 1000000000
LEDGER_DB_MAX_CONNS ?= 32
PROFILE_RUNS ?= 3
PROFILE_WARMUP_DURATION ?= 2m
PROFILE_DURATION ?= 10m
PROFILE_COOLDOWN_SECONDS ?= 60

.PHONY: test vet compose-up compose-down observability-up observability-down reset-load-data seed-distributed-users monitor profile-phase0 terraform-fmt

test:
	go test ./ledgerservice/... ./matchingengine/... ./orderservice/... ./outboxrelay/...

vet:
	go vet ./ledgerservice/... ./matchingengine/... ./orderservice/... ./outboxrelay/...

compose-up:
	docker compose -f ledgerservice/compose.yaml up --build

compose-down:
	docker compose -f ledgerservice/compose.yaml down

observability-up:
	docker compose -f ledgerservice/compose.yaml --profile observability up -d --build

observability-down:
	docker compose -f ledgerservice/compose.yaml --profile observability stop grafana prometheus cadvisor node-exporter

reset-load-data:
	docker compose -f ledgerservice/compose.yaml stop order-service ledger-service matching-engine
	docker compose -f ledgerservice/compose.yaml exec -T postgres psql -U ledger -d cex_ledger < orderservice/loadtest/reset-load-test.sql
	LEDGER_DB_MAX_CONNS=$(LEDGER_DB_MAX_CONNS) docker compose -f ledgerservice/compose.yaml up -d --build --force-recreate ledger-service matching-engine order-service

seed-distributed-users:
	docker compose -f ledgerservice/compose.yaml exec -T postgres psql -v ON_ERROR_STOP=1 -U ledger -d cex_ledger -v user_count=$(USER_COUNT) -v available_atomic=$(AVAILABLE_ATOMIC) < orderservice/loadtest/seed-distributed-users.sql

monitor:
	./scripts/monitor-load-test.sh

profile-phase0:
	LEDGER_DB_MAX_CONNS=48 RUNS=$(PROFILE_RUNS) WARMUP_DURATION=$(PROFILE_WARMUP_DURATION) DURATION=$(PROFILE_DURATION) COOLDOWN_SECONDS=$(PROFILE_COOLDOWN_SECONDS) ./scripts/profile-order-admission.sh

terraform-fmt:
	terraform -chdir=infra/aws fmt -recursive
