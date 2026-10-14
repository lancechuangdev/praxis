USER_COUNT ?= 10000
AVAILABLE_ATOMIC ?= 1000000000
LEDGER_DB_WRITER_MAX_CONNS ?= 32
LEDGER_DB_READER_MAX_CONNS ?= 16
PROFILE_RUNS ?= 3
PROFILE_WARMUP_DURATION ?= 0s
PROFILE_DURATION ?= 60s
PROFILE_COOLDOWN_SECONDS ?= 60

.PHONY: test vet protobuf-generate protobuf-lint compose-up compose-down observability-up observability-down reset-load-data seed-distributed-users monitor profile-phase0 phase1-verify phase1-pod-drill phase5-verify partition-migration-check terraform-fmt

test:
	go test ./api/... ./ledgerservice/... ./matchingengine/... ./orderservice/... ./outboxrelay/... ./riskservice/...

vet:
	go vet ./api/... ./ledgerservice/... ./matchingengine/... ./orderservice/... ./outboxrelay/... ./riskservice/...

protobuf-lint:
	buf lint api

protobuf-generate:
	cd api && buf generate

compose-up:
	docker compose -f ledgerservice/compose.yaml up --build

compose-down:
	docker compose -f ledgerservice/compose.yaml down

observability-up:
	OTEL_TRACES_ENABLED=true docker compose -f ledgerservice/compose.yaml --profile observability up -d --build

observability-down:
	docker compose -f ledgerservice/compose.yaml --profile observability stop grafana prometheus cadvisor node-exporter otel-collector tempo

reset-load-data:
	docker compose -f ledgerservice/compose.yaml stop order-service ledger-service matching-engine
	docker compose -f ledgerservice/compose.yaml exec -T postgres psql -U ledger -d cex_ledger < orderservice/loadtest/reset-load-test.sql
	LEDGER_DB_WRITER_MAX_CONNS=$(LEDGER_DB_WRITER_MAX_CONNS) LEDGER_DB_READER_MAX_CONNS=$(LEDGER_DB_READER_MAX_CONNS) docker compose -f ledgerservice/compose.yaml up -d --build --force-recreate ledger-service matching-engine order-service

seed-distributed-users:
	docker compose -f ledgerservice/compose.yaml exec -T postgres psql -v ON_ERROR_STOP=1 -U ledger -d cex_ledger -v user_count=$(USER_COUNT) -v available_atomic=$(AVAILABLE_ATOMIC) < orderservice/loadtest/seed-distributed-users.sql

monitor:
	./scripts/monitor-load-test.sh

profile-phase0:
	LEDGER_DB_WRITER_MAX_CONNS=48 RUNS=$(PROFILE_RUNS) WARMUP_DURATION=$(PROFILE_WARMUP_DURATION) DURATION=$(PROFILE_DURATION) COOLDOWN_SECONDS=$(PROFILE_COOLDOWN_SECONDS) ./scripts/profile-order-admission.sh

phase1-verify:
	./scripts/verify-phase1.sh

phase1-pod-drill:
	ACK_PHASE1_FAILURE_DRILL=delete-one-pod ./scripts/phase1-pod-failure-drill.sh

phase5-verify:
	./scripts/verify-phase5.sh

partition-migration-check:
	cd outboxrelay && go run ./cmd/partition-migration-check --hot-percent 60
	cd outboxrelay && go run ./cmd/partition-migration-check --hot-percent 80
	cd outboxrelay && go run ./cmd/partition-migration-check --hot-percent 95

terraform-fmt:
	terraform -chdir=infra/aws fmt -recursive
