# CEX Ledger Service

Independent Go microservice implementing the financial boundary described in
`docs/learning/cex-deposit-service-architecture.md`.

## Capabilities

- Atomic, idempotent deposit posting to `available` or `hold`
- Held-deposit release
- Synchronous order reservation over REST and gRPC
- Kafka consumption of `PostDeposit`, `TradeExecuted`, and `OrderCancelled`
- Partial-fill reservation consumption and cancellation release
- Immutable balanced journals plus mutable balance projections
- Matching-engine sequence-gap detection
- Transactional event outbox and at-least-once inbox deduplication
- Health, readiness, Prometheus metrics, and balance/reservation queries

## Run

```bash
docker compose up --build
```

HTTP listens on `:8081`, gRPC on `:9091`, and commands are consumed from
`ledger-commands`. The service writes events to `outbox_events`; run the
independent [`../outboxrelay`](../outboxrelay) service to batch-publish them to
Kafka topic `ledger-events`.

Deterministically invalid command envelopes are atomically recorded in the
inbox and written to the transactional outbox for
`LEDGER_COMMANDS_DLQ_TOPIC` (default `ledger.commands.dlq.v1`) before the source
offset is committed. Database, dependency, cancellation, and sequence-gap
errors remain transient and stop consumption without committing the offset.

## Controlled DLQ operations

`go run ./cmd/ledger-dlq` provides bounded, audited inspection and replay. It
requires `PRAXIS_OPERATOR_ID` to match an entry in
`PRAXIS_DLQ_ALLOWED_OPERATORS`, plus the normal Ledger database and Kafka
configuration. Production must inject these variables through an authenticated
operator job and restrict that job's database and Kafka IAM permissions.

Inspect one record without displaying its key, headers, or payload:

```bash
go run ./cmd/ledger-dlq --action inspect --partition 0 --offset 42 \
  --job-id INC-123-inspect --reason 'INC-123 payload validation investigation'
```

After the defect is resolved, replay a bounded range at no more than two
records per second:

```bash
go run ./cmd/ledger-dlq --action replay --partition 0 --offset 42 \
  --max-records 10 --failure-code invalid_event_payload --interval 500ms \
  --job-id CHG-456-replay --reason 'CHG-456 parser fix deployed' --approve-replay
```

The CLI verifies migrations but never applies them. Run the normal migration
job first so `004_dlq_replay_audit.sql` exists. Every selected record and final
outcome is retained in `dlq_operation_audit`; replay never deletes DLQ data.

The PostgreSQL writer pool is capped at 32 connections and the reader pool at 16
connections by default. Override them independently with
`LEDGER_DB_WRITER_MAX_CONNS` and `LEDGER_DB_READER_MAX_CONNS`; `/metrics` reports
writer acquisition pressure and writer/reader pool usage.

On EKS, set `LEDGER_DB_WRITER_HOST` to the RDS writer endpoint and
`LEDGER_DB_READER_HOST` to the RDS reader endpoint, along with
`LEDGER_DB_USER`, `LEDGER_DB_NAME`, and `LEDGER_DB_SECRET_ARN`. Ledger uses its Pod Identity role to read the JSON
`password` field directly from Secrets Manager at startup, then builds a
PostgreSQL URL on port 5432 with `sslmode=require`. A separate migration Pod
Identity role can read the RDS admin secret. `LEDGER_DATABASE_WRITER_URL` and `LEDGER_DATABASE_READER_URL` remain available for
local Compose and take precedence when set. When no reader URL or host is set,
the reader pool falls back to the writer URL;
`LEDGER_DB_PASSWORD` is a local fallback when no secret ARN is configured.
`GET /v1/balances/{user}/{asset}` and `GET /v1/reservations/{order}` use the
read-only reader pool and can observe replica lag; mutations, Kafka processing,
migrations, and consistency-sensitive transactional reads use the writer pool.
Restart Ledger after rotating the runtime secret so it reads the new value.

## API consistency contract

Mutation responses are produced from writer transactions and contain the
committed result. Callers should use that returned state instead of immediately
reading it back through a replica. Reads performed within mutation transactions
also use the writer.

`GET /v1/balances/{user}/{asset}` and `GET /v1/reservations/{order}` are
eventually consistent because they use the reader pool. A replica can
temporarily return an older projection. Balance versions are monotonic per
user-asset account, not globally: after observing version `123`, a caller must
not replace its cached value with version `122`. Equal versions are expected to
represent the same balance state.

Financial decisions and conditional mutations always use writer state. A
timeout or lost response is an ambiguous outcome, not proof of rollback. Retry
the exact same operation with the same command ID, and never reuse that ID for a
different payload. The current handlers do not all persist and compare a full
request fingerprint; production hardening must add that conflict check. Outbox
publication is at least once, so consumers must deduplicate events by event ID.

pgx uses its prepared-statement cache explicitly in `cache_statement` mode with
128 entries per connection. Override these settings with
`LEDGER_DB_QUERY_EXEC_MODE` and `LEDGER_DB_STATEMENT_CACHE_CAPACITY`. Direct
PostgreSQL connections should retain `cache_statement`. When using an external
transaction-mode pooler, validate its prepared-statement support or select a
compatible mode such as `cache_describe` through a measured deployment change.

Operational endpoints:

```text
GET /healthz
GET /readyz
GET /metrics
```

The image also supports `/ledger-service healthcheck` for ECS container health
checks. It requests the local `/readyz` endpoint, which verifies PostgreSQL
connectivity, and exits nonzero when Ledger is not ready.

Run `/ledger-service migrate` as a one-off task to apply the Ledger schema and
exit without starting HTTP, gRPC, or Kafka consumers. Normal service startup
runs migrations by default for local Compose. Set
`LEDGER_MIGRATE_ON_STARTUP=false` after running the one-off task: Ledger then
verifies that every migration embedded in its image is recorded with the
expected checksum and fails startup if any is missing or changed. ECS sets this
to `false`. The AWS runtime uses a restricted database role and a separate
secret from the one-off migration task.

## Lifecycle source capture

`LEDGER_LIFECYCLE_CAPTURE=false` preserves the default financial commit path.
After applying migration 005, enabling it captures validated Protobuf journal
facts in `ledger_journal_outbox` atomically with financial writes. Invalid facts
abort the transaction. Replays retain the original fact; source rows are
append-only. Migration 006 adds separate delivery bookkeeping and enqueues both
existing and newly captured facts. A bounded dispatcher and synchronous Kafka
adapter for `ledger.journals.v1` are enabled with
`LEDGER_JOURNAL_DISPATCH=true` (default false). This requires
`KAFKA_AUTH_MODE=msk_iam`, `AWS_REGION`, and the configured
`LEDGER_KAFKA_BROKERS`; provision the topic and IAM permissions first.
Consumers must deduplicate journals and check account versions;
this journal-keyed stream does not guarantee account ordering.

Dispatch adds one database connection beyond the financial pool budget. One
worker has a five-second transaction deadline and a three-second send deadline,
with jittered idle/error pacing. Shutdown joins it before closing resources.
`ledger_journal_dispatch_*` metrics report attempts, acknowledgements, failures,
empty polls and active calls. Publication failure does not block financial
readiness. Capture and dispatch can be enabled independently.

Enrollment requires a reviewed account baseline and consistent writer settings.
Capture does not backfill history, and disabling it introduces projection gaps.
See [ADR 0009](../docs/adr/0009-ledger-journal-source-capture.md) for the contract
and remaining deployment gates.

Run database integration tests against a disposable migrated database:

```bash
LEDGER_TEST_DATABASE_URL='postgres://...' go test -race ./internal/store
```

## Reserve funds

Alice must first have available funds, normally from a posted deposit:

```bash
curl -X POST localhost:8081/v1/deposits:post \
  -H 'content-type: application/json' \
  -d '{"command_id":"deposit-command-1","deposit_id":"deposit-1","user_id":"alice","asset_id":"asset_usdt","custody_position_id":"custody_position_alice_eth_usdt","amount_atomic":"1000000000","target_bucket":"available","source_system":"deposit-service"}'
```

Reserve 500 USDT:

```bash
curl -X POST localhost:8081/v1/orders:reserve \
  -H 'content-type: application/json' \
  -d '{"command_id":"reserve-command-1","order_id":"order-1","user_id":"alice","asset_id":"asset_usdt","amount_atomic":"500000000"}'
```

Query the balance:

```bash
curl localhost:8081/v1/balances/alice/asset_usdt
```

The seeded custody position is a development fixture. Replace it with
controlled asset, network, and custody provisioning in production.

## Regenerate protobuf code

```bash
make protobuf-generate
```

Ledger and Matching protobuf contracts are owned by the repository's shared
`praxis/api` module. Both servers and clients import its generated packages.
