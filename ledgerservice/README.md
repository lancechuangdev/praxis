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

The PostgreSQL pool is explicitly capped at 32 connections by default. Override
it with `LEDGER_DB_MAX_CONNS`; `/metrics` reports the configured maximum and
connection-acquisition pressure.

On EKS, set `LEDGER_DB_HOST`, `LEDGER_DB_USER`, `LEDGER_DB_NAME`, and
`LEDGER_DB_SECRET_ARN`. Ledger uses its Pod Identity role to read the JSON
`password` field directly from Secrets Manager at startup, then builds a
PostgreSQL URL on port 5432 with `sslmode=require`. A separate migration Pod
Identity role can read the RDS admin secret. `LEDGER_DATABASE_URL` remains
available for local Compose and takes precedence when set;
`LEDGER_DB_PASSWORD` is a local fallback when no secret ARN is configured.
Restart Ledger after rotating the runtime secret so it reads the new value.

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
protoc --go_out=. --go_opt=module=praxis/ledgerservice \
  --go-grpc_out=. --go-grpc_opt=module=praxis/ledgerservice \
  api/ledger/v1/ledger.proto
```
