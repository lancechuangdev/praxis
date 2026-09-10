# CEX outbox relay

Independent relay for the ledger service's PostgreSQL `outbox_events` table. It claims pending rows with short leases, publishes them to Kafka, and records publication outcomes. Delivery is at least once, so consumers must remain idempotent by event ID.

## Batching

Each claim returns up to `OUTBOX_CLAIM_SIZE` ordered events. The relay submits the entire claim in one `WriteMessages(ctx, messages...)` call. `kafka-go` then groups those messages into broker produce requests subject to `OUTBOX_KAFKA_BATCH_SIZE`, `OUTBOX_KAFKA_BATCH_BYTES`, and `OUTBOX_KAFKA_BATCH_TIMEOUT`. The writer is synchronous (`Async: false`) and uses `RequireAll`; successful rows are marked published only after Kafka acknowledges them.

Defaults:

| Variable | Default |
|---|---:|
| `OUTBOX_KAFKA_BROKERS` | `localhost:9092` |
| `OUTBOX_INSTANCE_ID` | hostname |
| `OUTBOX_HTTP_ADDRESS` | `:8082` |
| `OUTBOX_CLAIM_SIZE` | `500` |
| `OUTBOX_LEASE_DURATION` | `30s` |
| `OUTBOX_POLL_INTERVAL` | `100ms` |
| `OUTBOX_KAFKA_BATCH_SIZE` | `500` |
| `OUTBOX_KAFKA_BATCH_BYTES` | `524288` |
| `OUTBOX_KAFKA_BATCH_TIMEOUT` | `5ms` |

`OUTBOX_DATABASE_URL` is required and should point to the ledger database. The
relay migration creates `outbox_events` when it is absent and upgrades an older
ledger-created table with the lease columns when it is already present.

Run locally:

```bash
OUTBOX_DATABASE_URL='postgres://ledger:ledger@localhost:5433/cex_ledger?sslmode=disable' \
OUTBOX_KAFKA_BROKERS='localhost:9092' \
go run ./cmd/outbox-relay
```

Endpoints are `GET /healthz`, `GET /readyz`, and Prometheus-text `GET /metrics`.
