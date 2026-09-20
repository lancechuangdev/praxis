# CEX outbox relay

Independent relay for the ledger service's PostgreSQL `outbox_events` table. It claims pending rows with short leases, publishes them to Kafka, and records publication outcomes. Delivery is at least once, so consumers must remain idempotent by event ID.

## Batching

Each claim returns up to `OUTBOX_CLAIM_SIZE` ordered events. The relay submits the entire claim in one `WriteMessages(ctx, messages...)` call. `kafka-go` then groups those messages into broker produce requests subject to `OUTBOX_KAFKA_BATCH_SIZE`, `OUTBOX_KAFKA_BATCH_BYTES`, and `OUTBOX_KAFKA_BATCH_TIMEOUT`. The writer is synchronous (`Async: false`) and uses `RequireAll`; successful rows are marked published only after Kafka acknowledges them.

Publication acknowledgements are also set-based. A fully successful claim is
marked published with one PostgreSQL update instead of one update per event.
When Kafka returns per-message results, successful and failed IDs are written
in at most two updates; `unnest` preserves the distinct error message for each
failed event. Every update verifies that all expected leases are still owned.

Defaults:

| Variable | Default |
|---|---:|
| `OUTBOX_KAFKA_BROKERS` | `localhost:9092` |
| `OUTBOX_KAFKA_TOPIC_MAP` | empty (publish each stored topic unchanged) |
| `OUTBOX_INSTANCE_ID` | hostname |
| `OUTBOX_HTTP_ADDRESS` | `:8082` |
| `OUTBOX_CLAIM_SIZE` | `500` |
| `OUTBOX_LEASE_DURATION` | `30s` |
| `OUTBOX_POLL_INTERVAL` | `100ms` |
| `OUTBOX_KAFKA_BATCH_SIZE` | `500` |
| `OUTBOX_KAFKA_BATCH_BYTES` | `524288` |
| `OUTBOX_KAFKA_BATCH_TIMEOUT` | `5ms` |

`OUTBOX_DATABASE_URL` should point to the ledger database for local Compose.
For ECS, instead set `OUTBOX_DB_HOST`, `OUTBOX_DB_USER`, `OUTBOX_DB_NAME`, and
inject `OUTBOX_DB_PASSWORD` from Secrets Manager. The resulting PostgreSQL URL
uses port 5432 and `sslmode=require`. `OUTBOX_DATABASE_URL` takes precedence
when set. This configuration alone does not deploy the relay or grant database
access. The relay migration creates `outbox_events` when absent and upgrades an
older ledger-created table with lease columns when already present.

For the AWS MSK deployment, the Ledger outbox currently stores `ledger-events`
while the provisioned versioned topic is `ledger.events.v1`. Set
`OUTBOX_KAFKA_TOPIC_MAP=ledger-events=ledger.events.v1` on the Ledger relay.
The mapping is applied only to outgoing Kafka messages; committed rows keep
their original topic. The setting accepts comma-separated `source=destination`
pairs and rejects invalid or duplicate source names.

Run locally:

```bash
OUTBOX_DATABASE_URL='postgres://ledger:ledger@localhost:5433/cex_ledger?sslmode=disable' \
OUTBOX_KAFKA_BROKERS='localhost:9092' \
go run ./cmd/outbox-relay
```

Endpoints are `GET /healthz`, `GET /readyz`, and Prometheus-text `GET /metrics`.
