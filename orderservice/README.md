# Mock CEX order service

This independent service models the synchronous order-admission path for load
testing:

```text
HTTP client → mock risk check → ledger ReserveForOrder gRPC → matching engine gRPC
```

It is a benchmark fixture, not a production order service. It intentionally
does not persist an order state machine or implement compensation when matching
admission fails after a successful reservation.

## Modes

- `ORDER_LEDGER_MODE=mock` uses a configurable simulated reservation.
- `ORDER_LEDGER_MODE=grpc` calls the real ledger service and includes its durable PostgreSQL reservation transaction.
- `ORDER_MATCHING_MODE=mock` uses a configurable simulated matching engine.
- `ORDER_MATCHING_MODE=grpc` calls the independent mock matching engine; that engine publishes the resulting event to Kafka.

Every successful response contains stage timings and a `Server-Timing` header:

```json
{
  "order_id": "order-1",
  "status": "accepted",
  "reservation": {"reservation_id": "rsv-1", "balance_version": 2},
  "engine_sequence": 42,
  "timings": {"risk_ms": 1.1, "reserve_ms": 4.2, "matching_ms": 3.0, "total_ms": 8.4}
}
```

## Run

Baseline without external dependencies:

```bash
ORDER_LEDGER_MODE=mock \
ORDER_MATCHING_MODE=mock \
go run ./cmd/order-service
```

Real reservation and synchronous matching-admission path:

```bash
ORDER_LEDGER_MODE=grpc \
ORDER_LEDGER_GRPC_ADDRESS=localhost:9091 \
ORDER_MATCHING_MODE=grpc \
ORDER_MATCHING_GRPC_ADDRESS=localhost:9094 \
go run ./cmd/order-service
```

Alternatively, `../ledgerservice/compose.yaml` starts PostgreSQL, Kafka, the
ledger service, mock matching engine, and this order service together:

```bash
cd ../ledgerservice
docker compose up --build
```

Before a real-ledger test, give the default hot-account user sufficient funds:

```bash
curl -X POST http://localhost:8081/v1/deposits:post \
  -H 'content-type: application/json' \
  -d '{
    "command_id":"load-seed-command",
    "deposit_id":"load-seed-deposit",
    "user_id":"alice",
    "asset_id":"asset_usdt",
    "custody_position_id":"custody_position_alice_eth_usdt",
    "amount_atomic":"1000000000000",
    "target_bucket":"available",
    "source_system":"load-test"
  }'
```

The Order Service does not publish Kafka messages. The matching engine publishes
`OrderAccepted` after admitting the order and returns its engine sequence over
gRPC. Its synchronous Kafka writer combines concurrent publications subject to
configured record, byte, and timeout limits.

## Load test

The included k6 scenario uses a constant arrival rate. Start with a baseline,
then increase toward the measured accepted-order target:

```bash
docker run --rm --network=host \
  -e RATE=4050 \
  -e DURATION=60s \
  -e BASE_URL=http://localhost:8083 \
  -v "$PWD/loadtest:/scripts:ro" \
  grafana/k6 run /scripts/order-admission.js
```

For `grpc` mode, Alice needs enough available USDT for every unique order. The
test defaults to reserving one atomic unit per request. This single-user test is
deliberately a hot-account contention test. Use separately funded users to
measure a distributed workload.

Useful endpoints:

- `POST /v1/orders`
- `GET /healthz`
- `GET /readyz`
- `GET /metrics`

Key environment variables:

| Variable | Default |
|---|---:|
| `ORDER_HTTP_ADDRESS` | `:8083` |
| `ORDER_RISK_LATENCY` | `1ms` |
| `ORDER_RISK_REJECT_BPS` | `0` |
| `ORDER_LEDGER_MODE` | `grpc` |
| `ORDER_LEDGER_GRPC_ADDRESS` | `localhost:9091` |
| `ORDER_LEDGER_TIMEOUT` | `2s` |
| `ORDER_MOCK_LEDGER_LATENCY` | `3ms` |
| `ORDER_MATCHING_MODE` | `grpc` |
| `ORDER_MATCHING_GRPC_ADDRESS` | `localhost:9092` |
| `ORDER_MATCHING_TIMEOUT` | `2s` |
| `ORDER_MOCK_MATCHING_LATENCY` | `1ms` |
