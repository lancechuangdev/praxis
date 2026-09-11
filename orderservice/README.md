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

## Load tests

Both k6 scenarios use a constant arrival rate and reserve one atomic USDT unit
per order by default. Run these commands from the `orderservice` directory.

### Reset before each run

From the repository root, clear generated ledger activity before every measured
run:

```bash
cd /home/boris-alienware/projects/praxis
make reset-load-data
```

The command temporarily stops the Order, Ledger, and Matching services,
truncates balances, reservations, journals, entries, inbox/outbox events, and
engine offsets, and then restarts the services. It preserves migrations,
assets, networks, ledger accounts, and custody fixtures. This permanently
deletes the previous local test run. After resetting, seed either Alice for the
hot-account scenario or the generated users for the distributed scenario.

### Hot account

This intentionally sends every order through one `user × asset` balance row.
It measures per-account serialization, overspending safety, and lock contention;
it does not represent exchange-wide capacity.

After resetting, seed Alice using the deposit command in the earlier
[`Reserve funds`](#reserve-funds) example before running this scenario.

```bash
docker run --rm --network=host \
  -v "$PWD/loadtest:/scripts:ro" \
  grafana/k6 run \
  -e RATE=100 \
  -e DURATION=60s \
  -e BASE_URL=http://localhost:8083 \
  -e USER_ID=alice \
  -e RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)" \
  /scripts/order-admission-hot-account.js
```

### Distributed users

First seed 10,000 benchmark users with 1,000 USDT each. This fixture writes
balance projections directly and is only for load testing, not production
ledger posting:

From the repository root:

```bash
make seed-distributed-users
```

The defaults can be overridden, for example:

```bash
make seed-distributed-users USER_COUNT=20000 AVAILABLE_ATOMIC=2000000000
```

Then distribute order admission round-robin across those users:

```bash
docker run --rm --network=host \
  -v "$PWD/loadtest:/scripts:ro" \
  grafana/k6 run \
  -e RATE=1000 \
  -e DURATION=60s \
  -e BASE_URL=http://localhost:8083 \
  -e USER_COUNT=10000 \
  -e ENGINE_PARTITIONS=96 \
  -e RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)" \
  /scripts/order-admission-distributed-users.js
```

`USER_COUNT` must equal the number passed to the seed script. `RUN_ID` makes
request and order IDs unique across runs. If omitted, k6 generates one run ID
in `setup()` and shares it with every VU. Re-seeding does not replenish existing
users, so reset the test database when you need a completely fresh balance set.

#### Observed local results

On 2026-09-10, the full local path (Ledger gRPC + PostgreSQL + Matching Engine
gRPC + synchronous Kafka publication) was tested for 60 seconds per rate with
10,000 users and 96 engine partitions.

| Target | Achieved | Completed | Failed | Dropped | HTTP p95 / p99 | Reserve p95 | Thresholds |
|---:|---:|---:|---:|---:|---:|---:|---|
| 1,000/s | 999.77/s | 60,001 | 0 | 0 | 11.05 / 14.42 ms | 4.47 ms | Pass |
| 2,000/s | 1,999.68/s | 120,001 | 0 | 0 | 11.61 / — ms | 4.56 ms | Pass |
| 4,000/s | 3,999.40/s | 240,001 | 0 | 0 | 11.07 / 17.38 ms | 4.88 ms | Pass |
| 6,000/s | 5,999.06/s | 360,004 | 0 | 0 | 10.38 / 15.20 ms | 4.12 ms | Pass |
| 8,000/s | 7,997.78/s | 479,949 | 0 | 56 | 27.09 / 40.74 ms | 20.81 ms | Pass |
| 10,000/s | 9,973.09/s | 598,472 | 0 | 1,529 | 59.05 / 71.66 ms | 53.37 ms | Fail: p95 > 50 ms |

The first clear saturation signal appears at 8,000/s: dropped iterations begin
and ledger-reservation tail latency rises. At 10,000/s every started request is
accepted, but the generator drops 1,529 scheduled iterations and HTTP p95
exceeds the 50 ms objective. This is a single-host development benchmark, not a
production capacity claim. The pasted 2,000/s excerpt did not include HTTP p99
or reservation p95, so those values are left blank rather than inferred.

#### Connection-pool experiment

Reset and restart the stack with an explicit Ledger pool size, then reseed the
same users before every run:

```bash
cd /home/boris-alienware/projects/praxis
make reset-load-data LEDGER_DB_MAX_CONNS=16
make seed-distributed-users
curl -s http://localhost:8081/metrics | grep ledger_db_pool_max_connections
```

Repeat the 10,000/s monitored test with pool sizes `16`, `32`, `48`, and `64`,
using a distinct run ID such as `pool-16-rate-10000`. Never compare runs that
started with different database contents.

The clean 60-second runs produced the following service-side results. Averages
come from the cumulative application metrics captured by the monitor; they are
not k6 percentiles.

| Pool | Accepted | Approx. rate | Avg. reserve | Avg. order total | Peak acquired | Empty acquires | Result |
|---:|---:|---:|---:|---:|---:|---:|---|
| 16 | 544,594 | 9,077/s | 388.43 ms | 393.05 ms | 16 | 544,594 | Saturated |
| 32 | 598,944 | 9,982/s | 4.03 ms | 8.78 ms | 32 | 169,058 | Near target |
| 48 | 599,051 | 9,984/s | 2.97 ms | 7.74 ms | 48 | 62,668 | Best observed |
| 64 | 599,164 | 9,986/s | 3.03 ms | 7.79 ms | 33 | 41,640 | No material gain |

Pool 16 spent almost the entire run waiting to acquire database connections.
Increasing the pool to 32 removed that severe queue and recovered almost all of
the requested throughput. Pool 48 reduced average reservation latency by a
further 26% versus pool 32. Pool 64 did not improve latency or throughput over
48 and used at most 33 connections in the monitor samples, so 48 is the best
local setting among those tested. This is a workload- and machine-specific
result, not a production PostgreSQL connection recommendation.

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

## Monitor a load test

Start the sampler in a separate terminal before k6:

```bash
RUN_ID=distributed-8000 INTERVAL_SECONDS=2 make monitor
```

Stop it with `Ctrl+C` after the test. It writes timestamped service metrics and
PostgreSQL activity, WAL, database, and relation-size samples beneath
`orderservice/loadtest/results/monitor-$RUN_ID/`. The generated results are
ignored by Git.

For comparable measurements, reset and reseed the database before each run:

```bash
make reset-load-data
```

The reset removes local financial/load-test rows but preserves the schema and
reference fixtures. See `orderservice/README.md` for scenario-specific seeding.
