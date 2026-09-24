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

## Consistency contract

A successful `POST /v1/orders` response contains the Ledger reservation that
was committed on the PostgreSQL writer. Clients should update their local state
from this response instead of immediately issuing a GET for the same state. The
current mock Matching Engine returns only after Kafka acknowledges its
`OrderAccepted` event; it does not yet persist an order state machine.

Ledger balance and reservation GET endpoints are served from read replicas and
are eventually consistent. They can temporarily return an older version than a
mutation response. A client that has observed balance version `123` must not
overwrite that cached balance with replica version `122`. Versions are monotonic
per user-asset balance, not globally. Financial decisions and conditional
mutations are evaluated by Ledger against writer state, never client-cached or
replica state.

A timeout or lost response does not prove that a mutation failed. Retry the
exact same request with the same request/command ID. Callers must never reuse
that ID for a different payload. This benchmark fixture does not consistently
fingerprint idempotent requests and does not yet expose a complete production
pending-outcome or reconciliation API.

## Request context

Clients may send `X-Request-ID` and `X-Correlation-ID`. The service validates
and echoes both headers, generating safe values when either is absent or
invalid. The request ID identifies this HTTP attempt; the correlation ID stays
with the wider order workflow. Both IDs are propagated to Ledger and Matching
and appear in their Kafka event envelopes and headers.

The service also accepts valid W3C `traceparent` and `tracestate` headers and
propagates them through gRPC and Kafka. With `OTEL_TRACES_ENABLED=true`, the
services create HTTP, gRPC, risk-check, and Kafka-producer spans and export
them over OTLP. `make observability-up` enables this automatically and sends
the spans through the local OpenTelemetry Collector to Tempo.

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

On 2026-09-11, the same rate sequence was repeated with a 32-connection Ledger
pool under **Experiment C** conditions: Prometheus/Grafana off, raw monitoring
off, a clean reset and 10,000-user reseed before every run, and a 60-second gap
between runs.

| Target | Achieved | Completed | Failed | Dropped | HTTP avg / p95 / p99 | Reserve avg / p95 | Thresholds |
|---:|---:|---:|---:|---:|---:|---:|---|
| 1,000/s | 999.85/s | 60,001 | 0 | 0 | 8.89 / 11.03 / 13.82 ms | 3.02 / 4.54 ms | Pass |
| 2,000/s | 1,999.76/s | 120,001 | 0 | 0 | 8.86 / 11.39 / 15.03 ms | 3.25 / 5.19 ms | Pass |
| 4,000/s | 3,999.19/s | 239,986 | 0 | 15 | 7.61 / 9.67 / 14.32 ms | 2.47 / 3.88 ms | Pass |
| 6,000/s | 5,995.42/s | 359,777 | 0 | 225 | 7.11 / 8.53 / 11.50 ms | 2.18 / 2.86 ms | Pass |
| 8,000/s | 7,990.94/s | 479,517 | 0 | 484 | 7.11 / 9.49 / 13.68 ms | 2.22 / 3.62 ms | Pass |
| 10,000/s | 9,961.97/s | 598,472 | 0 | 1,529 | 14.65 / 47.90 / 68.63 ms | 9.49 / 42.23 ms | Pass |

Pool 32 remains within the configured latency thresholds through 10,000/s, but
the sharp reservation-tail increase at 10,000/s marks the saturation knee.
Dropped iterations begin at 4,000/s and reach 1,529 at 10,000/s, so a threshold
pass does not mean k6 started the entire requested workload.

The 10,000/s Experiment C test was then repeated three times with pool 48. Each
run used a clean reset/reseed and a 60-second cooldown; all started requests
were accepted.

| Run | Achieved | Completed | Failed | Dropped | HTTP avg / p95 / p99 | Reserve avg / p95 | Thresholds |
|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 9,991.21/s | 599,564 | 0 | 437 | 8.61 / 14.84 / 24.01 ms | 3.45 / 8.63 ms | Pass |
| 2 | 9,977.01/s | 598,896 | 0 | 1,105 | 11.43 / 39.96 / 58.25 ms | 6.24 / 33.79 ms | Pass |
| 3 | 9,973.76/s | 598,842 | 0 | 1,164 | 12.76 / 42.31 / 62.01 ms | 7.51 / 36.25 ms | Pass |
| Median | 9,977.01/s | 598,896 | 0 | 1,105 | 11.43 / 39.96 / 58.25 ms | 6.24 / 33.79 ms | Pass |

Using medians, pool 48 reduced HTTP p95 from 47.90 ms to 39.96 ms,
reservation p95 from 42.23 ms to 33.79 ms, and dropped iterations from 1,529
to 1,105 versus pool 32 at 10,000/s. Run 1 was unusually favorable; runs 2 and
3 were close, so the median is a better planning value. Performance degraded
across consecutive runs even with logical resets because Kafka logs,
PostgreSQL WAL/checkpoint state, caches, and host conditions persist. Experiment
C intentionally records only k6 measurements, so it cannot isolate those
server-side effects.

In the 2026-09-10 run, the first clear saturation signal appears at 8,000/s:
dropped iterations begin and ledger-reservation tail latency rises. At
10,000/s every started request is accepted, but the generator drops 1,529
scheduled iterations and HTTP p95 exceeds the 50 ms objective. These are
single-host development benchmarks, not production capacity claims. The pasted
2026-09-10 2,000/s excerpt did not include HTTP p99, so that value is left blank
rather than inferred.

#### Connection-pool experiment

Reset and restart the stack with an explicit Ledger pool size, then reseed the
same users before every run:

```bash
cd /home/boris-alienware/projects/praxis
make reset-load-data LEDGER_DB_WRITER_MAX_CONNS=16
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

#### Reproducible pool-48 profile

Run the Phase 0 profiling workflow from the repository root:

```bash
make profile-phase0
```

It performs three independent 60-second runs without a warm-up by default.
Every run resets and seeds the database, fixes the Ledger pool at 48, enables
strict k6 thresholds (including zero dropped iterations), captures
`pg_stat_statements` plus the existing monitor data, and runs
accounting-integrity checks. Results and a generated `baseline-report.md` are
written beneath `orderservice/loadtest/results/`. See the
[optimization plan](../docs/order-admission-optimization-plan.md#phase-0-establish-a-trustworthy-profile)
for acceptance criteria, shorter smoke-test overrides, and the retained
two-minute warm-up plus ten-minute endurance profile.

Useful endpoints:

- `POST /v1/orders`
- `GET /healthz`
- `GET /readyz`
- `GET /metrics`

The distroless image also supports `/order-service healthcheck` for ECS. It
requests the local `/readyz` endpoint and exits nonzero if Ledger or Matching
is unavailable.

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

### Data collected

k6 prints the client-side result at the end of the run. These metrics describe
what callers experienced:

| Metric | Description and use |
|---|---|
| `http_reqs` | Requests completed and their rate; confirms achieved throughput. |
| `http_req_duration` | End-to-end HTTP latency, including p95 and p99; checks the user-facing latency objective. |
| `http_req_failed` | Transport errors and unexpected HTTP responses; detects unavailable or rejected requests. |
| `checks_total`, `checks_succeeded`, `checks_failed` | Results of the `202 Accepted` check; distinguishes accepted orders from failures. |
| `order_failure_rate` | Custom fraction of orders not accepted; enforces the `<1%` threshold. |
| `order_risk_ms` | Risk-stage latency returned by Order Service; isolates risk processing from later stages. |
| `order_reserve_ms` | Ledger reservation latency returned by Order Service; identifies pressure in the synchronous balance path. |
| `order_matching_ms` | Matching submission latency returned by Order Service; shows whether Matching Engine is contributing to tail latency. |
| `order_total_ms` | Server-reported total admission latency; compares server work with HTTP duration and exposes network/client overhead. |
| `iterations`, `iteration_duration` | Completed scenario iterations and their latency; measures complete k6 work rather than HTTP alone. |
| `dropped_iterations` | Arrivals k6 could not start at the requested rate; an important saturation signal even when started requests succeed. |
| `vus`, `vus_max` | Active and permitted virtual users; shows how much concurrency k6 needed and whether `MAX_VUS` constrained the generator. |
| `data_sent`, `data_received` | Network volume and rate; useful for ruling network bandwidth in or out. |

The monitor samples the following server and PostgreSQL data every
`INTERVAL_SECONDS` seconds:

| File and fields | Description and use |
|---|---|
| `ledger.prom`: `ledger_reserve_requests_total`, `ledger_reserve_failures_total`, `ledger_reserve_duration_seconds_sum` | Reservation count, failures, and cumulative duration; derives Ledger throughput, failure rate, and average reservation time. |
| `ledger.prom`: `ledger_db_pool_acquired_connections`, `ledger_db_pool_idle_connections`, `ledger_db_pool_total_connections`, `ledger_db_pool_max_connections` | Instantaneous pgx pool usage and configured limit; shows whether the pool is full, underused, or oversized. |
| `ledger.prom`: `ledger_db_pool_acquire_total`, `ledger_db_pool_empty_acquire_total`, `ledger_db_pool_canceled_acquire_total`, `ledger_db_pool_acquire_duration_seconds_total` | Acquisition attempts, attempts made while no connection was immediately available, cancellations, and cumulative wait time; identifies connection-pool queueing. |
| `order.prom`: `order_requests_total`, `order_accepted_total`, `order_risk_rejected_total`, `order_failed_total` | Order outcomes; locates rejection or failure at the workflow boundary. |
| `order.prom`: `order_risk_duration_seconds_sum`, `order_reserve_duration_seconds_sum`, `order_matching_duration_seconds_sum`, `order_total_duration_seconds_sum` | Cumulative stage durations; compares where admission time is spent. |
| `matching.prom`: `matching_orders_submitted_total`, `matching_orders_accepted_total`, `matching_orders_failed_total` | Matching request outcomes; confirms whether admitted orders reach and are accepted by Matching Engine. |
| `matching.prom`: `matching_engine_duration_seconds_sum`, `matching_kafka_duration_seconds_sum` | Cumulative engine and Kafka publication time; separates matching work from event-publication latency. |
| `pg_stat_activity.tsv`: `wait_type`, `wait_event`, `state`, `connections` | Connection states and PostgreSQL wait categories; distinguishes CPU work, locks, I/O, client waits, and idle sessions. |
| `pg_stat_wal.tsv`: `wal_records`, `wal_fpi`, `wal_bytes`, `wal_buffers_full`, `stats_reset` | WAL records, full-page images, bytes, buffer exhaustion, and counter epoch; detects WAL generation or WAL-buffer pressure. |
| `pg_stat_database.tsv`: `xact_commit`, `xact_rollback` | Committed and rolled-back transactions; measures database transaction throughput and rollback rate. |
| `pg_stat_database.tsv`: `tup_inserted`, `tup_updated` | Rows inserted and updated; quantifies write amplification per admitted order. |
| `pg_stat_database.tsv`: `blk_read_time_ms`, `blk_write_time_ms` | Time PostgreSQL reports waiting for block reads and writes; helps identify storage pressure when I/O timing is enabled. |
| `pg_stat_database.tsv`: `temp_files`, `temp_bytes`, `deadlocks` | Temporary-file use and deadlocks; reveals memory-spilling queries and concurrency conflicts. |
| `relation_sizes.tsv`: `relation`, `total_bytes`, `table_bytes`, `indexes_bytes` | Table and index sizes over time; measures growth and identifies indexes or relations dominating storage. |
| `container_stats.tsv`: container, CPU percentage, memory usage/limit/percentage, network I/O, block I/O, and PIDs | Shows which Docker component consumes CPU or memory and whether PostgreSQL, Kafka, k6, or an application is the local bottleneck. |
| `host_cpu.tsv`: cumulative CPU-mode counters | Derives host CPU utilization and I/O wait between samples; detects competition among containers for the same machine. |
| `host_memory.tsv`: total, available, used, and swap memory | Detects whole-host memory pressure and swapping that can inflate latency. |
| `host_load.tsv`: 1/5/15-minute load and runnable/total tasks | Shows scheduler pressure and whether runnable work exceeds available CPU capacity. |

Most service and PostgreSQL statistics are cumulative counters. Compare the
first sample immediately before the run with the last sample immediately after
it; do not interpret the final value by itself. For a cumulative duration, use
the change in the duration sum divided by the change in its corresponding
request count. Gauge values such as acquired connections are interpreted per
sample, commonly by their peak during the load interval.

### Live Prometheus and Grafana dashboard

Start the application and observability services from the repository root:

```bash
make observability-up
```

Open:

- Grafana: <http://localhost:3000> (`admin` / `admin`, local development only)
- Prometheus: <http://localhost:9090>
- Prometheus target health: <http://localhost:9090/targets>
- Tempo API: <http://localhost:3200>

Grafana automatically provisions the **CEX / CEX Load Test** dashboard. It
shows workflow throughput, average stage latency, pgx pool usage and acquisition
wait, Matching/Kafka latency, per-container CPU and working memory, and host CPU
and memory. Prometheus scrapes every two seconds and retains seven days locally.

cAdvisor reads Linux host and Docker runtime data and therefore runs privileged
with read-only host mounts in this local development stack. Do not copy that
configuration into production; use ECS Container Insights and RDS monitoring in
AWS. A k6 container started during the test appears automatically in the
container panels.

To inspect traces, open Grafana **Explore**, select the **Tempo** data source,
choose **Search**, and filter on a service such as `order-service`. A request to
the Order Service can include `X-Correlation-ID` for business-workflow lookup
and `traceparent` for trace continuation; these remain distinct identifiers.

Stop only the observability containers with:

```bash
make observability-down
```

Prometheus and Grafana data remain in named Docker volumes. `make compose-down`
also preserves them; use `docker compose -f ledgerservice/compose.yaml down -v`
only when intentionally deleting all local Compose volumes.

For comparable measurements, reset and reseed the database before each run:

```bash
make reset-load-data
```

The reset removes local financial/load-test rows but preserves the schema and
reference fixtures. See `orderservice/README.md` for scenario-specific seeding.
