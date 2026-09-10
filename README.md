# Praxis CEX architecture playground

Independent CEX backend and load-testing project containing:

- `orderservice`: HTTP order admission, risk simulation, Ledger gRPC, and Matching Engine gRPC.
- `ledgerservice`: PostgreSQL-backed double-entry ledger and reservation APIs.
- `matchingengine`: mock partitioned engine that emits matching events to Kafka.
- `outboxrelay`: batched PostgreSQL-outbox-to-Kafka relay.
- `infra/aws`: independent VPC, Amazon MSK, Kafka topics, and Multi-AZ PostgreSQL Terraform.

## Quick start

Start the complete local stack:

```bash
make compose-up
```

## Local ports

| Host port | Service | Protocol | Purpose |
|---:|---|---|---|
| `8081` | Ledger Service | HTTP | Deposit API, health, readiness, and metrics |
| `9091` | Ledger Service | gRPC | Synchronous balance reservation and ledger operations |
| `8083` | Order Service | HTTP | Order-admission API, health, readiness, and metrics |
| `8084` | Matching Engine | HTTP | Health and metrics |
| `9094` | Matching Engine | gRPC | Synchronous `SubmitOrder` from the Order Service |
| `5433` | PostgreSQL | PostgreSQL | Local database access; containers use `postgres:5432` |
| `9093` | Kafka | Kafka | Local broker access; containers use `kafka:9092` |

The three HTTP ports are therefore:

```text
8081 = Ledger Service
8083 = Order Service
8084 = Matching Engine
```

Check the running services with:

```bash
curl http://localhost:8081/healthz
curl http://localhost:8081/readyz
curl http://localhost:8083/healthz
curl http://localhost:8083/readyz
curl http://localhost:8084/healthz
```

The Matching Engine currently has no `/readyz` endpoint, so
`http://localhost:8084/readyz` returns `404`.

Compose creates `ledger-commands`, `ledger-events`, and `matching.events.v1`
before starting the Ledger Service and Matching Engine. This prevents order
admission from failing because a required Kafka topic is missing.

Run all Go tests and static checks:

```bash
make test
make vet
```

The order-admission load-test instructions are in
[`orderservice/README.md`](orderservice/README.md).
