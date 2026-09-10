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

Run all Go tests and static checks:

```bash
make test
make vet
```

The order-admission load-test instructions are in
[`orderservice/README.md`](orderservice/README.md).

