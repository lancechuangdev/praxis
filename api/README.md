# Praxis API contracts

This module is the canonical source for Praxis protobuf contracts and their
generated Go transport types. Service modules import these packages; they do
not maintain service-local copies of shared `.proto` files or generated code.

Regenerate code from the repository root:

```bash
make protobuf-generate
```

Lint contracts before committing:

```bash
buf lint api
```

Keep domain models, persistence types, and business logic in their owning
services. This module contains wire contracts, wire-validation helpers, and
generated transport code.

## Risk v1

`risk.v1.RiskService/CheckOrder` is the versioned internal admission contract.
Its request carries the complete order and reservation inputs needed for a
deterministic decision; quantities and atomic amounts remain strings to avoid
precision loss. Its response uses an explicit decision enum and includes
stable decision and policy identifiers for comparison and audit. An
unspecified decision is not approval. Trace context is transported as standard
gRPC metadata rather than protobuf fields.

## Projection lifecycle v1

`events.lifecycle.v1` defines projection-only Order workflow snapshots and
complete Ledger journal facts. Generated types live in
`gen/events/lifecycle/v1`; `events/lifecycle/v1` provides strict `Encode`,
`Decode`, `Validate` and validated Kafka `Key` helpers. These bindings do not
replace the existing transitional JSON envelope in `events/v1`.

The initial contract carries exact string quantities, stable business
metadata, domain versions, complete debit/credit entries, and customer balance
snapshots. The codec rejects malformed/oversized records and inconsistent
dimensions while preserving unknown additive fields. Validation cannot prove
that a producer captured the true committed state; transaction-level tests
and reconciliation remain required before adoption.

See [ADR 0007](../docs/adr/0007-projection-lifecycle-contracts.md) for keys,
account-version chaining, compatibility, rollout, rollback and remaining work.
No lifecycle producer or projection storage is deployed by these contracts.

```bash
go test ./api/events/lifecycle/v1
```
