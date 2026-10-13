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
services. This module contains only wire contracts and generated transport code.

## Risk v1

`risk.v1.RiskService/CheckOrder` is the versioned internal admission contract.
Its request carries the complete order and reservation inputs needed for a
deterministic decision; quantities and atomic amounts remain strings to avoid
precision loss. Its response uses an explicit decision enum and includes
stable decision and policy identifiers for comparison and audit. An
unspecified decision is not approval. Trace context is transported as standard
gRPC metadata rather than protobuf fields.
