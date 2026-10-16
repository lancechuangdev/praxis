# Projection service foundation

This module begins Phase 6's derived consumer platform. It currently contains
an Order workflow transaction processor and transaction-model tests; it has no
executable, deployed storage, broker connection, read API or delivery provider.
MongoDB Atlas remains the planned projection store. PostgreSQL remains
authoritative and projected values cannot authorize financial mutations.

`internal/workflow.New` requires an isolated generation, `shadow` or `rebuild`
mode, and an implementation of the transactional `Store` contract. `Process`
strictly validates lifecycle bytes, topic and order key before storage. Its
transaction deduplicates exact events, enforces contiguous per-order versions
starting at 1, preserves owner identity, and writes the full snapshot and
receipt together. Recognized older duplicates are harmless; unknown stale
facts, gaps and conflicting IDs remain unacknowledged.

The store interface is not a persistence implementation. An adapter must
serialize first-document creation, enforce target-scoped unique inbox IDs,
isolate all generations/modes, and return success only after durable atomic
commit. An ambiguous commit returns an error; redelivery retains the original
ID and bytes. The in-memory store exists only in tests.

Successful `applied` and `duplicate` outcomes are eligible for source commit,
but a future broker adapter must also enforce contiguous partition commits.
Every error prevents acknowledgement. There is no automatic DLQ skip, history
bootstrap, source offset reset or read cutover. Consumer activation still needs
real storage tests, Kafka offset barriers, bounded gap handling, DLQ handoff,
metrics, retention policy and reconciliation evidence.

Run the protocol tests from the repository root:

```bash
go test -race ./projectionservice/...
```

See [ADR 0008](../docs/adr/0008-workflow-projection-transaction-protocol.md) for
the protocol, acceptance requirements and rollback boundaries.
