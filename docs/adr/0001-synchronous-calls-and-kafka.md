# ADR 0001: Use synchronous calls only for immediate decisions

- Status: Accepted
- Date: 2026-09-20

## Context

Praxis needs both immediate request decisions and reliable propagation of facts.
Using Kafka for every interaction would make an HTTP caller wait for an
unbounded asynchronous round trip. Using RPC for every interaction would
couple background consumers to service availability and make durable replay
difficult.

No service may keep a database transaction open while calling another service.
Cross-service workflows are sagas: every service commits locally, operations
are idempotent, and later failures are handled by compensation or
reconciliation rather than distributed ACID transactions.

## Decision

Use synchronous gRPC when the result determines the response currently being
returned to a caller. Give every call a deadline and propagate correlation and
trace context.

Use Kafka for facts that have already happened, commands whose caller does not
need an immediate result, fan-out, projections, notifications, reporting, and
reconciliation. Publish database-derived messages through a transactional
outbox.

The current and planned interactions are classified as follows:

| Interaction | Transport | Reason |
|---|---|---|
| Order → Risk `CheckOrder` | Synchronous gRPC | Admission needs an allow/deny decision. |
| Order → Ledger `ReserveForOrder` | Synchronous gRPC | Admission must know whether funds were reserved. |
| Order → Matching `SubmitOrder` | Synchronous gRPC | Admission must know whether Matching accepted and sequenced the order. |
| Approved deposit workflow → Ledger `PostDeposit` | `ledger.commands.v1` | Blockchain finality and risk screening occur asynchronously; the workflow can observe `LedgerPosted` later. |
| Matching → downstream consumers | `matching.events.v1` | Trades and cancellations are ordered facts used by accounting and projections. |
| Ledger → downstream consumers | Transactional outbox → `ledger.events.v1` | Committed accounting facts must survive process failure and support fan-out. |
| Notification, reporting, reconciliation | Kafka consumers | They must not increase admission latency or availability coupling. |

`ledger.commands.v1` is not part of order admission. A producer selects that
Kafka topic as record metadata; the topic name is not duplicated in the JSON
envelope.

## Failure semantics

- A synchronous timeout means the outcome may be unknown, not necessarily that
  the remote operation failed. Retry only with the same idempotency key.
- If Matching fails after Ledger reserved funds, Order requests an idempotent
  release. Reconciliation detects reservations that remain uncertain.
- Kafka delivery is at least once. Consumers deduplicate by stable message ID
  before applying business effects and commit offsets only after local state is
  durable.
- Poison records use bounded retries and a DLQ/replay process; consumers do not
  skip them silently.

## Consequences

- Order admission has explicit latency and availability dependencies on Risk,
  Ledger, and Matching.
- Background services remain decoupled and can catch up after an outage.
- Producers and consumers require stable IDs, compatible schemas, outboxes,
  inbox deduplication, monitoring, and operational replay procedures.
- A new interaction must document whether its caller needs an immediate result;
  convenience alone is not a reason to choose RPC or Kafka.
