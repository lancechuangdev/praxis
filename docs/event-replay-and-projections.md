# Event replay and projections

Status: design guidance; Praxis does not yet provide general replay or
projection-rebuild tooling.

Event replay means processing previously stored events again. It lets a
developer repair or create derived data without repeating the original
business transaction.

## Projection

A projection is a read-optimized view derived from authoritative records or
events. For example, an order event stream may contain:

```text
OrderPlaced(order_1, quantity=10)
OrderPartiallyFilled(order_1, quantity=4)
OrderPartiallyFilled(order_1, quantity=3)
OrderCancelled(order_1)
```

A consumer can project that history into a table that is easy for an API to
query:

```text
order_id:          order_1
status:            cancelled
original_quantity: 10
filled_quantity:   7
remaining_quantity: 3
```

The event history describes what happened. The projection stores the current
or summarized result. Examples include order status, portfolio positions,
daily trading volume, account summaries, search indexes, reporting tables, and
notification feeds.

A projection should be disposable and rebuildable when its source retains all
information needed for reconstruction. Authoritative financial state is not
automatically a projection: Praxis Ledger journals and invariants remain the
source of truth even if downstream services build balance or reporting views.

## How replay helps development

Replay is useful when:

- a consumer bug produced incorrect historical values;
- a new service or projection needs a backfill;
- a projection database or search index was lost or corrupted;
- a schema or calculation changed;
- a failed DLQ record is ready to be tried again;
- a new consumer version needs validation against historical traffic; or
- an incident must be reproduced from the original input.

For example, assume a reporting consumer accidentally excluded fees from daily
volume. Fixing the code corrects only future records. Replaying retained trade
events through the fixed consumer can rebuild all affected totals.

## Replay types

| Type | Trigger | Purpose |
|---|---|---|
| Kafka redelivery | Automatic after processing or offset-commit failure | Retry an unacknowledged record |
| DLQ replay | Operator after the underlying problem is fixed | Recover selected failed records |
| Offset-range replay | Operator resets or uses a new consumer group | Reprocess retained topic history |
| Projection rebuild | Deployment or recovery job writes new storage | Reconstruct an entire derived view |
| Idempotent command replay | Caller repeats the same command ID | Resolve an ambiguous outcome without repeating its effect |
| Workflow replay | Operator returns durable workflow state to processing | Continue a previously dead-lettered saga |

A retry is normally an immediate, automatic response to a transient failure.
A replay is commonly a later, deliberate operation over durable historical
input.

## Safe projection rebuild

Prefer a new consumer group and new destination tables rather than deleting a
live projection in place:

```text
retained Kafka events
        |
        v
new consumer group: reporting-v2-rebuild
        |
        v
new tables: reporting_v2
        |
        v
validate -> catch up -> switch reads -> retire old tables
```

A typical rebuild is:

1. Record the source topics, schema versions, retention boundary, and target
   code version.
2. Create isolated projection storage and a new consumer group.
3. Consume from the earliest required retained offsets.
4. Apply events in partition order and track progress.
5. Validate counts, totals, invariants, gaps, and duplicates against the
   authoritative system.
6. Catch up to live traffic while continuing validation.
7. Switch reads atomically or through a controlled deployment.
8. Keep an audit record and a rollback path before retiring the old view.

Using a new group is important because the existing group's committed offsets
represent its current progress. Using new tables is important because an
existing inbox may correctly classify every historical event as already
processed and because in-place rebuilding can expose partial results.

Kafka retention sets the maximum self-service replay window. If required
events have expired, reconstruction needs another authoritative source, such
as Ledger journals, an immutable archive, or a database snapshot plus events
after the snapshot.

## Idempotency and ordering

Every replayable event needs a stable event ID. Normal incremental consumers
should atomically apply the projection change and record the event ID in an
inbox:

```sql
BEGIN;

INSERT INTO inbox_events (consumer_name, event_id)
VALUES ($1, $2)
ON CONFLICT DO NOTHING;

-- Apply the projection only if the insert claimed the event.

COMMIT;
```

The inbox and projection update must share one local transaction. A separate
check followed by a separate insert has a race and a crash window.

Deduplication does not repair ordering. If a projection requires per-account or
per-market order, messages must use a suitable Kafka key and the consumer must
detect sequence gaps or stale versions. Rebuilding with more partitions or a
different partitioning rule can change ordering assumptions.

## Side effects

Do not blindly replay consumers that send money, place orders, reserve funds,
send email, invoke webhooks, or call other external systems. Repeating a
historical input can repeat the external effect even when a projection update
would have been harmless.

Use one of these patterns:

- split projection updates from side-effect delivery into different consumers;
- run the replaying consumer in shadow mode with external effects disabled;
- direct output to isolated test topics and databases;
- enforce durable idempotency at the external-effect boundary; or
- rebuild from authoritative state instead of replaying commands.

An event describing a completed transfer may be safe for a reporting
projection. Replaying a command that initiates a transfer is a different and
much higher-risk operation.

## Validation and observability

A replay or rebuild should expose:

- replay job ID, initiator, reason, code version, and time range;
- source topic, partition, starting offset, current offset, and target offset;
- processed, skipped, duplicate, and failed record counts;
- oldest event age and estimated completion time;
- invariant and reconciliation failures; and
- final validation and cutover outcome.

Rate-limit replay so historical work cannot starve live traffic or overload
databases and downstream services. Pause on deterministic failures rather than
creating an automatic DLQ-to-source loop.

## Current Praxis support

Praxis currently supports:

- Kafka redelivery of offsets that were not committed;
- Ledger operations that safely return existing results for repeated stable
  command or event IDs; and
- durable retries of eligible Order saga steps before they dead-letter.

Praxis does not currently provide:

- a DLQ replay CLI, API, or worker;
- controlled consumer-group offset replay;
- an automated projection rebuild and cutover workflow; or
- a recovery operation for dead-lettered Order sagas.

These capabilities require explicit authorization, audit, rate limiting,
idempotency tests, side-effect controls, and operational runbooks before they
are safe for production use.

