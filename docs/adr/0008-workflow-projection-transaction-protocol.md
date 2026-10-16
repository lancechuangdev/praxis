# ADR 0008: Order workflow projection transaction protocol

- Status: Accepted; processor implemented, storage and broker adapters pending
- Date: 2026-10-15

## Context

Order source capture and Kafka publication are opt-in foundations. A cancelled
publication may arrive late, and the broker acknowledgement/source-commit gap
may cause duplicate delivery. Row locks alone do not fence Kafka producers.
Phase 6 therefore needs explicit projection inbox and sequence behavior before
publication can be activated. Workflow snapshots describe saga progress, not
fills or customer execution status.

## Decision

Introduce `projectionservice/internal/workflow` as a transport-independent
processor over the lifecycle contract in ADR 0007. Keep service persistence and
projection logic outside the API module. The initial processor accepts only
`order.events.v1`, the typed Order workflow payload, and an exact order-ID key.
The codec's size, schema, producer and quantity validation runs before storage.
Producer metadata does not replace Kafka authentication and topic ACLs.

Every target is an explicit `(mode, generation)` pair. Modes are initially
`shadow` and `rebuild`; no live read or notification mode exists. Both workflow
documents and inbox receipts must be isolated by the entire pair. A rebuild
uses a new generation and, once broker wiring exists, a new consumer group.
The processor does not select storage collections or change read routing.

Within one durable transaction, the storage adapter must serialize changes to
the order, check a generation-scoped unique event-ID receipt, and atomically
write the full workflow fact and its inbox receipt. Serialization must cover a
previously nonexistent document too. A MongoDB adapter may use transaction
write-conflict retries or conditional version writes; a lookup followed by an
unconditional write is insufficient. Event IDs are unique across orders within
the target. The transaction callback has no external side effects and can be
retried by the adapter.

Receipts contain the SHA-256 digest of the original Protobuf bytes plus order
ID and workflow version. Identical redelivery is a duplicate even after later
versions are applied. Reusing an event ID with different bytes or coordinates
is a conflict; the processor does not normalize and accept re-encoded bytes.
Retain inbox receipts for the full supported replay window. Unknown additive
Protobuf fields remain in the stored full fact.

An empty projection accepts only version 1. An unseen next version must be
exactly the stored version plus one and preserve the authenticated owner key
captured in the original fact. Larger versions are gaps; unseen versions at or
below the stored version are stale conflicts requiring investigation. Neither
case writes an inbox receipt or advances the projection. Full snapshots do not
authorize skipping missing history. Existing Order rows whose first captured
version is greater than one therefore require a separately reviewed bootstrap;
this slice provides none.

Only a committed `applied` or `duplicate` outcome permits future source
acknowledgement. Storage failure, cancellation, an ambiguous commit and all
validation/sequence errors return no successful outcome. On ambiguous commit,
retrying the same bytes safely consults the inbox. The future Kafka adapter
must preserve contiguous partition commits: processing a later record must
never commit past a failed earlier offset. Gaps cannot be silently DLQed and
skipped. Invalid-record DLQ policy, bounded gap recovery, operator resolution,
lag/outcome metrics and retention enforcement remain separate work.

## Acceptance and rollback

The transaction-model tests cover identical and conflicting duplicates,
cross-order ID collisions, owner changes, gaps, stale versions, concurrent
duplicates, rollback at every storage operation/commit boundary, lost commit
acknowledgement, target isolation and validation before storage. They do not
prove a real storage adapter's atomicity or broker behavior.

Before runtime activation, implement and test Atlas transactions, unique
indexes, first-document races, database failure/ambiguous commit recovery,
partition offset barriers, bounded queues/gap recovery, atomic DLQ handoff,
telemetry and shadow reconciliation. The processor makes no financial calls
and cannot deliver notifications or authorize mutations. There is no consumer
runtime, deployed projection store, read API or cutover in this slice.

Rollback is a code revert before deployment. Future adapter rollback stops
consumption while retaining source topics, target generations, inbox receipts
and recorded offsets. No in-place rebuild or destructive migration is enabled.
