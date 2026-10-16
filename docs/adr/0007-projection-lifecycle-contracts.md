# ADR 0007: Separate projection lifecycle facts from transitional event streams

- Status: Accepted; Order source capture implemented, Kafka producer rollout pending
- Date: 2026-10-14

## Context

Phase 6 requires retained facts sufficient to rebuild disposable customer
views. Existing Ledger JSON events carry journal identifiers but omit complete
entries and account balances. Matching emits acceptance facts, and Order saga
progress has no event stream. Those inputs cannot reconstruct full history.
In particular, Order's `accepted` saga state is not proof of a fill, settlement,
or a terminal customer execution state.

## Decision

Introduce `events.lifecycle.v1` Protobuf messages and an opt-in migration to
new projection inputs. Keep existing JSON producers, topics and consumers
unchanged until each owner can publish the new contract atomically. The
Protobuf decoder never infers lifecycle facts from legacy JSON or commands.

The initial contracts are:

| Fact | Owner | Planned topic/key | Version/ordering |
| --- | --- | --- | --- |
| `OrderWorkflowSnapshot` | Order | `order.events.v1`, `order_id` | Full workflow snapshot; positive, contiguous per-order workflow version allocated in the state/outbox transaction. |
| `LedgerJournalPosted` | Ledger | `ledger.journals.v1`, `journal_id` | Immutable complete journal, including every entry and every affected customer balance snapshot; no cross-journal or account Kafka ordering promise. |

`ledger.journals.v1` is a new journal-scoped stream. It does not change the key
or meaning of the existing `ledger.events.v1` stream in ADR 0003. Each customer
balance snapshot carries `previous_version` and `version` from authoritative
storage before and after the journal's transaction. A transaction can increment
an existing balance more than once, so the contract requires an increasing
version chain rather than an assumed increment of one. The projection checks
that `previous_version` matches its last applied version. It must hold or
quarantine a gap; it cannot resolve one using arrival time. Version zero is
only an explicit empty-account bootstrap, never a substitute for missing
historical state.

Complete journal entries balance debit and credit atomic amounts separately
for every asset. Customer entries require a matching account/asset snapshot
and a valid balance bucket. Snapshots include the authenticated owner key,
asset, all five balance buckets, and exact nonnegative values. These facts
support history and reconciliation, not financial authorization. The consumer
must atomically deduplicate the journal and update all affected documents;
a partial update cannot be acknowledged. Concurrent journals on different
partitions may arrive out of account order, so the future consumer must provide
bounded gap handling or authoritative snapshot bootstrap before activation.

Metadata includes stable event ID, explicit type, schema version, business
occurrence timestamp, correlation, causation and producer. IDs and versions
must persist in the producer outbox; retries cannot create new IDs, versions,
or occurrence times. Trace context remains transport metadata. Occurrence time
is the committed fact's business time, not relay or replay time. Consumers
must not treat producer strings as authentication; broker identities and ACLs
provide transport authorization.

The codec caps records at 1 MiB before decoding, rejects unknown schema
versions and unknown payload variants, validates exact quantities and required
dimensions, and preserves unknown additive Protobuf fields. Integer atomic
amounts use canonical base-10 strings of at most 78 digits, matching current
Ledger `NUMERIC(78,0)` precision. Order quantities retain the current API's
positive integer-unit semantics. Limit prices are exact positive decimal
strings, without exponents. No binary floating-point fields are introduced.
Malformed deterministic records will require DLQ handoff in the consumer
foundation; a codec error alone is not a safe offset-commit policy.

Order workflow states mirror existing saga progress: reservation pending,
reserved, accepted by Matching, release pending, released and dead-lettered.
They are not execution statuses. Reservation IDs and release reasons are
required only where meaningful. Fill, cancellation, expiration, reservation
lifecycle and notification sequence contracts remain separate work, owned by
their authoritative domains. No new customer-facing status is inferred here.

## Delivery, acceptance and rollback

The initial slice supplies contracts, generated Go types, validation and tests.
Order now additionally provides opt-in transactional source capture in its own
`order_workflow_outbox`, with database-allocated versions and an internal
ProtoJSON snapshot converted strictly to Protobuf by `EncodeWorkflowOutbox`.
Capture defaults off and has no activated Kafka publisher. A source-side dispatcher now
locks the earliest unpublished fact per order through acknowledgement, records
publication and applies bounded retry backoff. An unwired runtime worker adds
bounded concurrency, jittered startup/idle/error pacing, joined cancellation and
dispatch counters. It is not connected to Order startup or its metrics handler.
The Kafka adapter now supplies TLS-only synchronous all-in-sync-replica writes
to `order.events.v1` keyed by order ID, strict payload-coordinate validation,
immutable source bytes/time and a bounded send-call deadline. It disables
automatic topic creation and uses one send attempt per source dispatch. Kafka
activation remains pending. This is not producer fencing:
connection loss can permit late delivery, so Kafka activation still requires
fencing or consumer stale-version handling plus deduplication. The integration
suite covers concurrent claims, retry order, invalid heads and acknowledgement
loss; execution against PostgreSQL remains an acceptance requirement.

Independently opt-in source-outbox observation now reports pending
and retried counts, oldest pending age and explicit collection failure through
Order's metrics endpoint. It uses a separate one-connection read-only pool with
query deadlines, does not affect readiness and cannot activate publication.
Deployed query-cost and telemetry verification remain acceptance gates.

The expand-only source-capture migration reserves unpublished version 1 for
existing sagas and advances versions even
while capture is off, making missing history explicit. See the
[Order rollout and rollback procedure](../../orderservice/README.md#phase-6-workflow-source-capture).
No topics, registry, consumers or Atlas resources are provisioned. Before publishing:

1. Allocate stable event IDs, workflow versions and snapshots in each owner's
   domain transaction, with atomic outbox insertion and idempotent replay.
2. Register schemas and enforce the ADR 0005 compatibility baseline in the
   deployment pipeline. Protobuf generation/linting is not registry deployment.
3. Provision new topics with reviewed retention, keys, IAM/ACLs and partition
   policy; do not repurpose existing retained records.
4. Prove outbox crash-window behavior, duplicate handling, owner identity,
   multi-account snapshot completeness and concurrent account-version chains.
5. Reconcile shadow projections against authoritative PostgreSQL before any
   authenticated read API or read cutover is activated.

Rollback before publication is a code revert. During a future dual-publication
window, disable new publication/consumption while retaining old contracts and
streams; keep new data for audit and deterministic replay. No destructive
migration or in-place projection rebuild is authorized by this ADR.

## Consequences

The first slice establishes a typed foundation without pretending current
streams are rebuild-complete. The next implementation work is ordered Order
Kafka publication, atomic Ledger producer adoption and the consumer's
inbox/version/gap/DLQ foundation. The initial Order workflow transaction protocol
is now implemented in `projectionservice`; [ADR 0008](0008-workflow-projection-transaction-protocol.md)
defines its deduplication, sequencing and storage requirements. A MongoDB
transaction adapter is implemented with opt-in replica-set tests; deployed
storage verification, consumer runtime wiring, gap recovery and DLQ handoff
remain pending. A fail-stop Kafka consumer adapter now provides sequential
processing/commit barriers and isolated target groups; live broker acceptance
is still required.
Order-history completion still requires
authoritative Matching lifecycle facts. Atlas reads,
rebuild generations, reconciled cutover and financial-authorization isolation
remain Phase 6 exit requirements.
