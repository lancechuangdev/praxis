# ADR 0011: durable projection recovery, private reads and cutover

Status: implemented for existing Order workflow and Ledger journal streams;
live AWS/Atlas acceptance pending.

## Scope

Phase 6 completes the existing `order.events.v1` workflow and
`ledger.journals.v1` journal streams. Full execution history needs Matching
fill/terminal sources that are not implemented. Notification history needs its
own source contract. Those histories remain explicitly gated, as agreed for
this implementation. Workflow progress never becomes an inferred fill status.
Current-balance APIs remain conditional on measured need; customer activity
contains historical snapshots and cannot authorize a mutation.

## Durable acknowledgement and recovery

The executable uses a durable MongoDB recovery queue before domain processing.
This supersedes ADR 0008's executable-level fail-stop-on-every-record policy;
the direct workflow/journal processors and low-level consumer constructors keep
their original transaction and commit contracts.

A Kafka source record is acknowledged only after a majority/snapshot transaction
has retained its exact topic, partition, offset, key and bytes. A built-in
unique source-coordinate ID makes ambiguous source commits harmless. Reusing
coordinates with changed bytes is an error. A valid record is pending; a
malformed supported-size record is retained in quarantine in the same durable
handoff. This queue is the projection DLQ. It does not require a second Kafka
publication to make a source commit safe.

A separate applier processes at most 32 due records per batch. Domain effects
still use the complete snapshot/journal plus inbox transaction. Only afterward
is the retained record marked done. A crash between those commits replays the
domain inbox receipt. Multiple appliers can race safely through conditional
writes, unique IDs and transactional quota updates.

Gaps remain pending with bounded persisted backoff. Later records can be
retained and applied, including a predecessor later in the same Kafka partition.
Stale, conflicting or invalid facts are quarantined; dependency failures retry.
The queue admits at most 1024 active records and 64 MiB of active source/key
bytes, with a 2 MiB transport-record ceiling. Saturation fails source consumption
without acknowledging an unretained record. Quarantined records count against
the quota. Completed records remain as redelivery receipts. No TTL, automatic
skip, deletion or business side effect is installed.

An operator can inspect an exact quarantine ID and selectively retry only after
supplying its inspected SHA-256 digest and a bounded repair reference. Original
bytes remain unchanged; operator reason/time/count are retained. Retry never
bypasses domain validation. Missing facts must come from retained authoritative
history or a new reconciled rebuild. The implementation cannot invent uncaptured
history or implicitly treat a nonempty account as version zero.

## Private read boundary

Read APIs default off. JWT validation requires RS256, the configured public key
and key ID, exact issuer/audience, an expiring token with issued-at and at most a
one-hour lifetime, a nonempty canonical user subject, and `projection:read` scope.
The subject supplies ownership; URL parameters and caller headers cannot
select another owner. Token/key material and raw dependency errors are not
returned by diagnostic endpoints.

Workflow snapshots, the per-order workflow timeline and customer journal
activity use owner-filtered queries and explicit indexes. Activity strips other
customers' entries and balances, including counterparties. Financial quantities
and versions retain exact string representations. Queries have bounded pages
and deadlines; cursors bind owner, view and storage generation. GET responses
include private cache policy, version fields and representation ETags, with
conditional requests evaluated only after authorization and ownership checks.

A read generation must have a matching verified reconciliation report and be
the current compare-and-swap read route. Pending/quarantined records, failed
consumers or a route mismatch make reads unavailable. This is a conservative
read gate, not a claim of zero broker lag or a substitute for authoritative
financial reads. The projection module contains no financial mutation client.

## Rebuild and cutover

Replay creates a separate rebuild generation. JSONL exports retain typed
Protobuf bytes; original events are processed only through projection stores.
The input is bounded to 128 MiB and 100000 records per artifact. Partition
ranges are checked, and complete artifact/checkpoint metadata is retained after
all queued effects drain. Re-running the same artifact is idempotent.

Snapshot-import coordinates have their own artifact-digest namespace. They do
not collide with actual Kafka offsets when the new generation starts consuming
retained records. Domain event IDs and exact source digests remain unchanged.

Authoritative export commands run read-only repeatable-read PostgreSQL
transactions. Order verifies complete capture through current saga state.
Ledger verifies every captured account's chain from empty and compares its
latest exact buckets/version/identity with authoritative balances. Ledger
exports are topologically ordered by account dependencies rather than time or
journal ID. Exports are
published atomically as new files; incomplete exports cannot replace a prior
artifact. Unsupported legacy baselines and capture gaps fail export.

Reconciliation compares the expected source state, history and inbox receipts
with a consistent MongoDB snapshot, including document counts and stable digest.
A rebuild report also requires a complete checkpoint for the same artifact.
Extra/missing records or incorrect identities, versions, amounts or receipts
block verification. Comparison is bounded to 400000 documents.

Operators pause and join consumers, drain recovery queues, export authoritative
state, reconcile, then activate using the expected current route revision.
Activation rechecks the target digest and empty queue. A changed target or
concurrent route update fails closed. Route changes retain an atomic audit of
previous/current selections and activation time. Old generations, reports, source topics
and receipts remain available. Rollback uses the old verified report and the
current route revision; it does not mutate the new generation or financial state.
Resume ingestion after route selection. Rebuild is projection-only and never
replays commands, financial mutations or notifications.

## Deployment and evidence

The opt-in `infra/projections` module supplies a dedicated replica set, private
Atlas endpoint, exact-topic MSK grants, generation-scoped database users,
Secrets Manager URIs, Pod Identity associations and private workload manifests.
Runtime credentials cannot provision/drop collections or change read routes.
Provisioning/reconciliation credentials are separate. URI secrets are fetched
with a deadline and never printed. The production template uses private
network-policy destinations and digest-pinned, non-root images. Source flags,
consumer activation and customer reads remain independently controlled.

Local PostgreSQL and MongoDB replica-set tests cover capture/export consistency,
reordered recovery, quota rollback, replay, inbox ambiguity, owner isolation,
pagination, conditional GETs and cutover. Terraform validation and mock plans
provide configuration evidence. They do not establish live IAM, DNS, TLS,
network reachability, credential rotation, broker acceptance, Atlas behavior,
query cost, retention coverage or operational cutover readiness. Those remain
explicit live acceptance gates. See the [runbook](../phase6-operations.md).
