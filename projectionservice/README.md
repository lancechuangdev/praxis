# Projection service foundation

This module begins Phase 6's derived consumer platform. It currently contains
an Order workflow transaction processor, MongoDB and Kafka adapters, and tests.
It has no executable, deployed consumer/store, read API or delivery provider.
MongoDB Atlas remains the planned projection store. PostgreSQL remains
authoritative and projected values cannot authorize financial mutations.

`internal/workflow.New` requires an isolated generation, `shadow` or `rebuild`
mode, and an implementation of the transactional `Store` contract. `Process`
strictly validates lifecycle bytes, topic and order key before storage. Its
transaction deduplicates exact events, enforces contiguous per-order versions
starting at 1, preserves owner identity, and writes the full snapshot and
receipt together. Recognized older duplicates are harmless; unknown stale
facts, gaps and conflicting IDs remain unacknowledged.

`internal/mongostore.New` implements the storage contract for an explicit,
caller-owned MongoDB client, database and target. Deployments must supply TLS,
authentication, private connectivity and a bounded client connection pool.
The in-memory store exists only in processor tests.

Successful `applied` and `duplicate` outcomes are eligible for source commit,
and the Kafka adapter preserves sequential processing and source commits.
Every error prevents acknowledgement. There is no automatic DLQ skip, history
bootstrap, source offset reset or read cutover. Consumer activation still needs
live storage/broker tests, bounded gap handling, DLQ handoff,
metrics, retention policy and reconciliation evidence.

Run the protocol tests from the repository root:

```bash
go test -race ./projectionservice/...
```

See [ADR 0008](../docs/adr/0008-workflow-projection-transaction-protocol.md) for
the protocol, acceptance requirements and rollback boundaries.

## MongoDB workflow adapter (not activated)

Create a store bound to one target with a positive transaction timeout of at
most 30 seconds. Call `Prepare(ctx)` explicitly with provisioning privileges
before processing a new generation. It creates `workflow_<mode>_<generation>`
and `workflow_inbox_<mode>_<generation>`, retaining the built-in unique `_id`
indexes for order IDs and event IDs respectively. Use ordinary unsharded
collections with default binary collation and no TTL indexes. The adapter does
not inspect existing collection options, sharding, extra indexes or retention;
review those independently before reuse. No collections are dropped or cleared
by the adapter. The caller owns client shutdown.

Each callback runs sequentially in a session transaction with snapshot read
concern, primary reads and majority write concern. Snapshot and inbox writes
share the session context. Existing snapshots are replaced only when the stored
version and owner still match the previously read state; first snapshots use
unique inserts. The driver retries transient transaction errors and uncertain
commits. Duplicate-key races and conditional-write conflicts receive at most
three additional whole-transaction retries with jitter. A single context budget
bounds session work, retries and commit; session cleanup has a separate one-second
budget. Exhausted/ambiguous failures return no successful processor outcome.

Snapshot documents contain owner, workflow version, event ID, occurrence-time
metadata and the complete Protobuf fact as binary data. Exact quantities and
nanosecond occurrence time remain in that payload; BSON date metadata uses
millisecond precision. Serialization retains unknown additive fields. The
inbox digest covers the original incoming bytes, independently of snapshot
serialization. These are workflow snapshots, not a completed customer order
history or execution-status API.

Optional integration tests require a disposable MongoDB replica set or test
Atlas deployment that supports transactions:

```bash
PROJECTION_TEST_MONGODB_URI='mongodb://localhost:27017/?replicaSet=rs0' \
  go test -race ./projectionservice/internal/mongostore -run TestMongo -v
```

The tests create uniquely named `praxis_projection_it_*` databases and drop only
those databases during cleanup. The test identity therefore needs database
creation/drop privileges; never use a production endpoint. Tests cover
concurrent first-event delivery, duplicates, contiguous updates, gap blocking,
rollback after both writes and mode/generation isolation. When the URI is unset
they skip; passing unit tests is not evidence of deployed Atlas correctness.
Database outage, failover and ambiguous server commit exercises remain live
acceptance gates. Roll back by stopping future consumers and retaining both
collections, inbox receipts and source offsets. Runtime wiring, DLQ, metrics endpoints, bootstrap,
reconciliation and read cutover remain pending.

## Kafka workflow adapter (not activated)

`internal/kafkaconsumer.New` accepts explicit brokers, a verified TLS 1.2+
Kafka dialer with the deployment's SASL configuration, a target-bound store,
and a positive commit timeout of at most 30 seconds. The constructor creates
the workflow processor for that same target. The store must be bound to it;
the MongoDB store rejects mismatches. No environment credentials are loaded.

`GroupNamespace` is required and must uniquely identify the environment and
destination database. Groups have the form
`projection-workflow-<namespace>.<mode>.<generation>`. Namespace and generation
names allow 1–64 ASCII letters, digits, hyphens and underscores, starting with
a letter or digit. Independent databases must never use the same namespace
with the same mode/generation on the same Kafka cluster. Retain the namespace
for restarts; use a new generation for rebuilds.

The reader subscribes only to `order.events.v1`, starts new groups at the first
retained offset, has a one-record queue, a 2 MiB fetch-byte setting, bounded dial
and read-backoff settings, and synchronous explicit commits. Broker/client
limits must accommodate the 1 MiB event contract plus record overhead. Reader
internals may prefetch per assigned partition; these settings are not a total
process-memory cap. Retained history must still include version 1 or an
explicit future bootstrap; starting at the earliest retained offset does not
prove complete history.

`Run(ctx)` processes and commits one fetched record before fetching another.
Only `applied` and `duplicate` outcomes allow commit. Every fetch, processing or
commit error stops the loop and closes the reader. This conservatively blocks
all partitions assigned to that reader rather than committing past a failed
head. No malformed event, gap or stale/conflicting fact is automatically skipped
or DLQed. Operators must resolve the cause before restarting; do not add an
unbounded automatic restart loop around deterministic failures.

The consumer is single-use, including after cancellation. Restart by creating
a new consumer with the same target and namespace so it fetches from retained
group offsets. Reusing a reader after it fetched a failed record could advance
past that record. A lost offset-commit acknowledgement may redeliver a fact;
the projection inbox handles that duplicate. Kafka commits and MongoDB commits
are not a distributed transaction or exactly-once transport.

`Run` owns reader shutdown. Cancel its context and join it before closing the
MongoDB client. `Close()` releases a constructed consumer that was never run;
it rejects calls after Run has started. Reader close has no context argument,
so live shutdown timing remains an acceptance exercise. `Prometheus()` exposes
active state and fetched/applied/duplicate/committed and failure counters without
business labels. They are not yet served by an HTTP endpoint and do not measure
broker lag. Process/commit failures include cancellation returned by those calls.

Controlled-reader tests cover commit ordering, failed-head barriers, unknown
outcomes, ambiguous commit/redelivery, commit deadlines, duplicate starts,
cancellation and target-group isolation. Live Kafka rebalance, broker failure,
offset recovery and end-to-end Atlas tests remain required. There is no deployed
consumer or production activation in this slice. Roll back future consumption
by stopping it and retaining group offsets, inboxes, snapshots and source topics.
