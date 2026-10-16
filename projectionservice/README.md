# Projection service foundation

This module begins Phase 6's derived consumer platform. It currently contains
an Order workflow transaction processor, MongoDB and Kafka adapters, and tests.
An opt-in executable now wires the adapters and serves health/metrics. No
consumer/store is deployed, and there is no read API or delivery provider.
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
before processing a new generation, or read-only `Verify(ctx)` for an existing
generation. Processing returns `ErrUnverified` until either succeeds.
`Prepare` creates `workflow_<mode>_<generation>`
and `workflow_inbox_<mode>_<generation>`, retaining the built-in unique `_id`
indexes for order IDs and event IDs respectively. Use ordinary unsharded
collections with default binary collation and no TTL indexes. `Prepare` finishes
by running the same read-only preflight as `Verify`. No collections or indexes
are dropped, repaired or cleared by the adapter. The caller owns client shutdown.

The preflight uses `hello`, full `listCollections` metadata and `listIndexes`
for both target collections, all under the store's context timeout. The runtime
identity needs permission to inspect collection options and indexes as well as
the normal transactional read/write privileges. Collection-creation privileges
are needed only for `Prepare`, so an existing generation can use a narrower
identity through `Verify`.

Verification requires a writable replica-set primary with logical sessions;
standalone and mongos endpoints are rejected. This initial check uses `hello`
and therefore requires a server supporting that command (MongoDB 5.0+). Direct
connections to shards are unsupported and must be excluded through endpoint
review; a replica-set handshake alone cannot distinguish those endpoints.
Each collection must be ordinary, with default options or explicit uncapped/
simple binary collation. Other options, including time-series, clustered or
validator configuration, are rejected pending a reviewed contract. Both
collections require the built-in `_id` index. TTL, partial, nonbinary-collation
and additional unique indexes are rejected. Ordinary nonunique secondary
indexes are permitted.

Any failed verification revokes the instance's verified state; subsequent
transactions are blocked until a successful check. Verification is a
point-in-time check, not a lock against later administrative changes. Recheck
after index/collection changes and before restarting consumers. In-flight
transactions are not cancelled by a failed recheck. Keep DDL/retention privileges
out of the runtime identity and control configuration changes independently.

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
rollback after both writes, mode/generation isolation, and TTL preflight
rejection/revocation/recovery. When the URI is unset
they skip; passing unit tests is not evidence of deployed Atlas correctness.
Database outage, failover and ambiguous server commit exercises remain live
acceptance gates. Roll back by stopping future consumers and retaining both
collections, inbox receipts and source offsets. Deployment, DLQ, bootstrap,
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
business labels. The executable serves them at `/metrics`; they do not measure
broker lag by themselves. The optional group-offset observer below supplies
lag separately. Process/commit failures include cancellation returned by those calls.

Controlled-reader tests cover commit ordering, failed-head barriers, unknown
outcomes, ambiguous commit/redelivery, commit deadlines, duplicate starts,
cancellation and target-group isolation. Live Kafka rebalance, broker failure,
offset recovery and end-to-end Atlas tests remain required. There is no deployed
consumer or production activation in this slice. Roll back future consumption
by stopping it and retaining group offsets, inboxes, snapshots and source topics.

## Opt-in executable

From the repository root, start disabled diagnostics with:

```bash
go run ./projectionservice/cmd/projection-service
```

Disabled mode opens only the HTTP listener and does not connect to MongoDB or
Kafka. `/healthz` returns 200; `/readyz` returns 503 because no consumer runs.
`/metrics` reports `projection_workflow_enabled 0`. HTTP defaults to `:8086`;
set `PROJECTION_HTTP_ADDRESS=127.0.0.1:8086` for local-only access. Protect the
listener through private network policy in a deployment. No financial or
customer read endpoint is registered.

Enable only in a reviewed non-production environment after provisioning and
verifying a dedicated destination generation and retained source history:

| Environment variable | Enabled-mode requirement/default |
| --- | --- |
| `PROJECTION_WORKFLOW_ENABLED` | Defaults to `false`; explicit `true` enables connections and consumption. |
| `PROJECTION_LAG_OBSERVE` | Defaults to `false`; opt-in group-offset observation while workflow runtime is enabled. |
| `PROJECTION_MODE` | Required: `shadow` or `rebuild`. |
| `PROJECTION_GENERATION` | Required isolated generation name. |
| `PROJECTION_GROUP_NAMESPACE` | Required unique environment/destination identity. |
| `PROJECTION_MONGODB_URI` | Required secret URI; verified TLS 1.2+ is enforced. |
| `PROJECTION_MONGODB_DATABASE` | Required explicit destination database. |
| `PROJECTION_KAFKA_BROKERS` | Required comma-separated `host:port` endpoints. |
| `PROJECTION_KAFKA_AUTH_MODE` | Defaults to `scram_sha512`; `msk_iam` selects AWS IAM OAuth. |
| `PROJECTION_KAFKA_USERNAME`, `PROJECTION_KAFKA_PASSWORD` | Required for SCRAM only; supply through the deployment secret path. |
| `AWS_REGION` | Required for MSK IAM. Credentials use the AWS SDK default provider chain at handshake time. |
| `PROJECTION_TRANSACTION_TIMEOUT` | Defaults to `10s`; positive, at most `30s`. |
| `PROJECTION_COMMIT_TIMEOUT` | Defaults to `5s`; positive, at most `30s`. |

The executable supports SCRAM-SHA-512 and MSK IAM OAuth over verified
system-root TLS for Kafka. It does not configure plaintext brokers or Kafka
client certificates. MongoDB URI TLS roots/certificates are
retained when supplied, insecure verification is rejected, and verified TLS is
enforced even when the URI omits TLS. The Mongo pool is capped at eight
connections with zero minimum, five-second connect/selection timeouts and a
ten-second client operation timeout. Transaction context deadlines also apply.

Startup validates configuration and runs read-only storage `Verify` before
constructing the Kafka consumer. It never calls `Prepare`, creates collections,
resets offsets or deletes data. Missing/unsafe collections or insufficient
inspection privileges fail startup. Runtime errors do not log source bytes,
credentials or raw database errors; inspect protected infrastructure diagnostics
when troubleshooting a failed preflight.

Readiness means the consumer loop is running, not that Kafka has assigned
partitions, the projection is caught up, or MongoDB remains healthy during an
idle period. The loop becomes unready before reader cleanup. A consumer failure
leaves HTTP running, returns 503 readiness, sets
`projection_workflow_consumer_failed 1`, and retains counters for inspection.
Health stays 200. `/status` retains sanitized process-local diagnostics as
described below. The process does not restart consumption. Resolve the error
and restart with the same target/namespace; do not reset offsets to bypass it.

On SIGINT/SIGTERM, the service cancels consumption, shuts HTTP down with a
five-second budget, joins the consumer and closes MongoDB with a five-second
cleanup context. Kafka reader cleanup is joined but has no context argument;
measure its timing before choosing a deployment termination grace period.
Startup verification remains point-in-time; control collection/index DDL and
recheck configuration before restarts. No live deployment, broker/Atlas outage
acceptance, DLQ skip policy, gap recovery, reconciliation or read cutover is
implied by this executable. Roll back consumption by disabling the flag and
restarting while retaining destination generations, inboxes and group offsets.

### MSK IAM authentication (opt-in)

Set `PROJECTION_KAFKA_AUTH_MODE=msk_iam`, `AWS_REGION` and the MSK IAM bootstrap
endpoints in `PROJECTION_KAFKA_BROKERS`. Keep the target and group namespace
stable when switching authentication for an existing destination. SCRAM secrets
are neither required nor used in IAM mode. Unsupported modes fail configuration
instead of falling back to another identity or plaintext.

The adapter uses the pinned AWS MSK signer already used elsewhere in Praxis.
Constructing configuration does not retrieve AWS credentials. Each SASL
handshake invokes the signer through the default AWS credential provider chain
under the earlier caller deadline or a five-second signing timeout. No custom
token cache is added. Empty tokens, control-character injection and tokens
expiring within five seconds are rejected. Cancellation prevents sending a
token even if signing finishes after cancellation. Signer errors and broker
challenges are sanitized; no tokens or credential-provider details are logged.
The broker still decides authentication and resource authorization. See the
[AWS signer](https://github.com/aws/aws-msk-iam-sasl-signer-go) and
[MSK client setup](https://docs.aws.amazon.com/msk/latest/developerguide/configure-clients-for-iam-access-control.html).

Provision the workload identity through the existing AWS identity path. Scope
its consume permissions to the destination's exact cluster, topic and group:

| Resource | Required consume actions |
| --- | --- |
| Accepted MSK cluster ARN | `kafka-cluster:Connect` |
| `order.events.v1` topic ARN | `kafka-cluster:DescribeTopic`, `kafka-cluster:ReadData` |
| Exact `projection-workflow-<namespace>.<mode>.<generation>` group ARN | `kafka-cluster:DescribeGroup`, `kafka-cluster:AlterGroup` |

These requirements follow [AWS's consume-policy documentation](https://docs.aws.amazon.com/msk/latest/developerguide/iam-access-control-use-cases.html).
The adapter does not create roles, attach policies, obtain bootstrap endpoints,
provision topics or enable runtime consumption. Rebuild groups need separately
reviewed permission scopes. Unit tests use injected signing and dummy local
credentials, covering OAuth framing, signing deadlines, new signing calls per
handshake, expiry/rejection, cancellation and redaction. No live AWS identity is
used by tests. Before rollout, verify role identity, allowed consumption,
denial outside the exact topic/group, credential renewal, broker reconnect and
shutdown in non-production. Live evidence remains pending. Roll back by stopping
consumption and restoring the prior reviewed authentication configuration while
retaining target collections and group offsets.

### Consumer failure diagnostics

`GET /status` returns JSON with `enabled`, `failed`, and, when enabled, a bounded
consumer snapshot. Responses use `Cache-Control: no-store`. This is an internal
operations endpoint protected by the same private-listener/network policy as
metrics; it is not a customer projection API.

The consumer reports a stage (`not_started`, `fetch`, `process`, `commit`,
`stopped` or `closed`), the current source coordinates when known, the last
commit acknowledgement observed by this process, and a fixed failure stage and
reason. Coordinates contain only the fixed source topic, partition and offset.
No business key, event ID, payload, headers, credentials, broker response or raw
error is included. Returned snapshots do not expose mutable internal state.

For example, a version gap can leave this diagnostic fragment:

```json
{
  "stage": "stopped",
  "current": {"topic": "order.events.v1", "partition": 0, "offset": 11},
  "last_committed": {"topic": "order.events.v1", "partition": 0, "offset": 10},
  "failure": {"stage": "process", "reason": "gap"}
}
```

`projection_workflow_failure{stage="process",reason="gap"} 1` exposes the same
failure as a gauge. Labels come from bounded internal codes; offsets and business
identifiers never become metric labels. Reasons are `gap`, `stale`, `conflict`,
`invalid`, `deadline`, `cancelled`, `authentication`, `authorization`, `closed`
or generic `error`. Normal parent-context cancellation does not set failure.
The gauge is absent before failure and resets with a new process/consumer.

Use a failed process/commit coordinate to identify the retained source record
through authorized broker tooling. A fetch failure has no current coordinate;
the preceding successful record is not marked as faulty. A failed commit may
have succeeded at Kafka despite a lost acknowledgement, so it is not added to
`last_committed`. These fields are neither a durable checkpoint nor a complete
partition/group inventory. They do not measure consumer lag and must never be
used to reset offsets, skip a gap or select read cutover. Preserve the original
group and let inbox deduplication resolve uncertain redelivery. Snapshot memory
is bounded independently of event count and partition cardinality. Tests cover
blocked-head coordinates, prior acknowledgements, commit ambiguity, sanitized
reason codes, caller mutation and graceful cancellation.

### Broker group-lag observation (opt-in)

Set `PROJECTION_LAG_OBSERVE=true` in an enabled workflow runtime to add group-lag
metrics. It is ignored when workflow runtime is disabled, which continues to
open no remote connections. A separate Kafka transport uses the same verified
TLS/SASL configuration, fixed `order.events.v1` topic and derived consumer group.
It queries topic metadata, committed group offsets and retained log start/end
offsets. It does not join the group, read event payloads, commit offsets or
change consumer readiness. It continues observing after the consumer stops.

Each metrics scrape has one two-second budget covering discovery and offset
queries; overlapping scrapes fail collection immediately instead of queueing
additional broker work. The observer accepts 1–256 partitions and rejects
partial, duplicate, unexpected or errored partition responses. Client transport
idle connections close after HTTP shutdown; no monitoring goroutine is added.
The existing IAM consume scope includes the topic/group describe permissions
needed by these queries. Live IAM authorization and query cost still require
verification in the target environment.

| Metric | Meaning |
| --- | --- |
| `projection_workflow_lag_observation_enabled` | Whether this process has the observer configured. |
| `projection_workflow_lag_observation_up` | Complete broker query collection succeeded; absent when disabled. |
| `projection_workflow_lag_partitions` | Number of partitions discovered for this sample. |
| `projection_workflow_lag_uncommitted_partitions` | Partitions for which Kafka returned no committed group offset. |
| `projection_workflow_lag_out_of_range_partitions` | Committed offsets before retained start or after observed log end. |
| `projection_workflow_lag_complete` | All discovered partitions have usable committed offsets within retained ranges. |
| `projection_workflow_lag_offsets` | Sum of log-end minus committed next-offset across all partitions; emitted only when complete. |
| `projection_workflow_lag_max_partition_offsets` | Largest partition offset lag; emitted only when complete. |

Failed collection emits only `observation_up 0`; it never serves stale cached
values or a healthy zero lag. Successful queries with uncommitted/out-of-range
offsets emit `complete 0` and omit aggregate lag. Missing commits, even on empty
partitions, are not treated as proof of complete history. An offset below log
start can indicate lost retained history: investigate before resuming; no offset
reset or automatic history bootstrap is performed.

Lag is an offset distance, not an exact number of business events, latency or
projection correctness. Offset holes, compaction and transaction markers can
affect interpretation. Broker calls are sequential observations, not an atomic
Kafka checkpoint; group progress is sampled before log ends to reduce ordinary
forward-progress inconsistencies. Compare against the Kafka group inspection
tools during acceptance; [Apache Kafka documents group offsets and log ends](https://github.com/apache/kafka/blob/trunk/docs/operations/basic-kafka-operations.md).
Do not use zero lag for read cutover without authoritative reconciliation.

Every replica observes the same group, so do not sum these gauges across pods.
The metrics have no per-partition, user or event labels. Set scrape cadence and
alert thresholds from deployed query-cost and backlog measurements; monitor
both collection health and coverage before interpreting lag. Unit tests cover
correct next-offset arithmetic, missing commits, retention/truncation bounds,
partial failures, cardinality limits, overflow, cancellation and concurrent
scrapes. Live Kafka/IAM lag validation remains pending. Roll back by disabling
the observation flag and restarting; consumption state and source offsets are
unchanged by monitoring.
