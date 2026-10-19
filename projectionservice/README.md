# Projection service

Phase 6 supports the existing Order workflow (`order.events.v1`) and Ledger
journal (`ledger.journals.v1`) streams. An independently configured process owns
one stream, shadow/rebuild generation and group namespace. PostgreSQL remains
authoritative. Full Matching execution and notification histories are gated on
missing source contracts; no fill status is inferred from workflow progress.
No current-balance endpoint or financial mutation client is provided.

## Processing and recovery

The executable first retains each Kafka record in a majority/snapshot MongoDB
transaction. Source coordinates, key and bytes are durable before offset
commit. Malformed supported-size records enter retained quarantine. The queue
admits at most 1024 active records/64 MiB, with a 2 MiB transport-record ceiling;
saturation stops consumption without acknowledging an unretained record.
Completed records remain as receipts for ambiguous commit/redelivery.

An applier processes bounded batches and uses the domain transaction protocol:

- Workflow updates atomically write the current snapshot, immutable transition
  history and exact-byte inbox receipt. Versions start at one and are contiguous;
  owners are stable.
- Journal updates atomically write the complete journal, every affected customer
  account snapshot and inbox receipt. Sorted account locks/conditional writes
  enforce stable owners/assets and exact previous-version chains. A journal
  may advance an account version more than once.

Effects commit before the queue record is marked done, so a crash replays the
inbox receipt. Gaps stay pending with persisted bounded backoff, allowing later
records to supply predecessors even within one Kafka partition. Invalid,
stale/conflicting records remain quarantined. Selective retry requires an
inspected source digest and repair reference and never bypasses validation.
No offset skipping, financial command replay or automatic state invention is
part of recovery.

The direct `workflow`/`journal` processors and low-level Kafka constructors
remain usable independently. Their fail-stop contracts are unchanged. Normal
executable ingestion uses `NewQueued`; same-head retry options apply only to
the direct consumer path.

## Storage and startup

`prepare` explicitly creates ordinary target-scoped collections and read indexes,
then verifies replica-set topology, collection options and identity/retention
indexes. Normal startup is read-only verification; missing or incompatible
storage blocks ingestion. No TTL, collection drop or destructive repair is
installed. Provisioning/reconciliation credentials are separate from runtime.

Workflow collections are `workflow_<mode>_<generation>`,
`workflow_history_<mode>_<generation>`, and `workflow_inbox_<mode>_<generation>`.
Journal collections are `journals_<mode>_<generation>`,
`journal_accounts_<mode>_<generation>`, and `journal_inbox_<mode>_<generation>`.
Each stream also has `queue_<stream>_<mode>_<generation>` and a matching
`queue_quota_...` collection. Shared control collections retain replay
checkpoints, reconciliation reports, current routes and route-change history.

The generation/mode is permanent for each store. Clients use verified TLS,
primary/majority reads and majority writes; domain transactions use snapshot
reads. Transaction deadlines and bounded race retries include commit. The
caller owns client shutdown. Standalone/mongos endpoints, TTL drift and
unsupported collection/index policies fail preflight.

Upgrades that add history/collections do not invent data for existing inbox
receipts. Build a fresh generation from complete retained source facts and
reconcile it before moving reads.

## Configuration

All features default disabled. `PROJECTION_WORKFLOW_ENABLED` is an enable alias;
prefer `PROJECTION_ENABLED`, and do not supply conflicting values.

| Setting | Purpose |
| --- | --- |
| `PROJECTION_ENABLED` | Enable the selected stream |
| `PROJECTION_STREAM` | `workflow` (default) or `journal` |
| `PROJECTION_HTTP_ADDRESS` | Health/metrics/read listener, default `:8086` |
| `PROJECTION_MODE`, `PROJECTION_GENERATION` | Explicit `shadow`/`rebuild` target |
| `PROJECTION_MONGODB_DATABASE` | Explicit destination database |
| `PROJECTION_MONGODB_URI` | Explicit TLS URI, or use the secret reference below |
| `PROJECTION_MONGODB_URI_SECRET_ARN` | Secrets Manager object with `uri`; retrieval has a five-second budget |
| `PROJECTION_SECRETS_MANAGER_ENDPOINT` | Optional HTTPS AWS private endpoint; emitted by the deployment module |
| `PROJECTION_GROUP_NAMESPACE` | Environment/database identity; use the emitted namespace from the deployment module |
| `PROJECTION_KAFKA_BROKERS` | Explicit TLS broker endpoints |
| `PROJECTION_KAFKA_AUTH_MODE` | `scram_sha512` or `msk_iam` |
| `PROJECTION_KAFKA_USERNAME`, `PROJECTION_KAFKA_PASSWORD` | SCRAM credentials when selected |
| `AWS_REGION` | Required for MSK IAM; credentials load during handshakes |
| `PROJECTION_TRANSACTION_TIMEOUT` | Store operation/transaction bound, default 10s, maximum 30s |
| `PROJECTION_COMMIT_TIMEOUT` | Source commit bound, default 5s, maximum 30s |
| `PROJECTION_LAG_OBSERVE` | Opt-in broker group-lag observation |
| `PROJECTION_READS_ENABLED` | Opt-in customer reads after reconciled route selection |
| `PROJECTION_JWT_ISSUER`, `PROJECTION_JWT_AUDIENCE`, `PROJECTION_JWT_KEY_ID` | Required read-token identities |
| `PROJECTION_JWT_PUBLIC_KEY_FILE` | RSA public PEM file for RS256 verification |

Configure one MongoDB URI source. URI/key/token values and raw dependency errors
are not exposed through diagnostics. Disabled mode constructs no external
clients. Admin commands require explicit target/storage configuration and
credentials but neither Kafka credentials nor a Kafka consumer.

Kafka groups are `projection-<stream>-<namespace>.<mode>.<generation>`. Never use
the same group namespace for independent destination databases. The deployment
module derives a namespace containing an environment/database hash.

## Endpoints and operations

`/healthz`, `/readyz`, `/status` and `/metrics` retain bounded, sanitized
process-local diagnostics. Readiness means a running source loop, not caught-up
financial state. Source commits mean durable enqueue; observe group lag,
`projection_queue_*` metrics and apply counters together. Failed/incomplete lag
samples never look like zero lag. Runtime stops on an ingestion/storage failure
and retains diagnostics; construct a fresh process to resume its group.

The private read APIs provide owned workflow snapshots, per-order workflow
history and customer-only journal activity. JWT subject and scope determine
ownership; other customers' journal data is stripped. Pages/cursors, deadlines,
exact strings, versions, private ETags and conditional GETs are implemented.
Unverified/inactive generations, pending/quarantined records and failed
consumers cannot serve customer reads. Reads remain nonauthoritative.

`prepare`, `inspect`, `retry`, `drain`, `replay`, `reconcile` and `activate` are
operator-only commands using explicit database credentials. Replay uses a new
rebuild target, durable per-record progress and completed artifact/partition
checkpoints. Snapshot-import coordinates cannot collide with live Kafka offsets.
Reconciliation checks snapshots, history and inbox receipts against independent
authoritative exports. Cutover uses an expected route revision and atomically
records route history; rollback retains both generations and their reports.
No command replays a financial mutation or sends a notification.

See the [operations runbook](../docs/phase6-operations.md) for source exports,
complete commands, limits, deployment, recovery and rollback. The
[deployment module](../infra/projections/README.md) supplies opt-in private Atlas,
MSK/IAM, secret and EKS configuration. Its image build context is the repository
root: `docker build -f projectionservice/Dockerfile .`.

## Verification and acceptance

`go test -race ./projectionservice/...` runs unit/protocol tests and skips real
storage tests unless `PROJECTION_TEST_MONGODB_URI` names a disposable replica
set. For mandatory PostgreSQL/MongoDB integration coverage use
`make phase6-verify`; see the runbook's disposable Compose stack and settings.
`make phase6-infra-verify` checks provider schemas and mocked plans without
applying resources. Local tests cover reordered recovery, quarantine, quota
rollback, crash-window deduplication, owner/cursor/ETag handling, replay,
reconciliation and route switching/rollback.

Local evidence does not establish live AWS/Atlas acceptance. IAM/secret renewal,
private DNS/TLS/network policy, broker authorization, Atlas permissions,
retention/backup, query costs and staged cutover remain environment gates.
Full execution/notification history remains gated on its owners' source facts.

Contracts: [ADR 0008](../docs/adr/0008-workflow-projection-transaction-protocol.md),
[ADR 0010](../docs/adr/0010-journal-projection-transaction-protocol.md), and
[ADR 0011](../docs/adr/0011-projection-recovery-reads-and-cutover.md).
