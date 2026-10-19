# Phase 6 operations: existing workflow and journal streams

Repository scope is the two implemented lifecycle streams. Full execution and
notification histories remain gated on their missing authoritative sources.
AWS/Atlas deployment and live verification are pending; these commands do not
imply that a production environment is enrolled.

## Provision and enroll

1. Build `projectionservice/Dockerfile` from the repository root and select an
   immutable image digest. Order/Ledger images also include `/workflow-export`
   and `/journal-export` respectively.
2. Configure and review the independent `infra/projections` module. Supply the
   existing VPC/private subnets, application security groups, EKS cluster, MSK
   cluster, publisher roles and exact per-generation service accounts/groups.
   See its README. Apply only in the selected environment after normal review.
3. Apply source migrations and grant the runtime roles access to the new
   outbox/delivery tables before enabling source capture/publication. Account
   for the additional single-connection dispatch pools (and Order's optional
   observation connection) in the database budget.
4. Enable `ORDER_LIFECYCLE_CAPTURE=true` and
   `LEDGER_LIFECYCLE_CAPTURE=true` consistently across writers. Existing capture
   gaps/legacy baselines require reviewed history recovery; a zero version is
   not a placeholder for missing data.
5. Enable `ORDER_WORKFLOW_DISPATCH=true`, set `ORDER_KAFKA_BROKERS`,
   `ORDER_KAFKA_AUTH_MODE=msk_iam`, and `AWS_REGION`. Enable
   `LEDGER_JOURNAL_DISPATCH=true` with Ledger's existing brokers and MSK IAM
   configuration. These flags default off; retained facts can drain separately
   from capture. Publication is at least once with stable bytes and IDs.
6. Configure each projection process with `PROJECTION_ENABLED=true`, stream
   `workflow` or `journal`, mode `shadow` or `rebuild`, a unique generation,
   explicit database and group namespace. Runtime Atlas access comes through
   `PROJECTION_MONGODB_URI_SECRET_ARN` (a secret containing `{"uri":"..."}`),
   or an explicit URI for local/operator use. Configuring both sources fails.
   Use the module's `PROJECTION_SECRETS_MANAGER_ENDPOINT` value for private
   secret retrieval; its dedicated endpoint does not change VPC-wide DNS.
7. With separate provisioning credentials, run `projection-service prepare`.
   It creates and verifies domain/history, recovery and control collections and
   required indexes. It performs no Kafka operations, resets no offsets, and
   drops/repairs nothing. Normal startup only verifies existing storage.
8. Deploy using the narrower runtime secret. Do not enable customer reads yet.
   Enable the opt-in projection scrape job in `infra/k8s`. Validate source
   dispatch metrics, consumer lag, queue health, retention,
   identities and private connectivity in the actual environment.

`PROJECTION_WORKFLOW_ENABLED` remains an enable alias; prefer
`PROJECTION_ENABLED`. Do not set conflicting values. Kafka settings use
`PROJECTION_KAFKA_AUTH_MODE=msk_iam` and explicit brokers/namespace. Direct
low-level consumer constructors still support same-head gap retries; the
executable's durable queue handles reordering independently of that option.

## Recovery and quarantine

Source commits mean retained queue records, not necessarily applied views.
`projection_*_enqueued_total`, queue apply counters, queue active bytes/records,
quarantine count and group lag must be assessed together. Queue observation
failure omits gauges rather than reporting an empty queue. Readiness reports a
running source loop; customer reads additionally enforce the route/report and
empty-queue gates.

Pending gaps retry with persisted backoff from one to 60 seconds. Later source
records can supply predecessors. Missing retained facts, conflicts and invalid
payloads require an operator; the process does not invent state or publish a
financial command. If the 1024-record/64-MiB active quota fills, source consumption
stops without losing the failed head. Repair/drain with the source stopped,
then restart a fresh consumer using the same group. Preserve both the queue
and Kafka offsets.

Use an operator identity with database permissions, not the read API:

```bash
projection-service inspect  # bounded metadata-only quarantine page
projection-service inspect --id 'ledger.journals.v1:0:123'
projection-service retry --id 'ledger.journals.v1:0:123' \
  --expected-digest '<digest returned by inspect>' --reason '<repair ticket>'
projection-service drain
```

Inspection without an ID lists at most 50 metadata records; use `--cursor`
for the next page. Exact-ID inspection returns source bytes and must be treated
as customer data. Retry
retains the original bytes and records its reason/time/count. An unresolved
invalid record will quarantine again. Drain has a 30-second budget and refuses
to claim success with pending/quarantined records. Do not edit quota counters,
rewrite source bytes, remove inbox receipts or skip a Kafka offset manually.
For unrecoverable source history, retain the blocked generation and rebuild a
new generation from reviewed retained authoritative facts.

## Rebuild, reconcile and switch reads

Use fresh `PROJECTION_MODE=rebuild` and `PROJECTION_GENERATION` values, with
separate target-scoped runtime/provisioning identities. Never reuse the active
consumer group for another database or generation.

1. Stop and join consumer processes involved in the cutover. Retain source
   publication and broker history. Drain each stopped target's recovery queue.
2. Produce complete authoritative exports using read-only database credentials:

   ```bash
   go run ./orderservice/cmd/workflow-export --output /secure/orders.jsonl
   go run ./ledgerservice/cmd/journal-export --output /secure/journals.jsonl
   ```

   Set `ORDER_DATABASE_URL` and `LEDGER_DATABASE_READER_URL` from the existing
   secret-management path; do not put secret values into tracked files.
   Exports are consistent snapshots with synthetic import coordinates, not
   claims about broker offsets. Each output must be a new file. Order export
   rejects uncaptured sagas or gaps. Ledger covers captured journals/accounts
   and rejects incomplete account chains or balance divergence. Missing old
   history requires explicit baseline work; these tools never fabricate it.
3. With the new target selected and operator credentials, run:

   ```bash
   projection-service prepare
   projection-service replay --input /secure/journals.jsonl
   projection-service reconcile --input /secure/journals.jsonl
   ```

   Select `workflow` and its export for Order. Replay/verification support at
   most 100000 records and 128 MiB per artifact; comparisons cap at 400000
   documents. A changed artifact belongs in a fresh rebuild generation.
   Replaying the identical artifact resumes safely. Complete checkpoints
   preserve the artifact digest and each input partition's start/exclusive end.
   Imported coordinates are namespaced apart from live Kafka coordinates.
4. Keep consumers stopped. Activate the returned verified report using the
   expected current read-route revision (zero only for first activation):

   ```bash
   projection-service activate --report '<report ID>' --expected-revision 0
   ```

   Activation rechecks the entire target digest and empty queue. A mismatch
   or concurrent activation fails. This selects a database read route; it does
   not change a gateway, DNS, source offsets or authoritative services.
5. Configure/route clients to the selected generation's private service, then
   resume its ingestion using its isolated group. Already imported facts are
   harmless duplicates when Kafka history is consumed. Verify lag, queue
   drain, amounts, ownership and representative read responses again.
6. Retain the prior generation and report. For rollback, stop/join the affected
   consumers, drain/reconcile the old target as needed, select its configuration,
   and activate its verified report with the current route revision. Restore
   gateway routing and resume that generation. No in-place rebuild or financial
   mutation is part of rollback.

## Authenticated reads

Customer reads require `PROJECTION_READS_ENABLED=true`, an external JWT issuer,
exact audience and key ID, and an RSA public PEM file (2048..8192 bits) mounted
through `PROJECTION_JWT_PUBLIC_KEY_FILE`. Set `PROJECTION_JWT_ISSUER`,
`PROJECTION_JWT_AUDIENCE`, and `PROJECTION_JWT_KEY_ID`. Keep signing keys in the
identity provider. Tokens use RS256, a canonical user ID in `sub`, issued-at,
expiry, a lifetime of at most one hour, and `projection:read` scope. Coordinate
public-key/key-ID rotation with token expiry and a service rollout.

Place the private ClusterIP services behind the existing TLS-authenticated
access boundary; do not expose their HTTP ports directly to the internet.
The provided policy permits only configured client/observability namespaces.
Supply `projection-jwt-public-key` with a `public.pem` entry when enabling reads.
Live TLS/identity/private-network acceptance remains required.

| Selected stream | Endpoint | Returned view |
| --- | --- | --- |
| workflow | `GET /v1/orders` | Customer's latest order workflow snapshots |
| workflow | `GET /v1/orders/{id}` | One owned workflow snapshot |
| workflow | `GET /v1/orders/{id}/history` | Owned workflow transitions in version order |
| journal | `GET /v1/activity` | Customer's journal activity and historical balance snapshots |
| journal | `GET /v1/activity/{id}` | One journal's customer-owned entries/balances |

Lists return up to 50 items and an opaque `next_cursor`; pass it as `cursor`.
Cursors bind the owner/view/generation. Unknown/cross-owner objects do not leak
other customers. Activity intentionally omits counterparties' entries/balances.
Responses retain exact strings, versions, private cache headers and ETags;
`If-None-Match` can return 304 only after authorization and ownership checks.
Unverified routes, pending/quarantined records, storage failure and stopped
consumers make reads unavailable. Views never authorize a reserve, withdrawal,
settlement or other financial mutation. These APIs do not claim execution fills
or notification history, or guarantee that the broker has zero lag.

## Local verification and remaining live gates

```bash
docker compose -f projectionservice/compose.test.yaml up -d --wait
LEDGER_TEST_DATABASE_URL='postgres://postgres:postgres@127.0.0.1:18765/phase6?sslmode=disable' \
ORDER_TEST_DATABASE_URL='postgres://postgres:postgres@127.0.0.1:18765/phase6?sslmode=disable' \
PROJECTION_TEST_MONGODB_URI='mongodb://127.0.0.1:27018/?replicaSet=rs0' \
make phase6-verify
```

These are disposable loopback-only test credentials. The verification script
requires all three database settings so transaction suites cannot silently
skip. Terraform validation/mock plan tests can run through
`make phase6-infra-verify` after provider initialization. Stop the disposable
stack with `docker compose -f projectionservice/compose.test.yaml down`.

Local PostgreSQL 16/MongoDB replica-set evidence does not establish live AWS
acceptance. Verify Atlas private DNS/SRV routing, security groups, network
policy, database privileges, backup/restore, MSK topic/registry compatibility,
IAM identity/renewal, secret/public-key rotation, query costs, retention windows,
backlog/failure recovery and staged cutover/rollback in the actual environment.
Full execution and notification histories remain gated until their owners
provide complete retained source facts. Current-balance API work remains
conditional on measured need. No production cutover was performed here.
