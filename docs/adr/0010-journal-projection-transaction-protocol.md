# ADR 0010: atomic journal projection transactions

Current executable/recovery/read rollout is defined by
[ADR 0011](0011-projection-recovery-reads-and-cutover.md). Historical rollout
notes below describe earlier slices; direct processor/adapter contracts remain
applicable, while the executable now acknowledges through a durable queue.

Status: implemented for the processor, MongoDB adapter and opt-in Kafka runtime;
deployment acceptance and general gap repair pending.

Ledger journals can affect multiple customer accounts, and different journals
can arrive out of account order on `ledger.journals.v1`. A journal must never
leave some projected accounts updated while others remain unchanged.

The `projectionservice/internal/journal` processor therefore requires one
transaction for the complete journal, every affected customer balance and its
inbox receipt. The storage contract isolates each shadow/rebuild generation.
Journal IDs and inbox event IDs must each be unique within the target. The
processor validates the strict lifecycle contract, topic and journal key before
touching storage. An inbox receipt records the exact source-byte digest and
journal ID: an identical replay is a duplicate, and event-ID reuse with changed
bytes or coordinates is a conflict. An existing journal under a new event ID
is also a conflict.

The processor locks customer accounts in lexical account-ID order, including
absent accounts, then checks every snapshot before writing any of them. Storage
must serialize conflicting account changes or force a transaction retry. For
an existing positive version, owner and asset must match. The incoming
`previous_version` must equal the stored version, and the new version must be
greater. A journal can advance an account more than once, so an increment of
exactly one is not required. Stored zero is only an explicitly empty account;
it cannot stand in for missing historical balances.

A future previous version is a gap. A new version at or below the stored version
without a recognized inbox receipt is stale. An overlapping version chain is
a conflict. Every failure leaves all documents unchanged and the source record
unacknowledged. Occurrence time never determines account order. A predecessor
can later permit an unchanged gapped record to succeed; this processor neither
fetches missing history nor performs automatic repair or DLQ skipping.

Storage may retry the callback serially on transaction conflicts. It must
atomically commit all writes or roll them back, honor cancellation, and report
ambiguous commits as errors. Redelivery resolves a committed transaction using
the stable inbox identity. A successful applied or duplicate outcome permits
the future broker adapter to acknowledge only while preserving its partition
commit barrier.

Tests use a serial transaction model to cover multiple-account writes, exact
amounts, deterministic lock order, version jumps, replay, concurrent duplicate
delivery, ambiguous commit, rollback at each storage stage, identity conflicts,
gaps, stale records, cancellation and target isolation. They establish processor
behavior. A MongoDB adapter now stores `journals_<mode>_<generation>`,
`journal_accounts_<mode>_<generation>` and `journal_inbox_<mode>_<generation>`
in one majority-write, snapshot-read transaction. Account snapshots preserve
exact Protobuf amounts, owner and asset, with conditional version writes.
Absent accounts use unique inserts; conflicting writes retry the entire
transaction within the existing shared timeout and retry bounds. All three
collections require successful read-only topology/options/index preflight.
Preparation creates ordinary collections with built-in unique `_id` indexes
and installs no TTL or destructive cleanup. An explicit `prepare` command is
available; normal runtime only verifies existing collections.

The Kafka consumer reuses the workflow processing/commit barrier with a separate
`ledger.journals.v1` topic and `projection-journal-<namespace>.<mode>.<generation>`
group. Journal failures map to the shared diagnostic/retry classes while
retaining their journal error identities. Stream-specific status and metric
prefixes distinguish journal consumption. The executable selects one stream per
process with `PROJECTION_STREAM=journal` and `PROJECTION_ENABLED=true`;
`PROJECTION_WORKFLOW_ENABLED` remains a backwards-compatible enable alias.
Conflicting enable flags fail configuration. Both streams default to disabled.

Local replica-set integration tests verify concurrent replay, competing account
chains, rollback, gap recovery, exact stored coordinates and retention-policy
rejection. They do not establish live Atlas or Kafka behavior. The optional
same-head retries can observe an externally committed predecessor but cannot
repair a predecessor later in the same partition. General gap repair,
bootstrap, reconciliation, authenticated read APIs and deployment acceptance
remain pending. No projected balance can authorize a financial mutation.
