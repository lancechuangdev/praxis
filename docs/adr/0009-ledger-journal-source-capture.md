# ADR 0009: atomic Ledger journal source capture

Status: accepted for opt-in repository implementation; deployment acceptance pending.

Ledger lifecycle projections need complete journal entries and exact customer
balance snapshots from the same transaction that posts the journal. Reading
balances after commit could associate a journal with a later balance version.

Ledger therefore captures `LedgerJournalPosted` into the append-only
`ledger_journal_outbox` table before committing a financial transaction.
`LEDGER_LIFECYCLE_CAPTURE` defaults to `false`. When enabled, the executable
configures both the writer connection setting and the store commit hook.
A configuration mismatch fails the transaction.

The expand-only migration tracks one newly inserted journal per transaction and
the first previous version of each changed customer balance using
transaction-local PostgreSQL settings. Multiple updates to the same account,
including self-trade settlement, preserve its original previous version and
its final snapshot. Versions need not advance by exactly one within a journal.
A deferred constraint requires the journal's source fact before commit.

The commit hook reads all journal entries and affected customer balances, checks
that every changed customer balance belongs to the journal, validates the strict
lifecycle contract, and inserts immutable Protobuf bytes. Atomic amounts remain
decimal strings. Missing correlation or causation identifiers fall back to the
source command identifier. Event identity is `evt_lj_` plus the journal ID.
Idempotent replays create no new journal or fact. Validation or insertion failure
rolls back the financial mutation and its inbox receipt together.

Capture covers deposit posting, hold release, reservation, reservation release,
trade settlement and cancellation through the existing store commit paths.
It rejects transactions with multiple new journals. Nonempty version-zero
balances require a reviewed bootstrap before enrollment. Existing positive
versions also need a compatible projection baseline; enabling capture does not
create historical facts or make an existing account's next fact start at zero.

Disabling capture restores the original commit path and retains captured facts.
It introduces gaps for future projection consumption, so operators must plan a
new baseline or reconciliation before resuming. Keep writers consistently
configured during an enrollment; a mixed deployment can leave gaps.

Migration 006 adds separate mutable `ledger_journal_delivery` bookkeeping,
backfills existing source IDs and atomically queues new source facts through an
insert trigger. A source dispatcher locks one due delivery row through its
synchronous publication attempt, with a transaction deadline of at most 30
seconds. Competing workers skip locked rows. Failed validation or publication
retains the fact, with retry delay increasing from one second to a maximum of
60 seconds when the transaction remains usable. Cancellation or connection
failure rolls back delivery changes. Successful acknowledgement is retained;
source facts are never deleted or rewritten. A lost acknowledgement or commit
can cause duplicate delivery, so consumers must deduplicate stable event IDs.

The Kafka adapter validates payload coordinates, keys `ledger.journals.v1` by
journal ID, preserves source bytes and occurrence time, requires verified TLS
and synchronous all-in-sync-replica acknowledgements, and makes one send attempt
per dispatch. Authentication is supplied through a caller-owned transport. It
does not promise account ordering or producer fencing; a timed-out send may
still finish. Existing JSON event publication continues independently.

The dispatcher and Kafka adapter are foundations and are not wired into the
executable. Runtime enrollment, Ledger consumers, account gap handling,
bootstrap/rebuild tooling and live AWS/Atlas acceptance remain pending.

Repository verification uses `LEDGER_TEST_DATABASE_URL` with disposable
PostgreSQL databases. Integration tests cover exact amounts, replay, financial
rollback on invalid facts, deferred enforcement, the one-journal bound,
immutable facts, concurrent account chains, settlement and self-trades,
cancellation, disabled capture and writer configuration mismatch. Dispatcher
tests cover competing workers, immutable retries, backoff, retained
acknowledgement, malformed records and timeout recovery. Adapter tests cover
wire identity, TLS policy, synchronous acknowledgement and deadline propagation.
Run the full database suite with `go test -race -p 1 ./ledgerservice/...` because
the migration package deliberately tampers with checksums in its test database.
These tests
provide local evidence; they do not establish deployment acceptance.
