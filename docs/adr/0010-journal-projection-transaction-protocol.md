# ADR 0010: atomic journal projection transactions

Status: accepted for the processor foundation; adapters and deployment pending.

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
behavior, not MongoDB durability or live Kafka delivery. MongoDB and Kafka
adapters, bounded cross-partition gap handling, bootstrap, reconciliation,
read APIs and deployment acceptance remain pending. No projected balance can
authorize a financial mutation.
