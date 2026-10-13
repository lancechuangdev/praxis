# Kafka topology inventory

Status: Phase 3 implementation inventory, based on repository behavior.

This document is the contract inventory for implemented Kafka paths. Proposed
topics are excluded until a producer or consumer exists. Production names are
the Terraform-managed names; local aliases are noted where they differ.

## Topics

| Topic | Local alias | Producer | Consumer group | Key | Payload and ordering |
| --- | --- | --- | --- | --- | --- |
| `ledger.commands.v1` | `ledger-commands` | External command producers; none implemented in this repository | `cex-ledger-service` | Producer-defined; the implemented consumer does not validate it | JSON command envelope. Kafka ordering is only per partition. Producers must key `TradeExecuted` and `OrderCancelled` by `engine_id:symbol`; deposit commands key by the affected user/asset account. |
| `matching.events.v1` | none | Matching Engine transactional outbox and its embedded relay | No implemented consumer | `symbol` | JSON event envelope. Per-symbol ordering is preserved while the partition count and key mapping remain unchanged. Implemented event type: `OrderAccepted`. |
| `ledger.events.v1` | `ledger-events` in Ledger outbox rows, mapped by Outbox Relay | Ledger transactional outbox and independent Outbox Relay | No implemented consumer | User-asset account for balance/reservation events; `engine_id:symbol` for booked trades | JSON event envelope. Ordering is per affected account or engine/symbol key, not globally. Implemented types: `LedgerPosted`, `HoldReleased`, `FundsReserved`, `FundsReleased`, and `TradeBooked`. |
| `ledger.events.dlq.v1` | none | Not implemented | Not implemented | Must preserve the source record key | Provisioned infrastructure only. It must not be used for `ledger.commands.v1`; that source requires its own `ledger.commands.dlq.v1`. |

## Implemented delivery guarantees

- Ledger and Matching write domain state and their outbox row in one PostgreSQL
  transaction.
- Relays publish synchronously with Kafka `RequireAll` acknowledgements and
  mark outbox rows published afterward. A crash in that window can redeliver,
  so delivery is at least once.
- The Ledger command consumer fetches one record, applies it, and commits the
  Kafka offset afterward.
- Ledger business operations use stable command or event identifiers and are
  idempotent at their domain tables.
- The Ledger consumer passes inbox metadata into the Ledger store. The
  financial mutation, its outbox events, and the `inbox_events` record commit
  in the same PostgreSQL transaction. The pre-check avoids unnecessary replay
  work, while the transactional insert closes the crash window.

## Compatibility and schema gaps

Current envelopes contain `id`, `type`, `aggregate_id`, `occurred_at`, and
`data`, with correlation and tracing fields present on producer output. They do
not carry an explicit envelope version, schema identifier, producer identity,
or content type. Consumers accept unknown JSON fields but reject unknown event
types.

Phase 3 must therefore add:

1. a versioned event envelope and a versioned DLQ envelope;
2. `ledger.commands.dlq.v1` and an atomic publish-plus-offset strategy;
3. deterministic versus transient failure classification; and
4. compatibility tests that cover old and new envelope versions.

## Partition changes

The order-sensitive topics above must not have their partition count increased
in place. Kafka's key-to-partition mapping can change when the count changes,
breaking per-key ordering across old and new records. Use the versioned-topic
migration procedure in [Kafka partition expansion](kafka-partition-expansion.md).
