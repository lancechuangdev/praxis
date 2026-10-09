# ADR 0003: Make Kafka keys and ordering scopes explicit

- Status: Accepted
- Date: 2026-10-08

## Context

Kafka guarantees record order only within one partition. Praxis uses ordering
for account-level Ledger operations and market-level Matching operations. A key
change or an in-place partition-count increase can move an existing business
scope to another partition and allow old and new records to be processed
concurrently.

Idempotency prevents duplicate effects; it does not restore order.

## Decision

Every topic contract declares its business ordering scope and stable record
key:

| Topic | Key | Ordering scope |
| --- | --- | --- |
| `order.events.v1` | `order_id` | One order lifecycle |
| `risk.events.v1` | `user_id` | One user's risk changes |
| `ledger.commands.v1` | `account_id` | Commands affecting one account |
| `ledger.events.v1` | `account_id` | Committed facts for one account |
| `matching.events.v1` | `market_id` | One market's canonical sequence |
| `notification.commands.v1` | `user_id` | One user's notification stream |
| `*.dlq.v1` | Original key | Preserve the source record's scope |

The key is Kafka record metadata, not a substitute for the corresponding field
inside the typed payload. Producers must use one reviewed partitioner and test
its output with representative keys.

No contract promises global order across partitions or topics. Consumers that
need stronger protection include a domain version, sequence, or epoch and
reject stale records or stop on unexplained gaps.

Changing a key, partitioner, ordering scope, or partition count is a migration,
not an ordinary configuration edit. Order-sensitive topics use either a
stop-drain-change-resume procedure or a versioned replacement topic with an
explicit barrier, validation, and rollback point. An online in-place partition
increase is prohibited unless a reviewed design proves that every affected
consumer is order-insensitive.

## Enforcement

- Contract tests verify keys and required sequence/version fields.
- Topic inventory records producers, consumers, partition counts, and ordering
  assumptions.
- Capacity tests include the expected dominant-market skew.
- Per-partition lag and hot-key metrics precede any topology change.
- Migration evidence records offsets, barriers, topic configuration, and
  reconciliation results.

## Consequences

- One dominant account or market cannot be scaled merely by adding partitions.
- Hot scopes require admission control, subdivision with new correctness rules,
  or a dedicated owner.
- Some consumers may use different derived topics when their ordering and
  fan-out needs differ from the canonical stream.

