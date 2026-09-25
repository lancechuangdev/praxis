# Ledger scaling and Market-Clearing Ledger Sharding

This document describes how the Praxis Ledger can scale from its current single-writer PostgreSQL design to **Market-Clearing Ledger Sharding (MCLS)**. MCLS is a target architecture, not an implemented feature in this repository.

For operational reconciliation, S3/Athena reporting, and analytical data controls, see [Ledger reconciliation and analytics](ledger-reconciliation-and-analytics.md).

## Goals and invariants

Scaling must preserve these invariants:

- A customer cannot spend more than their available or reserved balance.
- Every local journal is balanced for each asset.
- Commands and settlement legs are idempotent.
- Posted journals are immutable; corrections use compensating entries.
- Matching produces one authoritative sequence per symbol.
- Cross-shard obligations are explicit, measurable, and eventually settle to zero.
- Withdrawals cannot consume unsettled credits.

Throughput is secondary to these guarantees.

## Current architecture

The current infrastructure has one Ledger RDS Multi-AZ DB cluster:

```text
Ledger pods
    │ writer pool
    ▼
Ledger RDS cluster
    ├─ one writer
    └─ two readable standbys
```

Writes, reservations, trade booking, idempotency checks, and read-after-write traffic use the writer. Replica-safe ordinary reads use the reader endpoint. The writer endpoint follows the writer role during failover and is not AZ-aware.

The current design is the appropriate starting point. Adding application pods increases service availability and request concurrency, but it does not add PostgreSQL write capacity.

## Scale the single cluster first

Use the following progression before introducing MCLS.

### 1. Measure the actual constraint

Track at least:

- committed reservations and trades per second;
- p50, p95, and p99 transaction duration;
- writer CPU, memory, IOPS, throughput, WAL rate, and storage latency;
- row-lock and transaction-lock waits;
- active, idle, and waiting connections;
- autovacuum lag, dead tuples, and table/index bloat;
- replica lag and outbox backlog;
- latency by application AZ and writer AZ.

Do not shard based only on customer count or trading-pair count.

### 2. Optimize transactions

- Keep transactions short and acquire locks in deterministic order.
- Batch journal-entry inserts.
- Use `UPDATE ... RETURNING` to avoid unnecessary round trips.
- Keep only necessary indexes on hot write tables.
- Move reporting, reconciliation scans, and analytics off the writer.
- Partition large append-only journal, inbox, and outbox tables by time when measurements justify it.

### 3. Scale vertically

Increase the RDS instance class, provisioned storage, and IOPS. This retains one atomic PostgreSQL transaction for a trade and is operationally much simpler than sharding.

### 4. Control connections

Treat database connections as a cluster-wide budget:

```text
maximum potential connections = Ledger pods × writer pool size
```

Reduce per-pod pools as pod count grows. Introduce RDS Proxy or PgBouncer when connection churn or aggregate pool size becomes material. A proxy manages connections; it does not add write CPU or remove row contention.

### 5. Offload reads

Use the reader endpoint or disposable projections for stale-tolerant balance displays, history, reporting, and administration. Keep reservations, trade booking, cancellations, idempotency checks, and read-after-write operations on the writer.

Publish versioned balance projections through the outbox. A cache that has observed balance version `123` must reject replica or projection version `122`.

## Preferred read models for Praxis

Praxis should use two read models as demand grows:

1. **DynamoDB for current balance projections.**
2. **A separate PostgreSQL projection for account and settlement history.**

Do not add Redis, OpenSearch, or a data warehouse until measurements establish a separate need for them.

### DynamoDB balance projection

Store one item per user and asset:

```text
PK: USER#alice
SK: ASSET#USDT

available_atomic: 3000000000
reserved_atomic:  7000000000
balance_version:  123
```

A balance projector consumes committed Ledger outbox events and applies an item only when its version is newer than the stored version:

```text
incoming balance_version > stored balance_version → update
incoming balance_version ≤ stored balance_version → ignore
```

This makes duplicate and out-of-order event delivery safe. DynamoDB serves wallet screens, portfolio views, ordinary `GET /balances` requests, repeated polling, and WebSocket snapshots. It must never authorize reservations, trades, or withdrawals.

The committed mutation response remains the most recent read-after-write result. For example, if `POST /orders` returns balance version `123` while DynamoDB still contains `122`, the caller retains `123` until the projector catches up.

### PostgreSQL history projection

When history traffic begins to load the Ledger replicas materially, introduce a separate projection database for:

- transaction, deposit, withdrawal, and settlement history;
- pagination and date ranges;
- filters by asset, operation type, or status;
- customer-support and administrative timelines.

An illustrative table is:

```sql
CREATE TABLE account_activity (
    user_id         TEXT NOT NULL,
    activity_id     TEXT PRIMARY KEY,
    activity_type   TEXT NOT NULL,
    asset_id        TEXT NOT NULL,
    amount_atomic   NUMERIC NOT NULL,
    status          TEXT NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL,
    ledger_version  BIGINT NOT NULL
);

CREATE INDEX account_activity_user_time
    ON account_activity(user_id, occurred_at DESC);
```

The intended flow is:

```text
                         ┌─► DynamoDB balance projector
Ledger outbox → Kafka ───┤
                         └─► PostgreSQL history projector
```

Request routing is:

```text
Financial mutation or decision  → authoritative Ledger writer
Current balance display         → DynamoDB
Account history                 → projection PostgreSQL
Immediate result after mutation → committed mutation response
```

Initially, implement only the DynamoDB balance projection and continue sending history queries to the Ledger reader endpoint. Add the separate history database only after measured history traffic justifies another datastore.

Redis is deferred because DynamoDB already provides a durable, scalable key-value balance view; adding Redis immediately would create another consistency layer. OpenSearch is deferred until broad investigative search is required, and a warehouse is reserved for analytics, regulatory reporting, and long-range reconciliation rather than customer-facing reads.

## When MCLS becomes justified

MCLS is justified only after a tuned, vertically scaled writer cannot satisfy measured throughput or latency objectives.

A workload where approximately 66% of trading is `BTC-USDT` is not well balanced by assigning one physical database per market: the BTC-USDT database would still own most writes. Matching also cannot arbitrarily split one order book because BTC-USDT requires one price-time ordering authority.

MCLS therefore separates matching order from financial settlement:

```text
BTC-USDT Matching authority
            │ immutable TradeExecuted
            ▼
Settlement Coordinator
            │
            ├─► Ledger shard 0
            ├─► Ledger shard 1
            ├─► Ledger shard 2
            └─► Ledger shard 3
```

## Ownership and routing

Assign each customer to a stable virtual shard:

```text
virtual_shard = hash(user_id) % 1024
```

Map virtual shards to a smaller number of physical shards:

```text
virtual 0–255     → physical shard 0
virtual 256–511   → physical shard 1
virtual 512–767   → physical shard 2
virtual 768–1023  → physical shard 3
```

Do not use `hash(user_id) % physical_database_count`. Adding a database would remap most customers. Virtual shards let operators move a bounded ownership range.

The shard registry is versioned:

```text
virtual_shard | physical_shard | routing_epoch | state
42            | shard_0        | 7             | active
```

Every mutation carries its virtual shard and routing epoch. A destination rejects stale epochs so an outdated router cannot write to the previous owner after a move.

## Clearing-based settlement

Consider this trade:

```text
Alice buys 0.1 BTC from Bob for 7,000 USDT.
Alice belongs to shard A; Bob belongs to shard B.
```

Shard A books the buyer leg locally:

```text
Alice reserved USDT     -7,000
Shard-A clearing USDT   +7,000
Alice available BTC     +0.1
Shard-A clearing BTC    -0.1
```

Shard B books the seller leg locally:

```text
Bob reserved BTC        -0.1
Shard-B clearing BTC    +0.1
Bob available USDT      +7,000
Shard-B clearing USDT   -7,000
```

Aggregate clearing positions return to zero:

```text
BTC:  -0.1 + 0.1       = 0
USDT:  7,000 - 7,000   = 0
```

Each shard transaction is locally atomic and balanced. The two database transactions are not globally atomic; the coordinator makes partial completion durable and retryable.

## Settlement coordinator

Matching emits one immutable `TradeExecuted` event with a globally unique `trade_id`. The coordinator creates deterministic legs:

```text
trade-9001:buyer
trade-9001:seller
```

Its durable state machine is:

```text
pending → buyer_booked/seller_booked → settled
```

The legs may complete in either order. A timeout is retried with the same leg ID and payload fingerprint. Reusing an ID with different financial data is an error.

If one shard is unavailable, the completed leg remains committed and the missing leg remains pending. Clearing exposure shows the exact incomplete obligation. A reversal creates new compensating legs; it never modifies the original journal.

## Pending funds and withdrawals

Clearing does not make two clusters commit atomically. A customer credit can therefore be temporarily unsettled.

The conservative policy is:

- credit acquired assets to a `pending_settlement` bucket;
- permit reuse for internal trading only if risk limits allow it;
- prohibit withdrawal until both settlement legs complete;
- move pending funds to available after settlement;
- cap clearing exposure by shard and asset.

Immediate available credit is faster but means the exchange temporarily advances assets and must be treated as explicit credit exposure.

## Reconciliation

The Clearing Reconciler independently verifies:

```text
SUM(clearing balances) per asset = 0
```

It also checks:

- every settled trade has the required buyer and seller legs;
- every leg has exactly one journal and expected payload fingerprint;
- customer and system accounts conserve each asset;
- no sequence gaps or unexplained negative customer balances exist;
- pending duration and exposure remain below configured limits.

Reconciliation must use immutable records and be able to rebuild its conclusions from the trade log and shard journals.

## Terraform topology

Actual horizontal write scaling requires independent RDS clusters:

```text
ledger-shard-0: writer + two readable standbys
ledger-shard-1: writer + two readable standbys
ledger-shard-2: writer + two readable standbys
ledger-shard-3: writer + two readable standbys
```

Four physical shards mean four writers and eight standbys. Four schemas inside one cluster would still share one writer and would not horizontally scale writes.

Terraform should use `for_each` to provision per-shard clusters, secrets, IAM, alarms, migration Jobs, endpoints, and backups. It can also deploy the router, coordinator, shard workers, and reconciler.

Terraform must not decide live account ownership, move balances, retry financial legs, repair clearing exposure, or perform cutover. Those are versioned application workflows.

## Shadow verification versus unsafe dual writes

Do not synchronously make a production request depend on two unrelated commits:

```text
request → Legacy commit → MCLS commit → response
```

If one commit succeeds and the other times out, the caller cannot infer the second database's state.

For migration, safely **dual-process** one durable command stream:

```text
                         ┌─► Legacy Ledger — authoritative
Durable command/event ───┤
                         └─► MCLS — shadow
```

Reservations and trade events can go to both systems under different consumer identities. Legacy alone controls production order admission; MCLS cannot publish production events, authorize withdrawals, or serve customer balances.

Compare both decisions and economic results:

- reservation accepted or rejected;
- resulting available, reserved, and pending balances;
- balance versions;
- fees and journal totals;
- idempotent replay and conflict behavior;
- trade legs after the coordinator reports settlement complete.

A Legacy transactional outbox can additionally replay committed journals into MCLS. Original-command dual processing tests independent decision logic; journal replay proves that the target can reconstruct authoritative financial state. Using both gives stronger coverage.

## Migration plan while databases are empty

The repository currently has no production financial data, so use a simple parallel migration:

1. Add MCLS resources without changing or deleting the current Ledger cluster.
2. Provision the physical shard clusters and run identical schema migrations.
3. Deploy the router, settlement coordinator, shard workers, and reconciler with production outputs disabled.
4. Run reservation, same-shard trade, cross-shard trade, retry, crash, and reconciliation tests.
5. Confirm all clearing balances return to zero.
6. Switch Order and Matching configuration to MCLS.
7. Run load and failure tests.
8. Keep the old cluster temporarily with deletion protection.
9. Take and restore-test a final snapshot before explicitly removing it.

No database copying or CDC is necessary when there are no balances or journals to preserve.

## Migration plan with live financial data

For a future live migration, use expand–shadow–verify–cutover–contract:

1. Preserve the Legacy cluster and provision MCLS alongside it; a Terraform plan must show no Legacy replacement or destruction.
2. Create a consistent Legacy snapshot at a recorded journal/event watermark.
3. Load opening balances, reservations, and immutable history into their target virtual shards.
4. Start durable shadow processing for all changes after the watermark.
5. Keep Legacy authoritative while MCLS independently processes commands.
6. Compare every account, asset, decision, journal total, and global invariant until lag is zero and discrepancies remain zero for an agreed observation period.
7. Pause new order admission during a controlled cutover window.
8. Drain Matching, Ledger commands, and Legacy outbox events.
9. Apply the final watermark and verify balances, reservations, sequences, and clearing.
10. Advance the routing epoch and enable MCLS authority.
11. Resume admission and keep Legacy read-only for audit and recovery.

A gradual user-by-user cutover is harder because a trade may involve one customer on Legacy and another on MCLS. Either treat Legacy as a temporary clearing shard or prefer an all-at-once controlled cutover initially.

## Rollback

Before MCLS accepts authoritative writes, rollback only requires routing traffic back to Legacy.

After MCLS accepts writes, Legacy is stale. A safe rollback requires pausing admission, draining settlement, replaying every post-cutover MCLS journal into Legacy with deterministic IDs, verifying all totals, advancing the routing epoch again, and only then restoring Legacy authority. Fixing MCLS forward may be safer than reversing authority.

## Delivery stages

Implement MCLS only in measured stages:

1. Single-cluster observability and transaction optimization.
2. Vertical RDS and IOPS scaling.
3. Reader/projection offload and global connection budgeting.
4. MCLS domain model, virtual shard registry, and shadow mode.
5. Failure injection and reconciliation proof.
6. Production cutover only after explicit financial and operational review.
