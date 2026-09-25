# Ledger reconciliation and analytics

This document defines how Praxis separates operational reconciliation from historical analytics and reporting. These are target architecture decisions; the S3, Athena, and analytical pipelines are not yet implemented in this repository.

## Workload classification

Reconciliation is not a single workload.

**Operational reconciliation** answers bounded, low-latency questions:

- Did a trade receive every required settlement leg?
- Which clearing legs have been pending for more than 30 seconds?
- Does an outbox event have a corresponding consumer result?
- What is the current clearing exposure by shard and asset?
- Which recent balance projection differs from its Ledger version?

**Historical reconciliation and reporting** scan and aggregate large datasets:

- Reconstruct every balance from years of journal entries.
- Compare journals across every physical Ledger shard.
- Calculate monthly fees and trading volumes by market.
- Produce regulatory, treasury, finance, and auditor reports.
- Analyze long-term clearing exposure and settlement latency.

The first category fits an indexed PostgreSQL read model. The second is OLAP and should not run on the authoritative Ledger writer.

## Preferred architecture

```text
Ledger shard transactions
        │ transactional outbox
        ▼
       Kafka
        │
        ├─► DynamoDB balance projector
        │      current customer balance views
        │
        ├─► PostgreSQL reconciliation projector
        │      recent settlement state and exceptions
        │
        └─► S3 Parquet exporter
                 │
                 ▼
              Athena
                 │
                 └─ historical reconciliation and reports
```

The authoritative Ledger remains responsible for financial decisions and immutable journals. Every downstream store is rebuildable from authoritative records and must never authorize spending or withdrawals.

## PostgreSQL operational reconciliation

Use a separate PostgreSQL projection database for current reconciliation state, recent history, and exception queues. Initially, low-volume queries may use Ledger read replicas, but analytical growth must not consume capacity required for replication and customer reads.

Representative tables include:

```sql
CREATE TABLE settlement_status (
    trade_id          TEXT PRIMARY KEY,
    symbol            TEXT NOT NULL,
    matching_sequence BIGINT NOT NULL,
    buyer_leg_status  TEXT NOT NULL,
    seller_leg_status TEXT NOT NULL,
    expected_legs     INTEGER NOT NULL,
    completed_legs    INTEGER NOT NULL,
    first_seen_at     TIMESTAMPTZ NOT NULL,
    settled_at        TIMESTAMPTZ,
    last_error        TEXT
);

CREATE INDEX settlement_status_pending
    ON settlement_status(first_seen_at)
    WHERE settled_at IS NULL;
```

```sql
CREATE TABLE clearing_position (
    physical_shard TEXT NOT NULL,
    asset_id       TEXT NOT NULL,
    amount_atomic  NUMERIC NOT NULL,
    version        BIGINT NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (physical_shard, asset_id)
);
```

Operational queries are selective and indexed:

```sql
SELECT trade_id, symbol, buyer_leg_status, seller_leg_status
FROM settlement_status
WHERE settled_at IS NULL
  AND first_seen_at < now() - interval '30 seconds'
ORDER BY first_seen_at
LIMIT 1000;
```

PostgreSQL is appropriate here because the service needs transactions, uniqueness constraints, exact numeric values, idempotency, indexes, and predictable point/range queries.

## Historical data in S3

Export immutable Ledger facts to S3 in Apache Parquet format. Partition by event date and physical shard:

```text
s3://praxis-ledger-history/
  journals/event_date=2026-09-24/shard=0/part-000.parquet
  journals/event_date=2026-09-24/shard=1/part-000.parquet
  settlement_legs/event_date=2026-09-24/shard=0/part-000.parquet
  clearing_snapshots/snapshot_date=2026-09-24/part-000.parquet
```

Include stable identifiers and lineage fields:

```text
event_id
journal_id
trade_id
settlement_leg_id
user_id
asset_id
symbol
amount_atomic
balance_version
matching_sequence
physical_shard
virtual_shard
routing_epoch
occurred_at
source_topic
source_partition
source_offset
schema_version
```

Parquet is columnar, allowing analytical queries to read only the required partitions and columns. S3 lifecycle policies can move older immutable data to cheaper storage without changing the Ledger retention policy.

## Athena reporting

Athena queries the S3 dataset with SQL. It is the preferred initial analytical engine because it is serverless and suitable for periodic or ad hoc reports.

Example cross-shard clearing check:

```sql
SELECT
    asset_id,
    SUM(amount_atomic) AS net_clearing_atomic
FROM clearing_entries
WHERE event_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-30'
GROUP BY asset_id
HAVING SUM(amount_atomic) <> DECIMAL '0';
```

Example fee report:

```sql
SELECT
    symbol,
    asset_id,
    SUM(amount_atomic) AS fee_atomic
FROM journal_entries
WHERE account_type = 'trading_fee_revenue'
  AND event_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-30'
GROUP BY symbol, asset_id;
```

Athena reports are not part of the order or settlement hot path. Query delay or temporary unavailability must not block financial mutations.

## Exact financial representation

Never use binary floating-point values for financial quantities.

Use canonical atomic-unit integers where their maximum range is known, or exact decimal types when necessary:

```text
PostgreSQL: NUMERIC or validated atomic-unit integers
Parquet:    DECIMAL(precision, scale) or integer atomic units
Athena:     DECIMAL(precision, scale)
Redshift:   DECIMAL(precision, scale)
```

Every event must state its asset and representation. Do not infer decimal precision from display symbols.

## Reconciliation controls

Reconciliation must validate more than row counts.

### Per-trade controls

- Exactly one buyer and one seller settlement leg exist when required.
- Every leg has the expected payload fingerprint.
- Every leg maps to one immutable journal.
- Fees match the trade's declared fee policy.
- Matching sequences have no unexplained gaps.

### Per-account controls

- Projected balances equal the latest authoritative balance version.
- Available, reserved, pending, and hold buckets reconcile to journal movements.
- Customer buckets do not become negative unless an explicitly reviewed product permits credit.

### Per-asset controls

- Debits equal credits across customer and system accounts.
- Aggregate MCLS clearing positions equal zero after settled trades.
- Nonzero pending exposure is explained by identifiable incomplete legs.
- Custody positions reconcile with the internal Ledger under the custody policy.

### Pipeline controls

- Source Kafka offsets are monotonic per partition.
- Event IDs are unique and duplicate delivery is harmless.
- Export watermarks identify the exact included event range.
- Late events update the appropriate historical partition deterministically.
- The analytical dataset records its schema version and transformation version.

## Checkpoints and reproducibility

Each reconciliation run records:

```text
reconciliation_run_id
input watermark per source partition
routing epoch
schema and transformation versions
query or rule version
started_at and completed_at
result totals
exception count
output artifact locations
```

A reviewer must be able to rerun a report against the same immutable inputs and obtain the same result. Corrections create new events and new report versions rather than mutating previously issued evidence without an audit trail.

## Alerting and exception workflow

Create alerts for:

- incomplete settlement beyond its service-level threshold;
- clearing exposure above an asset or shard limit;
- source-to-projection version gaps;
- missing, duplicated, or conflicting settlement legs;
- failed S3 exports or stalled Kafka offsets;
- nonzero unexplained per-asset totals;
- reconciliation runs that miss their completion deadline.

Exceptions need explicit lifecycle states:

```text
open → assigned → explained/remediated → verified → closed
```

Store the evidence, operator identity, timestamps, and compensating journal IDs. Never repair financial history by directly editing posted journal rows.

## When to introduce Redshift

Athena is preferred initially for periodic and ad hoc analysis. Introduce Redshift only when measurements show a sustained need for:

- frequent complex dashboards;
- many concurrent analysts or BI clients;
- repeated large joins across Ledger and business datasets;
- predictable interactive latency;
- continuously running analytical workloads where a warehouse is more economical.

Redshift does not replace S3 as the immutable analytical archive. It is a serving layer that can be rebuilt from governed source data.

## Data-access boundaries

```text
Financial mutation or decision  → authoritative Ledger writer
Current customer balance        → DynamoDB projection
Recent history and exceptions   → PostgreSQL read model
Historical reports and audits   → S3 and Athena
High-concurrency BI, if needed   → Redshift
```

No analytical store is an authority for reservations, withdrawals, trade booking, or clearing completion.

## Delivery stages

1. Add versioned, complete Ledger events and immutable export identifiers.
2. Build PostgreSQL operational reconciliation projections and alerts.
3. Export journals and settlement facts to partitioned Parquet in S3.
4. Add Athena tables, workgroups, encryption, retention, and governed report queries.
5. Automate daily balance, clearing, custody, and pipeline reconciliations.
6. Add Redshift only after Athena workload and concurrency measurements justify it.
