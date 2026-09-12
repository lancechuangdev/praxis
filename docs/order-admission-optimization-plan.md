# Order-admission optimization plan: 10,000 requests/s with pool size 48

## Objective

Sustain an offered load of **10,000 order-admission requests per second** with
the Ledger PostgreSQL pool fixed at **48 connections**, while preserving the
current accounting, consistency, and idempotency guarantees.

This plan applies to the distributed-user workload. A hot-account workload is
a different concurrency problem because updates to one balance row must
serialize; it is not an acceptance test for this target.

### Acceptance criteria

A candidate passes the initial capacity gate only if all of the following hold
for at least three independent 60-second runs on freshly reset and seeded data:

| Measure | Required result |
|---|---:|
| Offered rate | 10,000 requests/s |
| Completed rate | >= 9,990 requests/s (99.9% of offered load) |
| Dropped k6 iterations | 0 |
| Accepted started requests | >= 99.99% |
| HTTP latency | p95 < 50 ms and p99 < 100 ms |
| Ledger reservation latency | p95 < 20 ms; stretch goal < 10 ms |
| Pool size | exactly 48 |
| Pool acquisition | p95 < 1 ms; no canceled acquires |
| Database correctness | no negative balance, duplicate reservation, unbalanced journal, or missing outbox event |

Report both the offered and completed rates. A percentile pass is not a
capacity pass if k6 drops scheduled arrivals.

## Current baseline and constraint

The 2026-09-11 pool experiment in `orderservice/README.md` found pool 48 to be
the best local setting:

| Measure | Current pool-48 result |
|---|---:|
| Approximate accepted rate | 9,984/s |
| Average Ledger reservation | 2.97 ms |
| Peak acquired connections | 48 |
| Empty acquisition attempts | 62,668 in 60 seconds |
| Median repeated-run HTTP p95 | 39.96 ms |
| Median repeated-run reservation p95 | 33.79 ms |
| Median repeated-run dropped iterations | 1,105 in 60 seconds |

This is close to the target but not yet a strict 10,000/s pass. Pool 64 did not
produce a material improvement, so the next gain should come from reducing
work and connection hold time rather than adding connections.

Little's Law gives a useful hard budget. At 10,000 reservations/s and 48 active
connections, average database-connection occupancy must remain below
approximately **4.8 ms per reservation** (`48 / 10,000`). That is an upper
bound, not a safe operating point. To absorb variance, checkpoints, and
background work, target <= 3.5 ms average occupancy and <= 1 ms pool wait.

## Why the current reservation path is expensive

`ReserveForOrder` currently performs roughly twelve client/database protocol
steps for a new reservation:

1. begin transaction;
2. insert the user/asset account with `ON CONFLICT DO NOTHING`;
3. select that account ID;
4. insert the balance projection with `ON CONFLICT DO NOTHING`;
5. insert the idempotency journal;
6. update the balance;
7. insert the reservation;
8. insert two ledger entries separately;
9. insert the outbox event;
10. select the reservation and balance version;
11. commit.

Each operation is individually small, but protocol round trips, repeated index
lookups, trigger queries, and WAL records accumulate while a scarce connection
is held. The two entry inserts also invoke the dimension-validation trigger
twice; that trigger performs additional lookups for every row.

## Optimization sequence

Change one variable at a time, retain the previous result, and stop adding
complexity once the acceptance criteria pass. The order below maximizes likely
gain while keeping correctness review manageable.

### Phase 0: establish a trustworthy profile

Status: implemented by `make profile-phase0`. The command fixes the Ledger
pool at 48, resets and seeds each run, resets PostgreSQL statistics at the
measurement boundary, executes three strict 60-second measurements, validates
ledger integrity, and writes a median/range report under
`orderservice/loadtest/results/`.

Before changing SQL:

- Add a dedicated benchmark command that always sets `RATE=10000`,
  `LEDGER_DB_MAX_CONNS=48`, identical VU limits, a unique run ID, no warm-up,
  and a 60-second test duration.
- Reset and reseed 10,000 users before every measured run. Do not include reset
  or seed time in results.
- Capture k6 output and the existing service, pool, PostgreSQL, container, and
  host metrics. Keep observability settings identical between comparisons.
- Enable `pg_stat_statements` and collect calls, total/mean execution time,
  rows, WAL bytes, shared-block hits/reads, and temporary writes for the
  reservation statements.
- Sample `pg_stat_activity` waits and, in a separate diagnostic run, use
  `EXPLAIN (ANALYZE, BUFFERS, WAL, SETTINGS)` on representative statements.
  Do not run `EXPLAIN ANALYZE` on the production load path.
- Record database settings that affect the comparison, especially PostgreSQL
  version, CPU/memory limits, storage, `shared_buffers`, `max_connections`,
  checkpoint/WAL settings, and synchronous commit policy.

Deliverable: a baseline report with three runs, medians, ranges, database
connection occupancy, pool-wait time, statement costs, WAL per accepted order,
and CPU saturation. This distinguishes SQL latency from k6 or host saturation.

Run the default profile from the repository root:

```bash
make profile-phase0
```

For a short workflow smoke test (not a capacity result):

```bash
make profile-phase0 PROFILE_RUNS=1 PROFILE_DURATION=10s \
  PROFILE_COOLDOWN_SECONDS=0
```

The longer endurance profile is intentionally retained for later use. It
changes database state substantially during warm-up and answers a different
question from the fresh-data 60-second capacity baseline:

```bash
make profile-phase0 PROFILE_WARMUP_DURATION=2m PROFILE_DURATION=10m
```

### Phase 1: remove avoidable work from the synchronous path

This is the safest, highest-priority change.

Status: implemented and measured on 2026-09-12. All three pool-48 runs at an
offered 10,000 requests/s completed with zero dropped iterations, zero request
failures, and passing integrity checks. Median completed throughput was
9,997.91/s, HTTP p95 was 14.09 ms, and reservation p95 was 7.35 ms. This passes
the Phase 1 latency and reliability gates; the measured rate also exceeds the
9,990/s acceptance threshold.

#### Measured Phase 1 improvement

The end-to-end comparison below uses the three-run medians from the Phase 0
and Phase 1 profiles. A lower value is better for every measure except
completed throughput.

| Metric | Phase 0 | Phase 1 | Improvement / saved |
|---|---:|---:|---:|
| Completed throughput | 9,982.27/s | 9,997.91/s | 0.16% higher |
| HTTP average | 14.93 ms | 7.89 ms | 47.1% lower |
| HTTP p95 | 56.70 ms | 14.09 ms | 75.1% lower |
| HTTP p99 | 94.94 ms | 24.11 ms | 74.6% lower |
| Reservation average | 9.51 ms | 2.73 ms | 71.3% lower |
| Reservation p95 | 49.07 ms | 7.35 ms | 85.0% lower |
| Reservation p99 | 87.70 ms | 15.65 ms | 82.2% lower |
| Dropped iterations | 0 | 0 | no change |
| Failure rate | 0% | 0% | no change |

The database comparison uses run 2 from each phase because both runs started
all scheduled iterations without request failures and avoid the anomalous
Phase 0 run 1. `pg_stat_statements` was reset immediately before each measured
interval and ran with `track=all` in both tests.

| Database measure | Phase 0 run 2 | Phase 1 run 2 | Saved |
|---|---:|---:|---:|
| SQL statement executions | 15.60 million | 13.20 million | 15.4% |
| Total SQL execution time | 774.4 s | 477.1 s | 38.4% |
| Ledger-entry insert calls | 1.20 million | 600,000 | 50.0% |
| Ledger-entry insertion time | 301.6 s | 186.2 s | 38.3% |
| Empty pool acquisitions | 160,681 | 48,539 | 69.8% |
| Total pool acquisition wait | 2,175 s | 358 s | 83.5% |
| Statement-attributed WAL | 7.03 GB | 6.76 GB | 3.9% |

The four removed commands per successful reservation account for 2.4 million
fewer executions over 600,000 orders: the account conflict insert, balance
conflict insert, second individual ledger-entry insert, and final reservation
select. Ledger-entry batching alone halved entry-insert calls and reduced their
total SQL execution time by 38.3% while inserting the same 1.2 million rows.

The latency reduction is larger than the raw statement-count reduction because
the pool was operating near its saturation knee. Shorter transactions released
connections sooner, which reduced empty acquisitions by 69.8% and aggregate
pool wait by 83.5%; the smaller queue then reduced latency for subsequent
requests. WAL declined only 3.9% because Phase 1 preserves the durable journal,
reservation, balance update, two ledger entries, and outbox event.

The detailed local artifacts are retained in:

```text
orderservice/loadtest/results/phase0-pool-48-rate-10000-20260912T183533Z/
orderservice/loadtest/results/phase1-pool-48-rate-10000-20260912/
```

1. **Do not provision accounts during normal order admission.** The load seed
   already guarantees that users and balances exist. Make account creation an
   explicit onboarding/deposit operation. Reservation should select the
   existing account once and return `not found` if it is absent. This removes
   two writes and one lookup from every request, including their conflict-index
   checks and WAL/locking overhead.
2. **Return the response from writes.** Use `UPDATE ... RETURNING version` and
   retain the known reservation values in Go, or use the reservation insert's
   `RETURNING` clause. Remove the final join/select in `reservationTx` from the
   successful new-order path.
3. **Insert both journal entries in one statement.** Use a multi-row `VALUES`
   insert. This saves a protocol step even before broader CTE consolidation.
4. **Keep metadata serialization outside the acquired-connection window** where
   possible. Generate deterministic IDs and marshal the outbox payload before
   `BeginTx`.
5. **Bound transaction and lock waits.** Phase 1 continues to use the incoming
   RPC context, which already bounds connection acquisition and statement
   execution without adding a `SET LOCAL` round trip. Evaluate server-side
   `statement_timeout` and `lock_timeout` as connection settings before a
   production rollout, where protection from orphaned client requests may
   justify their broader behavioral impact.

Expected new-reservation path: begin, account lookup, journal/idempotency write,
balance update, reservation insert, two-entry batch insert, outbox insert,
commit. This phase alone should remove four or more protocol/database steps.

Gate: rerun the full matrix. Continue only if statement counts and connection
occupancy fall without changing replay or insufficient-funds behavior.

### Phase 2: consolidate the reservation into one database call

Status: the PostgreSQL-function option was implemented and measured on
2026-09-12. It keeps request validation, ID generation, payload serialization,
context cancellation, outcome mapping, and metrics in Go while moving the
atomic database operation behind `reserve_for_order(...) RETURNS TABLE`.
Explicit outcomes cover reserved, replay, conflict, missing-account, and
insufficient-funds cases. Concurrent tests also verify one mutation plus
idempotent replays when 20 identical commands arrive simultaneously.

The primary three-run comparison produced the following medians:

| Metric | Phase 1 | Phase 2 function | Improvement / saved |
|---|---:|---:|---:|
| Completed throughput | 9,997.91/s | 9,998.65/s | 0.01% higher |
| HTTP average | 7.89 ms | 6.98 ms | 11.5% lower |
| HTTP p95 | 14.09 ms | 11.70 ms | 17.0% lower |
| HTTP p99 | 24.11 ms | 20.78 ms | 13.8% lower |
| Reservation average | 2.73 ms | 1.90 ms | 30.5% lower |
| Reservation p95 | 7.35 ms | 4.76 ms | 35.2% lower |
| Reservation p99 | 15.65 ms | 12.19 ms | 22.1% lower |
| Empty pool acquisitions, run 2 | 48,539 | 26,851 | 44.7% lower |
| Aggregate pool wait, run 2 | 358.0 s | 178.1 s | 50.2% lower |

Phase 2 run 1 dropped 551 scheduled arrivals despite remaining within the HTTP
latency thresholds. Runs 2 and 3 and an additional clean confirmation run all
completed approximately 600,000 requests with zero drops, zero failures, and
passing integrity checks. The confirmation recorded 9,998.47/s, HTTP p95 of
11.68 ms, and reservation p95 of 4.79 ms.

Do not sum all `pg_stat_statements` execution time or WAL for this comparison:
with `track=all`, PostgreSQL attributes work to both the outer function call
and its nested statements. The non-double-counted `pg_stat_wal` database
counter for equivalent run 2 fell from approximately 6.55 GB in Phase 1 to
6.03 GB in Phase 2, a 7.9% reduction.

The CTE alternative was not retained as a second implementation. The stored
function already passes the target, expresses the required outcome branching
more clearly, and the plan's governing rule is to stop adding complexity once
the acceptance criteria pass. The detailed artifacts are retained in:

```text
orderservice/loadtest/results/phase2-function-pool-48-rate-10000-20260912/
orderservice/loadtest/results/phase2-function-confirmation-20260912/
```

Two implementations were considered for executing the reservation in one
application/database call:

1. a parameterized statement using data-modifying CTEs; and
2. a PostgreSQL function, such as `reserve_for_order(...) RETURNS TABLE`, that
   performs the operation and returns the reservation result.

Prefer a function returning a row over a `CALL`-style stored procedure. The
caller needs a structured reservation result, and the operation does not need
to manage transaction boundaries independently. When invoked as one statement
without an application-managed transaction, the function call is already
atomic.

#### Option A: data-modifying CTE

Replace the successful reservation mutations with one parameterized statement.
The intended shape is:

```sql
WITH account AS (
    SELECT u.id
    FROM user_asset_accounts AS u
    WHERE u.user_id = $1 AND u.asset_id = $2 AND u.status = 'active'
),
journal AS (
    INSERT INTO ledger_journals (...)
    SELECT ...
    ON CONFLICT (source_system, source_event_id) DO NOTHING
    RETURNING id
),
balance AS (
    UPDATE user_asset_balances AS b
    SET available_atomic = available_atomic - $3::numeric,
        reserved_atomic = reserved_atomic + $3::numeric,
        version = version + 1,
        updated_at = now()
    FROM account, journal
    WHERE b.user_asset_account_id = account.id
      AND b.available_atomic >= $3::numeric
    RETURNING b.user_asset_account_id, b.version
),
reservation AS (
    INSERT INTO fund_reservations (...)
    SELECT ... FROM balance
    RETURNING id, order_id, status, original_atomic, remaining_atomic
),
entries AS (
    INSERT INTO ledger_entries (...)
    SELECT ... FROM reservation, balance
    UNION ALL
    SELECT ... FROM reservation, balance
    RETURNING id
),
event AS (
    INSERT INTO outbox_events (...)
    SELECT ... FROM reservation
    RETURNING id
)
SELECT reservation.*, balance.version
FROM reservation, balance, event;
```

The production statement must distinguish four outcomes explicitly:

- newly reserved;
- idempotent replay with the same order and operation semantics;
- idempotency-key/order conflict;
- missing account or insufficient funds.

Do not infer those outcomes solely from an empty result. If a single statement
makes that logic opaque, use two prepared statements: a fast replay/conflict
check followed by one atomic mutation statement. Correctness is more important
than achieving a nominal one-round-trip design.

CTEs reduce network/protocol overhead; they do not make row-lock contention
disappear. PostgreSQL may materialize data-modifying CTE results, so validate
the complete plan and WAL cost rather than assuming fewer statements are
automatically faster.

#### Option B: PostgreSQL function

Expose a versioned database function with a typed contract similar to:

```sql
SELECT *
FROM reserve_for_order(
    user_id => $1,
    asset_id => $2,
    amount_atomic => $3,
    order_id => $4,
    command_id => $5,
    correlation_id => $6,
    causation_id => $7,
    occurred_at => $8
);
```

The function should perform the existing-account lookup, journal idempotency
check, conditional balance update, reservation insert, two-row ledger-entry
insert, and outbox insert, then return a typed outcome and reservation fields.
It must distinguish newly reserved, replay, conflict, missing-account, and
insufficient-funds outcomes without relying on exception-message parsing in
Go.

The stored function can make branching clearer than one large CTE while still
reducing the application/database exchange to one statement. It does not
remove trigger execution, foreign-key checks, index maintenance, balance-row
contention, or WAL generation. Those costs must remain visible in the profile.

Manage the function through a checksum-protected schema migration. Keep the Go
adapter responsible for request validation, context cancellation, domain-error
mapping, and metrics. Avoid `SECURITY DEFINER` unless it is required and its
search path and privileges are explicitly hardened.

#### Selection gate

Benchmark Phase 1 and the selected one-call implementation under identical
pool-48 conditions. Implement the CTE alternative as a fallback only if the
stored function does not satisfy the gate. Compare:

- reservation and HTTP p50/p95/p99;
- mean connection occupancy and pool acquisition wait;
- calls and execution time reported by `pg_stat_statements`;
- database CPU, buffers, locks, and WAL per accepted order;
- migration and rollback complexity; and
- replay, conflict, insufficient-funds, cancellation, and ledger-integrity
  behavior.

Select the simpler implementation when performance is materially equivalent.
Do not retain both production paths after the evaluation, except temporarily
behind a benchmark or rollout flag.

Gate: target <= 3.5 ms mean connection occupancy, reservation p95 < 20 ms,
zero pool-acquire cancellations, and no regression in database CPU or WAL per
order.

### Phase 3: prepared statements and statement-shape cleanup

Status: implemented and measured on 2026-09-12. The Ledger now explicitly
configures pgx `cache_statement` mode with a bounded 128-entry cache per
connection, validates alternative execution modes at startup, exposes both
settings through environment configuration, logs the effective values, and
uses one canonical SQL string for the reservation function call. All three
pool-48 runs completed approximately 600,000 requests with zero drops, zero
failures, and passing integrity checks.

| Metric | Phase 2 function | Phase 3 | Improvement / saved |
|---|---:|---:|---:|
| Completed throughput | 9,998.65/s | 9,998.56/s | materially unchanged |
| HTTP average | 6.98 ms | 6.88 ms | 1.6% lower |
| HTTP p95 | 11.70 ms | 11.39 ms | 2.7% lower |
| HTTP p99 | 20.78 ms | 19.13 ms | 7.9% lower |
| Reservation average | 1.90 ms | 1.83 ms | 3.8% lower |
| Reservation p95 | 4.76 ms | 4.50 ms | 5.4% lower |
| Reservation p99 | 12.19 ms | 10.34 ms | 15.2% lower |
| Empty pool acquisitions, run 2 | 26,851 | 25,610 | 4.6% lower |
| Aggregate pool wait, run 2 | 178.1 s | 140.9 s | 20.9% lower |

These small changes should not be interpreted as proof that enabling the
statement cache caused the entire improvement: pgx already defaults to
`cache_statement`, so Phase 2 was normally receiving the same behavior. Phase
3 makes that performance assumption explicit, testable, observable, and less
vulnerable to a connection-string or deployment change. The strongest result
is repeatability: all three Phase 3 runs passed with narrow latency ranges.

The explicit casts in `reserveForOrderSQL` remain intentional. They make the
stored-function signature deterministic and prevent resolution changes if an
overload is added later. The hot path contains no dynamic SQL variants, and it
continues to use PostgreSQL's default `READ COMMITTED` behavior. A deployment
through an external transaction-mode pooler must validate prepared-statement
support and benchmark a compatible mode such as `cache_describe` before
changing the default.

Detailed artifacts are retained in:

```text
orderservice/loadtest/results/phase3-prepared-pool-48-rate-10000-20260912/
```

- Keep all SQL parameterized with stable text. pgx can then reuse its statement
  cache instead of repeatedly parsing many statement variants.
- Confirm cache hits and server prepared-statement behavior under the deployed
  connection/pooler topology. Transaction-mode external poolers may require a
  different pgx configuration.
- Avoid dynamic SQL in the hot reservation path. The current dynamic balance
  column selection is outside `ReserveForOrder`, but future batching should not
  introduce per-request SQL variants.
- Remove casts that are proven redundant by typed parameters. Retain exact
  `NUMERIC(78,0)` semantics unless a separately reviewed domain migration shows
  every supported asset amount fits in `BIGINT`; changing the amount type is a
  financial-data decision, not a routine performance tweak.
- Keep transactions at the default `READ COMMITTED` isolation unless testing
  demonstrates a correctness need for stronger isolation.

### Phase 4: index validation, not indiscriminate index addition

The important point lookups already have suitable unique indexes:

| Access path | Existing supporting constraint/index |
|---|---|
| account by `(user_id, asset_id)` | `user_asset_accounts` unique constraint |
| journal idempotency by `(source_system, source_event_id)` | `ledger_journals` unique constraint |
| reservation by `order_id` | `fund_reservations` unique constraint |
| balance update by account ID | `user_asset_balances` primary key |

Additional indexes increase WAL, cache pressure, and insert cost, so add one
only after a captured plan shows a missing access path. Specific candidates to
evaluate are:

- `user_asset_accounts (user_id, asset_id) INCLUDE (id, status)` if heap fetches
  for the account lookup are material and visibility-map conditions make an
  index-only scan realistic;
- a narrower outbox pending index only if relay work measurably interferes with
  admission; and
- removal of redundant indexes only after checking constraint backing indexes,
  production query traffic, and foreign-key maintenance needs.

After large reset/reseed cycles, run `ANALYZE` and verify autovacuum keeps up.
Monitor dead tuples and index/table growth during longer tests. Never create or
drop a production index without measuring write cost and using an online-safe
migration procedure.

### Phase 5: batching and aggregation

Batching is useful only at boundaries where waiting for a batch does not break
the synchronous latency objective.

**Use immediately:**

- multi-row insert for the two ledger entries;
- batched outbox relay claim, Kafka publish, and acknowledgement;
- batched/asynchronous metrics aggregation rather than per-request external
  writes; and
- batch maintenance operations such as seed, archive, and reconciliation.

**Evaluate after Phases 1-4:**

- micro-batch reservations by draining up to 16-64 queued commands or waiting
  at most 100-250 microseconds, then send a set-based statement using arrays or
  `unnest`;
- partition each batch by user/account key and preserve per-key order;
- return one independent result per command, including replay, conflict, and
  insufficient-funds outcomes; and
- cap the queue and apply backpressure rather than allowing unbounded latency.

A batch must not combine unrelated reservations into one all-or-nothing
transaction: one invalid order cannot roll back valid orders. Use per-row
outcome logic or smaller independent transactions. Benchmark at low and high
load because batching can improve throughput while worsening light-load tail
latency.

**Do not put Kafka acknowledgement on the reservation critical path.** The
transactional outbox is already the correct durability boundary; relay
publication can remain asynchronous.

### Phase 6: reduce trigger and WAL overhead only with equivalent safeguards

The `ledger_entries_validate` row trigger performs dimension queries for every
entry. Profile it after consolidating statements. If it remains material:

- replace repeated lookups with a set-based validation path for the two rows;
- consider caching immutable ledger-account dimensions in the application only
  if database constraints still prevent invalid persisted data; or
- use a narrowly designed statement-level trigger with transition tables.

Do not simply disable the trigger. Any replacement must retain asset, account
type, account purpose/bucket, account status, and custody validation.

Tune checkpoint/WAL settings only after measuring `wal_bytes`, full-page
images, buffer-full events, checkpoint duration, and storage latency. Keep
durability settings such as `synchronous_commit` unchanged for financial
writes unless the business explicitly accepts the corresponding data-loss
window.

## Test matrix

Run each viable candidate at 1k, 4k, 8k, 10k, and 11k requests/s. The 11k run
is a headroom test, not part of the primary acceptance gate. Use 60-second
runs without a warm-up for the initial like-for-like capacity comparison.

For each rate:

1. reset and reseed the same 10,000-user dataset;
2. reset PostgreSQL measurement statistics;
3. measure for 60 seconds;
4. cool down sufficiently to avoid overlapping checkpoint or Kafka backlog;
5. repeat three times in randomized baseline/candidate order; and
6. run integrity checks after every test.

After a candidate passes the capacity gate, separately run the retained
two-minute warm-up plus ten-minute measurement as an endurance test.

Capture at minimum:

- offered, started, completed, accepted, failed, and dropped rates;
- HTTP and per-stage p50/p95/p99/max latency;
- pool acquired/idle, empty/canceled acquires, and acquisition duration;
- database CPU, I/O, locks, waits, commits/rollbacks, WAL and checkpoints;
- per-statement calls, latency, rows, buffers, and WAL;
- Kafka publish latency and backlog; and
- host CPU throttling, memory/swap, network, and load-generator saturation.

Integrity checks must confirm:

- every successful order has exactly one reservation, journal, expected pair of
  ledger entries, and outbox event;
- replaying the same command does not mutate balances twice;
- reusing an idempotency key with conflicting semantics is rejected;
- the sum of balance deltas matches the accepted reservation total;
- no balance is negative; and
- debit and credit totals balance per journal and asset.

## Delivery plan and decision gates

| Stage | Change | Expected value | Exit decision |
|---|---|---|---|
| 0 | Reproducible profile | Identifies actual bottleneck | Baseline variance understood |
| 1 | Remove account provisioning/final read; batch entries | Large reduction in synchronous work | Keep if latency and WAL improve |
| 2 | One CTE statement vs. stored function | Fewer round trips and shorter connection hold | Select the simpler measured winner |
| 3 | Prepared/stable statements | Lower parse/protocol overhead | Verify with statement statistics |
| 4 | Evidence-based index changes | Lower lookup cost where demonstrated | Reject indexes that increase total cost |
| 5 | Bounded micro-batching | Headroom if single-request SQL is insufficient | Keep only with latency and isolation guarantees |
| 6 | Trigger/WAL tuning | Remove remaining database overhead | Requires equivalent integrity/durability |

Promote a change only when its three-run median improves and its worst run does
not materially regress. Roll out behind a feature flag or separate code path,
canary it, and retain the old reservation implementation until production
correctness and latency are stable.

## Recommended first implementation slice

Implement Phase 1 as one reviewable change:

1. replace `ensureUser` in `ReserveForOrder` with a read-only existing-account
   lookup;
2. make the conditional balance update return the new version;
3. insert both ledger entries with one multi-row statement;
4. build the response without the final reservation join; and
5. add replay, conflict, insufficient-funds, concurrent same-account, and
   accounting-invariant tests.

Then run the full pool-48 test matrix. Its profile will show whether the CTE or
micro-batching phases are necessary. This avoids committing to a complex batch
architecture before the simpler removal of four hot-path operations has been
measured.
