# Order-admission optimization plan: 10,000 requests/s with pool size 48

## Objective

Sustain an offered load of **10,000 order-admission requests per second** with
the Ledger PostgreSQL pool fixed at **48 connections**, while preserving the
current accounting, consistency, and idempotency guarantees.

This plan applies to the distributed-user workload. A hot-account workload is
a different concurrency problem because updates to one balance row must
serialize; it is not an acceptance test for this target.

### Acceptance criteria

A candidate passes only if all of the following hold for at least three
independent 10-minute steady-state runs after a 2-minute warm-up:

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
pool at 48, resets and seeds each run, performs a separate warm-up, resets
PostgreSQL statistics at the measurement boundary, executes three strict
10-minute measurements, validates ledger integrity, and writes a median/range
report under `orderservice/loadtest/results/`.

Before changing SQL:

- Add a dedicated benchmark command that always sets `RATE=10000`,
  `LEDGER_DB_MAX_CONNS=48`, identical VU limits, a unique run ID, warm-up, and
  test duration.
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
make profile-phase0 PROFILE_RUNS=1 PROFILE_WARMUP_DURATION=5s \
  PROFILE_DURATION=10s PROFILE_COOLDOWN_SECONDS=0
```

### Phase 1: remove avoidable work from the synchronous path

This is the safest, highest-priority change.

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
5. **Set explicit transaction and lock timeouts** slightly below the Ledger RPC
   deadline. A queued request should fail predictably instead of occupying a
   connection until the caller has already timed out.

Expected new-reservation path: begin, account lookup, journal/idempotency write,
balance update, reservation insert, two-entry batch insert, outbox insert,
commit. This phase alone should remove four or more protocol/database steps.

Gate: rerun the full matrix. Continue only if statement counts and connection
occupancy fall without changing replay or insufficient-funds behavior.

### Phase 2: consolidate the reservation with data-modifying CTEs

Replace the successful reservation mutations with one parameterized statement
inside one transaction. The intended shape is:

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

Gate: target <= 3.5 ms mean connection occupancy, reservation p95 < 20 ms,
zero pool-acquire cancellations, and no regression in database CPU or WAL per
order.

### Phase 3: prepared statements and statement-shape cleanup

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
is a headroom test, not part of the primary acceptance gate.

For each rate:

1. reset and reseed the same 10,000-user dataset;
2. warm up for 2 minutes;
3. measure for 10 minutes;
4. cool down sufficiently to avoid overlapping checkpoint or Kafka backlog;
5. repeat three times in randomized baseline/candidate order; and
6. run integrity checks after every test.

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
| 2 | One CTE mutation statement | Fewer round trips and shorter connection hold | Keep only if plans and tails improve |
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
