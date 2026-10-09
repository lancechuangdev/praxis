# Phase 0 baseline

## Status

Phase 0 records a reproducible local baseline for the current synchronous Order
to Ledger and Matching path. It is not an AWS, EKS, multi-zone, or production
capacity claim. Production-like infrastructure measurement remains the first
gate in Phase 1 of the [implementation roadmap](implementation-roadmap.md).

## Reproduction

From the repository root, run:

```bash
make profile-phase0
```

The profile runs three isolated 60-second tests by default. Each run resets and
seeds PostgreSQL, fixes the Ledger writer pool at 48 connections, sends a
constant-arrival-rate distributed-account workload, captures application,
PostgreSQL, host, and container measurements, and runs accounting-integrity
queries. The complete procedure and environment overrides are documented in
the [Order Service README](../orderservice/README.md#reproducible-pool-48-profile).

Do not compare two results unless workload, duration, host resources, service
images, database settings, pool sizes, and observability configuration are
recorded. Retain the git commit and dirty-file count with every run.

## Recorded local result

The retained result is
`orderservice/loadtest/results/phase3-prepared-pool-48-rate-10000-20260912`.
It was captured on September 12, 2026 from commit
`5e9bacec49f8833b0b18836d8c06ae8ddeb839ff` with six dirty files, so it is a
repeatable engineering reference rather than a release certification.

Workload and environment:

| Property | Value |
| --- | ---: |
| Runs | 3 |
| Duration per run | 60 seconds |
| Offered rate | 10,000 iterations/second |
| Seeded users | 10,000 |
| Ledger writer pool | 48 connections |
| Logical CPUs | 32 |
| Host memory | approximately 62.5 GiB |

Median results, with the minimum and maximum across the three runs:

| Metric | Median | Range |
| --- | ---: | ---: |
| Completed requests/second | 9,998.56 | 9,997.32-9,998.63 |
| Dropped iterations | 0 | 0-0 |
| Failure rate | 0 | 0-0 |
| HTTP average | 6.88 ms | 6.83-6.89 ms |
| HTTP p95 | 11.39 ms | 11.30-11.52 ms |
| HTTP p99 | 19.13 ms | 17.89-19.17 ms |
| Reservation average | 1.83 ms | 1.79-1.86 ms |
| Reservation p95 | 4.50 ms | 4.42-4.64 ms |
| Reservation p99 | 10.34 ms | 9.58-10.39 ms |

All three retained runs report `PASS` for the accounting-integrity query. The
generated report and per-run evidence remain in the result directory, including
the k6 summaries, environment, PostgreSQL statistics, monitor samples, and
integrity status.

## Baseline coverage

| Required signal | Local evidence | Production-like evidence |
| --- | --- | --- |
| Latency and throughput | Recorded above | Pending Phase 1 |
| HTTP failure and dropped-arrival rate | Recorded above | Pending Phase 1 |
| Database pool, statement, WAL, host, and container samples | Retained per run | Pending Phase 1 |
| Financial integrity | Three passing post-run checks | Restore and reconciliation proof pending |
| Kafka consumer lag and outbox age | Instrumentation exists; not exercised by this synchronous profile | Pending Phase 1 |
| Per-pod distribution and zone behavior | Not applicable to local Compose | Pending Phase 1 |

The local baseline is sufficient to compare code changes on the same controlled
workload. It is not sufficient to set production rate limits, autoscaling
targets, connection budgets, or service-level objectives.

## Change acceptance

For a local before-and-after comparison:

1. use the same workload and recorded environment;
2. require all integrity checks to pass;
3. treat any nonzero business failure as a regression unless the experiment
   intentionally injects failure;
4. report dropped iterations as lost offered load rather than omitting them;
5. compare medians and ranges across at least three runs; and
6. investigate material tail-latency, database-wait, resource, or WAL growth
   even when average throughput is unchanged.

Phase 1 must replace local capacity assumptions with measurements from the
deployed topology before production thresholds are selected.
