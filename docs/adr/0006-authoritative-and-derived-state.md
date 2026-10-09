# ADR 0006: Keep authoritative state separate from derived views

- Status: Accepted
- Date: 2026-10-08

## Context

Praxis plans caches, customer-facing order views, balance projections,
reporting, notifications, reconciliation, and analytical exports. These views
improve read performance and operations, but they can lag, miss an event, or be
rebuilt. Treating one as financial authority would allow stale data to approve
a mutation or hide a discrepancy.

## Decision

Authoritative state is the state committed by its owning service:

- Ledger journals, entries, balances, and reservations are authoritative for
  financial state.
- Matching's fenced market sequence and durable book state are authoritative
  for matching decisions.
- Order's idempotency and saga state are authoritative for admission workflow
  progress.
- Risk's versioned rules and decision record become authoritative when Risk is
  extracted.

Everything built from events, replicas, exports, or caches is derived. Derived
state may serve presentation, search, notification, reporting, anomaly
detection, and stale-tolerant reads. It must not authorize reservations,
withdrawals, matching, settlement, or repair.

Derived records expose source version, sequence, checkpoint, or freshness when
clients need to reason about staleness. Consumers apply updates idempotently and
reject stale versions. A rebuild writes isolated storage, validates against the
authority, catches up, and switches reads atomically.

Reconciliation may identify a discrepancy automatically. Repair requires an
audited, idempotent command to the authoritative owner and human approval until
a specific repair has been proven safe for automation.

## Consequences

- Read-after-write operations may need the owner rather than a replica or cache.
- Projection loss is recoverable if source retention or an authoritative
  snapshot covers the required history.
- Cache and analytics outages do not stop financial mutation paths unless an
  explicit product policy chooses to fail closed.
- Projection freshness, sequence gaps, rebuild progress, and reconciliation
  differences require monitoring.

