# ADR 0002: Give each write model one owning service

- Status: Accepted
- Date: 2026-10-08

## Context

Praxis separates Order, Risk, Ledger, Matching, and asynchronous consumers so
that they can be deployed and scaled independently. Independence is lost when
services update one another's tables, share unrestricted database credentials,
or derive financial decisions from eventually consistent copies.

The Outbox Relay is a special-purpose publisher. It needs narrowly scoped
access to claim and mark outbox rows, but it does not own the domain state that
created those rows.

## Decision

Each write model has exactly one owning service:

| State | Owner | Other access |
| --- | --- | --- |
| Order requests, idempotency records, and sagas | Order | Read through an API or committed events. |
| Risk rules, limits, and decision audit | Risk when extracted; Order until then | Read through the versioned Risk contract or events. |
| Balances, reservations, journals, and entries | Ledger | Read through Ledger APIs, replicas, or derived projections. |
| Market sequence, order book, and matching recovery state | Matching | Read through Matching APIs or committed events. |
| Reporting and customer-facing read models | The projection that builds them | Disposable; never used to authorize a financial mutation. |
| Notification preferences and delivery attempts | Notification | No authority over an order or financial state. |
| Reconciliation runs, findings, and approvals | Reconciliation | Repairs occur only through an owning service's reviewed command. |

A service must not write another service's tables. Cross-service changes use a
versioned synchronous command when the current response needs a decision, or a
durable event/command when it does not, following
[ADR 0001](0001-synchronous-calls-and-kafka.md).

Each service receives its own database role and workload identity. Permissions
are limited to its schema, topics, consumer groups, and required secrets. The
Outbox Relay receives only the access needed to lease, publish, and mark rows
in its paired outbox; it cannot mutate domain tables.

Schema migrations run under a separate migration identity. They do not broaden
runtime permissions.

## Enforcement

- New tables identify an owner in their design and migration review.
- Runtime credentials must not have cross-service write grants.
- Integration tests exercise public contracts rather than direct writes to a
  dependency's schema.
- Reconciliation detects discrepancies but submits reviewed, idempotent repair
  commands to the owner.
- Shadow implementations use isolated storage and suppress external effects.

## Consequences

- Cross-service workflows are sagas rather than distributed transactions.
- Derived reads may be stale and must expose version or freshness information.
- Some operations require new APIs or events instead of convenient SQL access.
- A database outage can affect several schemas physically, but ownership and
  credentials remain logically isolated.

