# Order and Ledger caching

This document describes a proposed caching strategy for the Praxis Order and Ledger services. It separates read acceleration from authoritative financial decisions. The projections and HTTP caching behavior described here are target architecture unless explicitly identified as current behavior.

For the broader Ledger scaling plan, including the proposed DynamoDB balance projection, see [Ledger scaling and Market-Clearing Ledger Sharding](ledger-scaling-and-mcls.md).

## Goals and invariants

Caching should:

- reduce latency for repeated reads;
- reduce PostgreSQL reader load and backend CPU;
- reduce response transfer when a representation has not changed;
- preserve tenant isolation and the privacy of financial data;
- make staleness visible and bounded;
- never weaken Ledger correctness.

The following invariants take priority over cache hit rate:

- Cached balances never authorize a reservation, withdrawal, or trade.
- Cached order state never determines whether a mutation is safe.
- Idempotency checks use authoritative durable state.
- A stale projection cannot overwrite or replace a newer observed version.
- Private financial responses are never stored in a shared public cache.

## Current read paths

The Order service currently exposes order mutations but no order query endpoint:

```text
POST /v1/orders
POST /v1/orders/{order_id}/execution-complete
```

The Ledger exposes these relevant reads:

```text
GET /v1/balances/{user}/{asset}
GET /v1/reservations/{order}
```

Ledger balance and reservation reads currently use the PostgreSQL reader pool. Reservations, trades, cancellations, idempotency checks, and read-after-write operations remain writer concerns.

Reader replicas are the first level of read scaling, but they can lag and remain part of the Ledger database's capacity envelope. A separate projection becomes useful when display traffic materially loads those replicas.

## Cache candidates

### Current balance display

Wallet and portfolio displays are good candidates for a versioned balance projection:

```text
Ledger writer transaction
        |
        v
Transactional outbox
        |
        v
Kafka
        |
        v
DynamoDB balance projection
        |
        v
Balance GET and WebSocket snapshot
```

Store one item per user and asset:

```text
PK: USER#<user-id>
SK: ASSET#<asset-id>

available_atomic
reserved_atomic
balance_version
updated_at
```

The projector applies an event only when its version is newer than the stored version:

```text
incoming balance_version > stored balance_version  -> update
incoming balance_version <= stored balance_version -> ignore
```

This makes duplicate and out-of-order event delivery safe. The projection serves display reads only. `ReserveForOrder` must always use the authoritative Ledger writer balance.

### Order and reservation status

If Praxis adds endpoints such as these, they can use an event-driven order projection:

```text
GET /v1/orders/{order_id}
GET /v1/orders?status=open
```

The projection can combine committed Order, Matching, and Ledger facts into a customer-facing view. Active state should have explicit version and freshness information. Completed, rejected, and cancelled orders are better cache candidates because their terminal state no longer changes.

Reservation status is mutable while an order is active, so a cached reservation must not be used to release or settle funds. The Ledger writer remains authoritative for mutations.

### Reference and public data

Assets, symbols, precision rules, public fee schedules, and maintenance notices change infrequently and are strong candidates for application, API-gateway, browser, or CDN caching.

Public market data may also use edge caching when its freshness target allows it. It should have a separate policy from private Order and Ledger data.

## Cache layers

Use the smallest number of layers that meet measured requirements:

| Layer | Appropriate Praxis data | Main concern |
|---|---|---|
| Browser | Private balance or order representations with revalidation | Sensitive data retained on the device |
| CDN | Public reference and market data | Never cache private responses publicly |
| API gateway | Public GET responses | Correct cache key and authorization behavior |
| DynamoDB | Durable current-balance projection | Eventual consistency and projection lag |
| Redis | Short-lived shared views when measurements justify it | Another availability and invalidation dependency |
| Application memory | Small immutable configuration or reference data | Per-replica inconsistency and unbounded memory |

Do not introduce Redis merely because caching is desired. The proposed DynamoDB projection already provides a durable, scalable current-balance view. Redis is appropriate only for a distinct requirement such as extremely low-latency ephemeral data that DynamoDB, PostgreSQL replicas, or client revalidation cannot satisfy.

## HTTP cache policy

### Private financial data

For a private response that may be retained locally but must be revalidated before reuse:

```http
Cache-Control: private, no-cache
```

`private` prevents compliant shared caches from storing the response. `no-cache` permits storage but requires revalidation before reuse.

When a response must not be stored at all, use:

```http
Cache-Control: no-store
```

Use `no-store` deliberately because it also prevents useful client-side conditional caching. Authentication alone does not automatically prevent shared caching; the response must set an explicit policy.

Responses should also use `Vary` when representation content legitimately varies by a request header. Avoid using an authorization token itself as a general-purpose shared-cache key.

### Public reference data

Public, slowly changing data can use a bounded freshness window:

```http
Cache-Control: public, max-age=60, stale-while-revalidate=30
```

Actual durations should follow the data's freshness requirement rather than a single service-wide default.

## ETags and conditional requests

The Ledger already exposes a monotonically increasing balance version. It can form the basis of a strong representation ETag:

```http
HTTP/1.1 200 OK
Cache-Control: private, no-cache
ETag: "balance-alice-usdt-v123"
```

The client later sends:

```http
If-None-Match: "balance-alice-usdt-v123"
```

If the representation is unchanged, the server returns:

```http
HTTP/1.1 304 Not Modified
ETag: "balance-alice-usdt-v123"
```

An ETag must identify the complete representation, including any fields affected by API version, permissions, or query parameters. Do not let two users share an ETag-backed private representation accidentally.

ETags reduce serialization and response transfer. They reduce database work only when the server can validate the version from a cheaper projection or cache; querying PostgreSQL for the version still performs a database read.

## Read-after-write consistency

Projection and replica lag can produce this sequence:

```text
POST /v1/orders commits Ledger balance version 123
                         |
                         v
Immediate GET sees projection version 122
```

Praxis should not move a client backward after it has observed a newer committed version. Use the committed mutation response as the immediate source of truth. The existing order-admission response includes the Ledger reservation, so clients should not issue an immediate GET for the same state.

As read APIs evolve, one or more of these mechanisms may be used:

- include version numbers in mutation and query responses;
- let clients ignore a response older than their highest observed version;
- accept a minimum expected version on a read;
- route an explicit strongly consistent read to the writer;
- wait for a projection to reach the requested version within a short deadline;
- return projection time and freshness metadata.

Strong reads should be explicit and limited because routing every read to the writer defeats read scaling.

## Invalidation and projection updates

Prefer event-driven projection updates over a cache-aside design that depends on best-effort deletion:

```text
Database commit -> transactional outbox -> Kafka -> projection update
```

The outbox makes publication retryable after a process crash. Consumers must be idempotent and version-aware. A TTL can bound storage and staleness, but it is not the primary correctness mechanism.

The following sequence is unsafe without a durable recovery path:

```text
1. Commit database change.
2. Attempt to delete cache entry.
3. Process crashes before deletion.
4. Stale entry remains available.
```

When using cache-aside for nonfinancial reference data, protect against cache stampedes with request coalescing, bounded concurrency, TTL jitter, or stale-while-revalidate. Negative caching of missing records should use a short TTL so newly created data does not remain hidden.

## Optimistic concurrency

Conditional writes and caching solve different problems but can use the same version value. A future order-replacement endpoint could return:

```http
ETag: "order-123-v10"
```

The client submits an update with:

```http
If-Match: "order-123-v10"
```

The service then performs a conditional update:

```sql
UPDATE orders
SET price = $1,
    version = version + 1
WHERE order_id = $2
  AND version = 10;
```

If no row is updated, the client's state is stale. The API can return `412 Precondition Failed` or a documented `409 Conflict`.

Clients must not update Ledger balances directly through optimistic locking. Balance changes remain domain operations enforced by Ledger transactions.

## Data that must remain authoritative

Do not use cached or projected data to:

- authorize `ReserveForOrder`;
- determine whether funds are sufficient;
- book or settle a trade;
- release a reservation;
- perform an idempotency check;
- enforce exact position or open-order limits;
- determine a mutation's outcome after a timeout;
- run financial reconciliation.

These operations use the authoritative writer and committed journal state. A cache may help present their results afterward.

## Security and isolation

- Authorize every private cache read; possession of an order or user identifier is not authorization.
- Include tenant or account identity in private cache keys.
- Do not place credentials, access tokens, or raw personal data in cache keys or logs.
- Encrypt shared cache and projection traffic and storage according to the same data classification as the source.
- Give projections and caches the minimum retention required by their use case.
- Purge or expire derived data according to account-deletion and regulatory retention policy.

## Failure behavior

A read cache or projection failure should not corrupt financial state. Choose behavior per endpoint:

- fall back to a Ledger reader for ordinary reads when capacity permits;
- use an explicit strongly consistent writer read only when the contract requires it;
- return `503 Service Unavailable` rather than presenting unboundedly stale data;
- never route a financial mutation through a cache fallback.

Apply timeouts and circuit breakers so a slow cache does not make the service slower than its database fallback. Limit fallback concurrency to prevent a cache outage from creating a database stampede.

## Observability

Measure:

- hit, miss, revalidation, and eviction rates;
- response latency by data source;
- projection lag in time and versions;
- stale responses and rejected version regressions;
- cache and projection errors;
- fallback volume and database load during cache failure;
- item count, storage size, and hot-key distribution;
- conditional GET `200` and `304` counts.

Avoid account IDs, order IDs, or API keys as metric labels. They create high cardinality and may expose sensitive identifiers. Structured logs can contain appropriately protected identifiers when operational investigation requires them.

## Recommended progression

1. Keep all financial mutations and decisions on the Ledger writer.
2. Continue serving ordinary Ledger reads from PostgreSQL readers.
3. Add version fields and private ETag revalidation to balance and reservation GET responses.
4. Build the DynamoDB current-balance projection when measured read traffic justifies it.
5. Add an event-driven order-status and history projection when Order GET endpoints are introduced.
6. Use CDN or gateway caching only for explicitly public reference and market data.
7. Add Redis only when measurements establish a separate latency or workload need.

The governing rule is:

```text
Cache for presentation and repeated reads.
Use authoritative storage for financial decisions.
```
