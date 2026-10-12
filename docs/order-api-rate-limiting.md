# Order API rate limiting

Local concurrency and bounded queue protection are implemented. Local global,
account, and market token buckets run in observe-only mode. Authoritative quota
enforcement remains pending authenticated identity, shared limiter storage, and
production-like capacity measurements.

## Goals

The limiter should:

- prevent one client from consuming disproportionate Order service capacity;
- protect the Order service, Ledger, and Matching Engine from aggregate overload;
- allow short, legitimate bursts while enforcing a sustainable request rate;
- apply consistently across all Order service replicas;
- reject excess traffic before it performs expensive work or creates side effects;
- provide clients and operators with clear feedback.

Rate limiting is not a replacement for authentication, balance checks, price bands, maximum order size, position limits, or open-order limits. Those are separate security and risk controls.

## Layered limits

An order request should pass each applicable layer:

```text
Request
   |
   v
Ingress per-IP limit
   |
   v
Per-account or per-API-key limit
   |
   v
Order endpoint weighted limit
   |
   v
Service-wide global limit
   |
   v
Authentication, validation, risk, Ledger, and Matching
```

### Per-account or per-API-key limit

This is the primary trading limit. Each authenticated account or API key gets its own bucket, so one client exhausting its allowance does not consume another client's allowance.

Use the authenticated account or API-key identifier as the limiter key. Do not use `order_id`, `request_id`, or Ledger `command_id`; a client controls or creates many of those identifiers and could use them to bypass the limit.

Different account tiers may have different capacity and refill rates, but tier changes should be controlled configuration rather than values supplied by the client.

### Per-IP limit

Apply a coarse per-IP limit at the ingress or API gateway. It protects authentication endpoints and reduces obvious abusive traffic before it reaches the service.

It should not be the primary order limit. Multiple legitimate traders may share a corporate NAT address, while one trader may distribute requests across many addresses. Only trust forwarding headers from known proxies, and derive the client address at the trusted edge.

### Global limit

A shared global bucket caps aggregate admission when the Order service or a downstream dependency approaches its safe capacity. This protects the system even when every individual client remains below its own limit.

The global threshold should be based on measured end-to-end capacity, including Ledger reservation and Matching submission throughput, rather than only HTTP handler capacity.

### Market and business limits

Busy markets may need per-symbol protection so traffic for one symbol cannot degrade every market. Separately enforce business controls such as maximum open orders per account and excessive order-to-cancel ratios. These controls can reject a request even when token buckets have capacity.

## Token bucket behavior

Each bucket has:

- a **capacity**, which determines the largest permitted burst;
- a **refill rate**, which determines the sustainable request rate;
- a **cost** for each operation.

For example:

```text
Per API key
  capacity:    100 tokens
  refill rate: 20 tokens per second

Operation costs
  place order:       1 token
  cancel order:      1 token
  replace order:     2 tokens
  batch of N orders: N tokens
```

These values are illustrative, not production recommendations. Load tests and production measurements should determine the actual limits.

For every request, the limiter atomically:

1. Calculates the tokens accumulated since the bucket was last updated.
2. Caps the result at the bucket capacity.
3. Allows the request and deducts its cost when enough tokens remain.
4. Rejects the request when the available balance is too small.

Using fractional tokens or sufficiently precise integer units avoids losing refill capacity through rounding.

## Distributed enforcement

Order service replicas must share limiter state. Independent in-memory buckets would multiply the effective allowance by the number of replicas and make behavior depend on load-balancer routing.

Redis is a common implementation:

```text
Order pod A ---+
Order pod B ---+---> shared Redis token buckets
Order pod C ---+
```

Refill, comparison, and deduction must be one atomic operation, implemented with a Redis function or Lua script. A non-atomic read followed by a write could allow two replicas to consume the same final token.

Use separate, namespaced keys for each layer, for example:

```text
rate:order:account:<account-id>
rate:order:api-key:<api-key-id>
rate:order:symbol:<symbol>
rate:order:global
```

Set expirations on inactive per-client buckets so unused keys do not accumulate indefinitely. The global and per-symbol buckets may remain persistent.

A small local emergency limiter can shed extreme traffic before contacting Redis, but it must not be presented as the authoritative per-account limit.

## Request flow and side effects

Perform the authoritative rate-limit check after authentication identifies the account, but before Ledger reservations, Matching Engine calls, database writes, or event publication:

```text
authenticate -> rate limit -> validate -> reserve funds -> submit order
```

Rejected requests must not consume an Order idempotency key or create a partial order. A retry after the bucket refills should be able to use the same request ID normally.

Whether invalid authenticated requests consume tokens is a policy choice. Charging them discourages clients from using validation failures as free load; rejecting obviously malformed or oversized input at the edge first avoids unnecessary limiter work.

## Rejection response

Return HTTP `429 Too Many Requests` when a request exceeds a limit. Include a conservative `Retry-After` value and machine-readable error information:

```http
HTTP/1.1 429 Too Many Requests
Retry-After: 1
Content-Type: application/json

{
  "code": "rate_limit_exceeded",
  "message": "Order request rate limit exceeded",
  "retry_after_ms": 1000
}
```

Quota headers may expose the applicable limit, remaining capacity, and reset time. Avoid revealing global capacity or internal overload thresholds when that information would create operational or security risk.

Clients should honor `Retry-After`, use exponential backoff with jitter, and retain the same idempotency key when retrying an order whose outcome is uncertain. A request rejected before execution has not created an uncertain outcome.

## Failure policy

Define behavior for limiter-store failures explicitly:

- **Order mutations:** normally fail closed with `503 Service Unavailable` when the authoritative limiter cannot be reached. Failing open could send an unbounded surge to Ledger and Matching.
- **Read-only endpoints:** may use a bounded local fallback when temporary over-admission is acceptable.
- **Global protection:** retain local concurrency and request-queue limits so a Redis outage cannot create unlimited in-process work.

A limiter failure is different from exceeding a quota. Return `503`, not `429`, when the service cannot determine whether a request is allowed.

## Configuration and rollout

Keep limits in versioned server-side configuration. Support different values by environment, endpoint class, and approved account tier. Changes should be auditable and should not require clients to reconnect.

A safe rollout is:

1. Measure request rates, bursts, downstream latency, and capacity.
2. Run the limiter in observe-only mode and record decisions without rejecting traffic.
3. Compare proposed limits with legitimate client behavior.
4. Enable generous limits and monitor rejections and downstream saturation.
5. Tune gradually using production evidence and load tests.

## Observability

Record metrics by limit class and endpoint, while avoiding high-cardinality account or API-key labels:

- allowed and rejected requests;
- token-bucket decision latency;
- Redis errors and timeouts;
- local fallback or fail-closed decisions;
- retry-after duration;
- global and per-symbol saturation;
- downstream Ledger and Matching latency and errors.

Structured logs may include the authenticated account or API-key identifier under the service's normal privacy and retention controls. Alerts should distinguish client-specific throttling from global saturation and limiter infrastructure failure.

The initial success criteria are that abusive clients are isolated, normal bursts remain successful, all replicas enforce one shared allowance, and throttling occurs before any order side effect.
