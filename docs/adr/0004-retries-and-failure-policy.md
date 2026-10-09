# ADR 0004: Bound retries with one end-to-end failure policy

- Status: Accepted
- Date: 2026-10-08

## Context

Praxis can retry at the HTTP client, Order saga, gRPC client, service-mesh, and
Kafka consumer layers. Independent retry policies multiply attempts, increase
load during an outage, and make the point of failure difficult to determine.
For a timed-out financial mutation, the server may have committed even though
the caller did not receive the response.

## Decision

Treat all attempts for one operation as a single end-to-end retry budget.

- A synchronous call has a deadline inside its caller's remaining deadline.
- A timeout means the outcome is unknown. A mutation may be retried only with
  the same stable idempotency key.
- Automatic retries are limited to explicitly classified transient failures.
- Validation, authorization, invariant, schema, and other deterministic errors
  are not retried automatically.
- Backoff is bounded and uses jitter. Concurrency, attempt count, and total
  elapsed time are bounded.
- Application code owns financial retry semantics. Transparent gRPC or mesh
  retries remain disabled until an RPC-specific contract assigns them part of
  the same budget.
- Durable workflow retries persist their attempt count and next-attempt time.
  Only one worker may own an attempt.
- Kafka consumers retry transient failures in a bounded manner and atomically
  quarantine poison records according to the DLQ contract.
- Recovery traffic is rate- and concurrency-limited so it cannot starve new
  work.

Each operation documents its idempotency key, retryable statuses, maximum
attempts, overall deadline or recovery window, compensation, and terminal
handling. A retry policy without those fields is incomplete.

## Failure responses

- Quota exhaustion returns `429 Too Many Requests` with conservative retry
  guidance.
- Failure of an authoritative admission dependency normally returns `503
  Service Unavailable` or the operation-specific unavailable status.
- An ambiguous mutation result is not reported as a confirmed failure.
- Exhausted durable workflows enter a dead-letter state and require audited
  recovery or reconciliation.

## Consequences

- Some callers receive bounded failure rather than indefinite attempts.
- Idempotency storage and retention are required at mutation boundaries.
- Retry budgets, attempts by layer, deadlines, backlog, and dead letters become
  required telemetry.
- Outlier detection and circuit breaking may stop attempts but cannot invent a
  separate retry allowance.

