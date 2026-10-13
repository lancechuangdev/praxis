# Order client notifications

This document describes a proposed notification architecture for Praxis order updates. WebSocket, Server-Sent Events, mobile push, webhook delivery, and the Notification service are not currently implemented in this repository.

For the planned service boundaries and Kafka topics, see [Microservices migration](microservices-migration.md). For the rule that notifications remain outside the financial hot path, see [ADR 0001](adr/0001-synchronous-calls-and-kafka.md).

## Goals and invariants

The notification system should:

- deliver accepted, partially filled, filled, cancelled, rejected, and expired order updates promptly;
- support browsers, mobile applications, and institutional API clients;
- recover safely from disconnects and missed messages;
- preserve ordering for updates to the same order or account;
- isolate delivery failures from order execution and Ledger settlement;
- prevent one user from observing another user's orders;
- make duplicate delivery harmless.

Notifications are derived views of committed facts. A notification must never determine whether an order executes, whether funds settle, or whether a Ledger transaction commits.

## Choose delivery by client type

A webhook is a server-to-server callback and is not normally suitable for a browser or mobile application. Browsers and mobile devices do not expose stable public HTTPS endpoints and may be offline, suspended, behind NAT, or moving between networks.

Use the following channels:

| Client | Primary channel | Recovery or background channel |
|---|---|---|
| Browser trading UI | WebSocket or SSE | Order snapshot and event replay |
| Mobile app in foreground | WebSocket | Order snapshot and event replay |
| Mobile app in background | APNs or FCM push | Fetch current state when opened |
| Institutional backend | Signed webhook | Delivery log and replay API |
| Simple or degraded client | Bounded polling | Paginated order GET API |

### WebSocket

WebSocket is the preferred interactive channel when a trading client needs low-latency updates and may later need other bidirectional realtime features. Keep order commands on authenticated HTTP or gRPC APIs; receiving a notification is not acknowledgement of a new command.

### Server-Sent Events

SSE is a simpler alternative when data only flows from server to client. It uses ordinary HTTP semantics and supports a last-event identifier, but it is less suitable when Praxis wants one bidirectional connection for several realtime features.

### Mobile push

Apple Push Notification Service and Firebase Cloud Messaging can alert a backgrounded mobile app. Push delivery may be delayed, duplicated, grouped, or dropped, so a push message is a hint that state changed, not authoritative order state. The app fetches the current order view when opened.

Avoid placing sensitive quantities or other private financial details in lock-screen notifications unless the user has explicitly enabled them.

### Webhook

Webhooks are appropriate for customers that operate an HTTPS backend, such as an institutional order-management system. The receiving backend must acknowledge quickly and process the event idempotently.

## Proposed architecture

```text
Order and Matching transactions
          |
          v
Transactional outbox
          |
          v
Kafka order and execution events
          |
          +-------------------------+
          |                         |
          v                         v
Order status projection       Notification service
          |                         |
          |                         +--> WebSocket/SSE gateway
          |                         +--> APNs/FCM provider
          |                         +--> Webhook dispatcher
          |                         +--> Email provider, if enabled
          v
Snapshot, history, and replay API
```

The producer commits its domain change and outbox event in the same local transaction. Delivery begins only from committed outbox events. Matching, Order, and Ledger never wait for a browser connection, push provider, or customer webhook.

An order-status projection should combine the committed Order, Matching, and relevant Ledger facts required for the customer-facing view. Praxis stores this customer-facing order history in MongoDB Atlas on AWS. It is rebuildable and must not authorize financial mutations.

The Notification service also stores notification history in MongoDB Atlas as a
derived delivery view. Each record includes the authenticated account owner,
stable event ID, per-user stream sequence, channel, delivery state, occurrence
time, and bounded delivery-attempt metadata. Provider tokens, webhook secrets,
and other credentials are not stored in notification-history documents. A
unique event/channel constraint makes repeated Kafka delivery harmless.

Notification history supports reconnect replay, customer-visible delivery
history, and operator investigation. It is not proof that a customer observed
an event, and deleting or rebuilding it must not change committed order state.

## Event contract

An illustrative client event is:

```json
{
  "event_id": "evt-123",
  "event_type": "order_updated",
  "user_id": "alice",
  "order_id": "order-456",
  "status": "partially_filled",
  "filled_quantity": "0.25",
  "remaining_quantity": "0.75",
  "order_version": 7,
  "stream_sequence": 817,
  "occurred_at": "2026-10-04T18:30:00Z",
  "schema_version": 1
}
```

The contract should include:

- a globally unique `event_id` for deduplication;
- an `order_id` and authenticated owner identity;
- a monotonic order version so stale events can be ignored;
- a resumable per-user or per-stream sequence;
- the business occurrence time, distinct from delivery time;
- an explicit schema version.

Use strings or exact atomic-unit values for financial quantities. Do not use binary floating-point numbers.

## Ordering, duplication, and terminal states

Kafka and delivery retries are at least once, so consumers and clients must tolerate duplicates. A client records recently observed event IDs and ignores an order version older than or equal to the newest applied version.

Partition order events by an ordering key that matches the required guarantee. `order_id` preserves ordering for one order. `user_id` can preserve a single customer stream but may create hot partitions for unusually active accounts. Document which guarantee the API provides rather than implying global ordering.

Terminal statuses such as filled, cancelled, rejected, and expired must not regress to an earlier active state. Conflicting terminal events should create an operational exception rather than being resolved by arrival time.

## Reconnect and replay

A live connection alone is insufficient because it can disconnect precisely when an order changes. Every realtime client needs a recovery path:

```text
1. Client stores the last applied stream sequence.
2. Client reconnects and supplies that sequence.
3. Server replays later retained events in order.
4. Client deduplicates and applies increasing versions.
5. If the cursor expired, client fetches a fresh snapshot.
6. Live delivery continues after the replay boundary.
```

An illustrative WebSocket subscription message is:

```json
{
  "type": "subscribe",
  "channel": "orders",
  "after_sequence": 817
}
```

For SSE, the client can send `Last-Event-ID`. For either protocol, the server should establish the snapshot/replay boundary so events committed during snapshot loading are not lost.

Keep replay retention bounded. When the supplied cursor is too old, return an explicit resynchronization instruction rather than silently starting at the newest event.

## Authentication and authorization

- Authenticate the realtime connection with a secure cookie or short-lived token.
- Do not put long-lived credentials in WebSocket or webhook URLs because URLs appear in logs and monitoring systems.
- Authorize subscriptions by the authenticated account; knowing an `order_id` is not authorization.
- Revalidate authorization when credentials expire, an account is disabled, or permissions change.
- Limit connections, subscriptions, and message rates per account and IP.
- Encrypt all external delivery with TLS.

A connection gateway may route by authenticated user ID internally, but client-supplied user IDs must not select another user's stream.

## Webhook security and delivery

Webhook endpoints must use HTTPS. Praxis should sign the exact transmitted body with a per-endpoint secret and include a key identifier, event ID, and delivery timestamp in headers. The receiver verifies the signature over the raw body and rejects timestamps outside a bounded replay window.

Illustrative headers are:

```http
Praxis-Event-ID: evt-123
Praxis-Delivery-Timestamp: 1791163800
Praxis-Signature: v1=<signature>
```

Webhook delivery should use:

- endpoint ownership verification before activation;
- short connection and response deadlines;
- bounded exponential retry with jitter;
- stable event IDs across retries;
- per-customer concurrency and rate limits;
- delivery-attempt history;
- dead-letter handling and controlled replay;
- secret rotation with an overlap period;
- protection against SSRF and private-network destinations.

Treat any documented `2xx` response as delivery acknowledgement. Do not let a slow or failing endpoint block another customer or a Kafka partition indefinitely.

## Push-notification behavior

The Notification service stores user channel preferences and provider tokens separately from order state. Tokens are secrets and require encryption, rotation, invalid-token cleanup, and access controls.

Push messages should contain a stable reference such as `order_id` and a generic event category. On user interaction, the app authenticates and retrieves the current projection. Provider acknowledgement means the provider accepted the message, not that the user saw it.

Respect notification preferences, quiet hours where appropriate, and jurisdictional privacy requirements. Security-critical account notifications may have a different policy from routine fill notifications.

## Backpressure and failure isolation

Delivery providers and clients can be slower than the event stream. Apply bounded queues and isolate channels so one provider does not delay the others:

```text
Notification policy consumer
       |
       +--> realtime delivery queue
       +--> push delivery queue
       +--> webhook delivery queue
       +--> email delivery queue
```

Do not keep unbounded messages in gateway memory. Disconnect persistently slow realtime clients and let them recover through replay. Pause or dead-letter repeatedly failing webhook deliveries according to documented policy.

Notification backlog must never apply backpressure to Matching or Ledger transactions. Kafka lag and delayed client notifications are operational incidents, but they are not reasons to undo a completed trade.

## Client API behavior

The order read model remains the customer-facing source for current status. Realtime and push messages announce changes; clients reconcile them with snapshot and history endpoints.

Clients should:

- deduplicate by `event_id`;
- apply only increasing `order_version` values;
- persist the latest resumable sequence;
- reconnect with exponential backoff and jitter;
- fetch a snapshot after cursor expiry or an unexplained sequence gap;
- avoid interpreting connection loss as order failure;
- reuse the original idempotency key when retrying a command with an uncertain outcome.

## Observability

Measure:

- event-to-client delivery latency by channel;
- Kafka consumer lag and oldest undelivered event age;
- active realtime connections and subscriptions;
- reconnect, replay, snapshot-resync, and slow-client counts;
- delivery attempts, successes, retries, and dead letters;
- webhook response status and latency;
- invalid push tokens and provider errors;
- authorization failures and rate-limit rejections;
- order projection version and event-sequence gaps.

Avoid user IDs, order IDs, endpoint URLs, and provider tokens as metric labels. Use protected structured logs and trace correlation for individual investigations.

## Recommended delivery stages

1. Define versioned order lifecycle events and publish them through a transactional outbox.
2. Build a rebuildable MongoDB Atlas order-status/history projection with authenticated snapshot and history endpoints.
3. Implement a Notification consumer with inbox deduplication and a fake provider.
4. Add authenticated WebSocket or SSE delivery with sequence-based replay.
5. Add APNs and FCM for background mobile notifications.
6. Add signed webhooks for institutional customers, including retry and replay operations.
7. Add email only where product requirements justify its latency and privacy trade-offs.

The governing rule is:

```text
Order execution commits first.
Notification delivery happens asynchronously afterward.
```
