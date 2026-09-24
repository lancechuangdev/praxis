# Durable matching admission

The matching service admits orders over gRPC and persists the accepted order and
its `OrderAccepted` outbox event in one PostgreSQL transaction. PostgreSQL locks
the row for the requested symbol while assigning `engine_sequence`, so every
replica observes one serial order for a book such as `BTC-USDT`.

```text
Order Service ──gRPC SubmitOrder──► Matching (one pod per AZ)
                                      │
                                      └──transaction──► Matching RDS writer
                                                        ├─ matching_orders
                                                        └─ outbox_events ──► relay ──► Kafka
```

`request_id` is an idempotency key. Retrying the same request returns the stored
result; reusing it with a different payload is rejected. Therefore a process can
commit and crash before returning gRPC without creating a second order on retry.
The `matching-engine relay` process is intentionally outside the admission transaction and publishes
at least once; consumers must deduplicate by event `id`.

The service connects through `MATCHING_DATABASE_WRITER_URL`, or constructs the
URL from `MATCHING_DB_WRITER_HOST`, `MATCHING_DB_USER`, `MATCHING_DB_NAME`, and
one of `MATCHING_DB_SECRET_ARN`/`MATCHING_DB_PASSWORD`. `MATCHING_DB_WRITER_MAX_CONNS`
defaults to 32. `MATCHING_KAFKA_TOPIC` names the topic stored in outbox rows.

Run `/matching-engine migrate` before rollout, or leave
`MATCHING_MIGRATE_ON_STARTUP=true` for local development. Production uses the
separate three-instance Matching RDS cluster declared in `infra/aws`.

This version durably admits orders and lays out the price-time order index. Trade
execution and fill generation remain the next matching-domain increment.
