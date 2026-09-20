# Mock matching engine

Load-test fixture for synchronous order admission. It accepts `SubmitOrder`
over gRPC, assigns a sequence independently per symbol/engine partition, and
publishes an `OrderAccepted` fact to `matching.events.v1` through Kafka.

```text
Order Service ──gRPC SubmitOrder──► Matching Engine
                                      │
                                      └──Kafka OrderAccepted──► consumers
```

The mock serializes admission per partition, deduplicates `request_id` in
memory, and acknowledges only after synchronous Kafka publication succeeds.
Its state is not durable and it does not implement an order book, matching, or
recovery, so it is not a production matching engine.

Set `MATCHING_KAFKA_ENABLED=false` to measure gRPC/engine overhead without
Kafka. Default ports are gRPC `:9092` and HTTP health/metrics `:8084`.

The binary also supports a `healthcheck` command used by ECS: it exits
successfully only when the local `/readyz` endpoint returns HTTP 200. The
Terraform Matching ECS service is opt-in by image digest and deliberately
single-replica because this mock has no durable partition ownership.
