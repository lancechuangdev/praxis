# Kafka dead-letter queues

Status: partially implemented. The Ledger command consumer has a versioned DLQ
envelope, deterministic failure classification, a provisioned
`ledger.commands.dlq.v1` topic, and transactional PostgreSQL inbox/outbox
handoff. Operator inspection, replay, and the remaining future consumers are
not yet implemented.

Praxis may use a Kafka topic as the only dead-letter store for failures caused
by consuming Kafka records. A database dead-letter table is not required for
that case. Database-backed dead letters remain appropriate for workflows whose
authoritative state already lives in a database, such as the Order saga.

These mechanisms address different failure boundaries:

| Failure | Durable destination |
|---|---|
| A Kafka record cannot be decoded or processed after bounded retries | Kafka DLQ topic |
| A database-backed business workflow exhausts its recovery policy | Workflow state and, optionally, a database dead-letter table |

A DLQ preserves failed work for investigation and controlled replay. It does
not replace stable event IDs, consumer inbox deduplication, producer
idempotence, schema compatibility, or reconciliation.

## Topic convention

Give each source topic a corresponding DLQ topic:

```text
ledger.commands.v1     -> ledger.commands.dlq.v1
ledger.events.v1       -> ledger.events.dlq.v1
matching.events.v1     -> matching.events.dlq.v1
```

Keep the original record key so related records remain inspectable together.
A replay must publish to the original topic with the original key and stable
event ID. Replaying must not invent a new business event identity.

DLQ retention should be longer than source-topic retention and long enough for
the operations response target. A DLQ is not permanent archival storage.

## Consumer policy

For each record:

1. Classify the failure as transient or terminal.
2. Retry transient failures with bounded exponential backoff and jitter.
3. Send terminal failures, or transient failures whose attempt budget is
   exhausted, to the source topic's DLQ.
4. Commit the source offset only when processing succeeded or the DLQ write is
   durable.
5. Alert on DLQ writes and continue consuming when ordering rules permit it.

Examples of terminal failures include malformed envelopes, unsupported schema
versions, invalid required fields, and deterministic business validation
failures. Network errors, broker errors, database unavailability, and service
timeouts are normally transient. Do not move infrastructure failures directly
to a DLQ merely to reduce consumer lag.

If later records for the same key depend on the failed record, dead-lettering
it and continuing can corrupt the projection. Such consumers must pause that
key or partition, repair and replay the gap, or rebuild the projection from an
authoritative source.

## Atomic handoff

Publishing a DLQ record and committing the source offset are a dual write. The
implementation must not perform them as unrelated operations:

- If the offset commits but the DLQ publish fails, the record is lost.
- If the DLQ publish succeeds but the offset commit fails, the DLQ may contain
  duplicates.

The preferred Kafka-only implementation uses a Kafka transaction to publish
the DLQ record and commit the consumed offset atomically. The producer must use
a stable, instance-unique `transactional.id`; consumers of transactional output
must use `read_committed` isolation. Producer idempotence alone is insufficient
because it does not make the DLQ publish and offset commit atomic.

An alternative is to write a dead-letter outbox row in the same local database
transaction as application state, then publish it with an outbox relay. That
is not a Kafka-only design, but is useful when processing already requires a
database transaction.

All consumers and replay tools must tolerate duplicate DLQ records. Kafka
transactions cover Kafka writes and offsets, not external database mutations.

## DLQ record contract

Use a versioned envelope containing enough information to reproduce and audit
the failure without depending on short-lived application logs:

```json
{
  "version": 1,
  "id": "dlq:ledger.commands.v1:3:4812",
  "source_topic": "ledger.commands.v1",
  "source_partition": 3,
  "source_offset": 4812,
  "source_timestamp": "2026-10-07T18:30:00Z",
  "source_key": "YWNjdF8x",
  "source_headers": [
    {"key": "traceparent", "value": "MDAtLi4u"}
  ],
  "consumer_group": "cex-ledger-service",
  "failure_class": "deterministic",
  "failure_code": "unsupported_event_type",
  "failure_reason": "unsupported event type",
  "failed_at": "2026-10-07T18:31:05Z",
  "attempt_count": 1,
  "replay_count": 0,
  "original": "ey4uLn0="
}
```

JSON represents the binary source key, header values, and original value as
base64 strings. Preserve those bytes and the header order exactly. Normalized
failure codes support automation; free-form failure reasons are diagnostic
text. Payloads may contain personal or financial data, so DLQ access,
encryption, log redaction, and retention must be at least as restrictive as
for the source topic.

## Replay

Replay is an explicit operational action, not an automatic loop from the DLQ
back to the source topic. Tooling should:

1. Select records by source topic, error code, time range, and event ID.
2. Require the underlying defect or dependency failure to be resolved.
3. Preserve the original key, payload, headers, and event ID.
4. Add replay metadata in separate headers, including the DLQ topic, partition,
   offset, operator or job ID, and replay time.
5. Publish through an idempotent producer and record the result.
6. Rate-limit replay so it cannot overwhelm the recovered consumer.

Do not delete a DLQ record to indicate resolution. Kafka records are immutable;
resolution status belongs in replay audit data or operational tooling.

For broader developer guidance on historical reprocessing, read
[Event replay and projections](event-replay-and-projections.md).

## Observability

At minimum, expose and alert on:

- DLQ writes by source topic, consumer group, and error code;
- DLQ publish failures;
- retry attempts and exhausted retries;
- DLQ topic size and oldest record age;
- replay attempts, successes, and failures; and
- consumer lag while a partition is paused.

Logs and traces should include the stable event ID, correlation ID, source
topic/partition/offset, and resulting DLQ topic/partition/offset.

## Current Praxis implementation and gaps

The Ledger command consumer classifies malformed envelopes, invalid command
payloads, and unsupported event types as deterministic failures. It inserts a
deterministic inbox marker and a `ledger.commands.dlq.v1` outbox event in one
PostgreSQL transaction before committing the source offset. A redelivery in
the crash window reuses the same DLQ ID and cannot create another outbox row.
Infrastructure provisions the topic and grants Ledger narrowly scoped write
access.

The following work remains:

- add authenticated, authorized, audited, and rate-limited inspection and
  selective replay tooling;
- add DLQ metrics, alerts, and a tested replay runbook;
- test crashes before and after handoff and offset commit; and
- add corresponding DLQs if future source consumers are implemented.
