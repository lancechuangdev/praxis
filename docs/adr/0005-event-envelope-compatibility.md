# ADR 0005: Version event envelopes and evolve payloads compatibly

- Status: Accepted
- Date: 2026-10-08

## Context

Praxis deploys producers and consumers independently and retains events for
redelivery, replay, and projection rebuilding. A consumer may therefore see
records written by older or newer application versions. The current repository
contains transitional JSON payloads; planned cross-service contracts require a
single compatibility policy before more producers and consumers are added.

## Decision

New cross-service event contracts use registry-controlled Protobuf schemas and
a common versioned envelope containing:

- `event_id`;
- `event_type`;
- `schema_version`;
- `occurred_at`;
- `correlation_id`;
- `causation_id`;
- `producer`; and
- a typed payload.

The Kafka topic and partition key remain record metadata. The payload includes
the business identifier needed to interpret and validate the event, but it does
not duplicate the topic name as authority.

Schema evolution follows these rules:

- never reuse a field number or change an existing field's meaning;
- reserve removed field numbers and names;
- add fields as optional or with a safe semantic default;
- tolerate unknown fields;
- use a new event type or topic version for incompatible semantic changes;
- keep producer and consumer versions compatible through rolling deployment;
- preserve stable event IDs across redelivery;
- represent money and quantities with exact integer units or decimal strings,
  never binary floating point; and
- apply size limits and reject malformed envelopes before business handling.

Existing JSON events migrate producer by producer. During a bounded transition,
a consumer may accept both documented representations. New consumers must not
infer compatibility from unversioned fields.

Compatibility checks run in CI against the accepted schema baseline. Contract
tests also verify envelope population, stable IDs, key selection, and old/new
producer-consumer combinations.

## Consequences

- Schema registry and compatibility policy become production dependencies.
- Incompatible changes require explicit migration and possibly a new topic.
- Events carry small metadata overhead in exchange for replayability and
  independent deployment.
- The existing transitional JSON contracts need planned conversion rather than
  an uncoordinated rewrite.

