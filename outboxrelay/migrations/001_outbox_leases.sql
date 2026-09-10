-- The relay can bootstrap its own outbox table. The columns without relay
-- prefixes form the producer contract used by the ledger service.
CREATE TABLE IF NOT EXISTS outbox_events (
    sequence_number BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    id              TEXT PRIMARY KEY,
    topic           TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    aggregate_id    TEXT NOT NULL,
    message_key     TEXT NOT NULL,
    payload         JSONB NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL,
    published_at    TIMESTAMPTZ,
    attempt_count   INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_error      TEXT,
    claimed_by      TEXT,
    claimed_until   TIMESTAMPTZ,

    CHECK (btrim(id) <> ''),
    CHECK (btrim(topic) <> ''),
    CHECK (btrim(event_type) <> ''),
    CHECK (btrim(aggregate_id) <> ''),
    CHECK (btrim(message_key) <> ''),
    CHECK (attempt_count >= 0),
    CHECK (
        (claimed_by IS NULL AND claimed_until IS NULL)
        OR (claimed_by IS NOT NULL AND claimed_until IS NOT NULL)
    )
);

-- Keep this migration usable against databases created by the older ledger
-- migration, which did not include the lease columns.
ALTER TABLE outbox_events
    ADD COLUMN IF NOT EXISTS claimed_by TEXT,
    ADD COLUMN IF NOT EXISTS claimed_until TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS outbox_pending_idx
    ON outbox_events (next_attempt_at, sequence_number)
    WHERE published_at IS NULL;

CREATE INDEX IF NOT EXISTS outbox_events_claimable_idx
    ON outbox_events (next_attempt_at, claimed_until, sequence_number)
    WHERE published_at IS NULL;
