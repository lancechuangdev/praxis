-- Mutable relay state is separate from immutable journal source facts.
CREATE TABLE ledger_journal_delivery (
    event_id TEXT PRIMARY KEY REFERENCES ledger_journal_outbox(event_id),
    published_at TIMESTAMPTZ,
    attempt_count BIGINT NOT NULL DEFAULT 0 CHECK (attempt_count>=0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ledger_journal_delivery_pending ON ledger_journal_delivery(next_attempt_at,event_id)
    WHERE published_at IS NULL;

INSERT INTO ledger_journal_delivery(event_id) SELECT event_id FROM ledger_journal_outbox;

CREATE FUNCTION enqueue_lifecycle_journal() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO ledger_journal_delivery(event_id) VALUES(NEW.event_id);
    RETURN NEW;
END $$;
CREATE TRIGGER ledger_journal_enqueue AFTER INSERT ON ledger_journal_outbox
    FOR EACH ROW EXECUTE FUNCTION enqueue_lifecycle_journal();
