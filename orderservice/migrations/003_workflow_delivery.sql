-- Expand-only delivery bookkeeping. Immutable source facts remain retained.
ALTER TABLE order_workflow_outbox
    ADD COLUMN published_at TIMESTAMPTZ,
    ADD COLUMN attempt_count BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now();
CREATE INDEX order_workflow_unpublished
    ON order_workflow_outbox (order_id, workflow_version)
    WHERE published_at IS NULL;
