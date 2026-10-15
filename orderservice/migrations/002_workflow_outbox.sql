-- Expand-only. Order owns both this source outbox and its saga version.
-- Existing rows reserve unpublished version 1. Their first capture starts at
-- version 2+, making the need for a historical bootstrap explicit.
ALTER TABLE order_sagas ADD COLUMN workflow_version BIGINT NOT NULL DEFAULT 1
    CHECK (workflow_version > 0);

CREATE TABLE order_workflow_outbox (
    event_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    workflow_version BIGINT NOT NULL CHECK (workflow_version > 0),
    occurred_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    UNIQUE (order_id, workflow_version)
);

-- Retry metadata, claims and no-op state replays must not create new facts.
CREATE FUNCTION version_order_workflow() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        NEW.workflow_version := 1;
    ELSIF ROW(NEW.state, NEW.reservation_id, NEW.release_reason) IS DISTINCT FROM
          ROW(OLD.state, OLD.reservation_id, OLD.release_reason) THEN
        NEW.workflow_version := OLD.workflow_version + 1;
    ELSE
        NEW.workflow_version := OLD.workflow_version;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER order_workflow_version BEFORE INSERT OR UPDATE ON order_sagas
    FOR EACH ROW EXECUTE FUNCTION version_order_workflow();

CREATE FUNCTION capture_order_workflow() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    event_id TEXT;
    event_time TIMESTAMPTZ;
    workflow_state TEXT;
BEGIN
    IF current_setting('praxis.order_lifecycle_capture', true) IS DISTINCT FROM 'on' THEN
        RETURN NEW;
    END IF;
    IF TG_OP = 'UPDATE' THEN
        IF NEW.workflow_version = OLD.workflow_version THEN RETURN NEW; END IF;
    END IF;
    event_id := 'evt_ow_' || encode(sha256(convert_to(NEW.order_id, 'UTF8')), 'hex') || ':' || NEW.workflow_version;
    event_time := clock_timestamp();
    workflow_state := CASE WHEN NEW.state = 'started' THEN 'RESERVATION_PENDING'
                           ELSE upper(NEW.state) END;
    INSERT INTO order_workflow_outbox(event_id, order_id, workflow_version, occurred_at, payload)
    VALUES (event_id, NEW.order_id, NEW.workflow_version, event_time,
        jsonb_build_object(
            'event_id', event_id,
            'event_type', 'OrderWorkflowSnapshot',
            'schema_version', 1,
            'occurred_at', event_time,
            'correlation_id', COALESCE(NULLIF(NEW.correlation_id, ''), NEW.request_id),
            'causation_id', COALESCE(NULLIF(NEW.causation_id, ''), NEW.request_id),
            'producer', 'order-service',
            'order_workflow', jsonb_build_object(
                'request_id', NEW.request_id,
                'order_id', NEW.order_id,
                'user_id', NEW.request_payload->>'user_id',
                'workflow_version', NEW.workflow_version::text,
                'state', 'ORDER_WORKFLOW_STATE_' || workflow_state,
                'symbol', NEW.request_payload->>'symbol',
                'side', NEW.request_payload->>'side',
                'order_type', NEW.request_payload->>'order_type',
                'quantity', NEW.request_payload->>'quantity',
                'price', CASE WHEN NEW.request_payload->>'order_type' = 'limit'
                              THEN NEW.request_payload->>'price' ELSE '' END,
                'reserve_asset_id', NEW.request_payload->>'reserve_asset_id',
                'reserve_amount_atomic', NEW.request_payload->>'reserve_amount_atomic',
                'reservation_id', COALESCE(NEW.reservation_id, ''),
                'release_reason', COALESCE(NEW.release_reason, '')
            )
        ));
    RETURN NEW;
END;
$$;
CREATE TRIGGER order_workflow_capture AFTER INSERT OR UPDATE ON order_sagas
    FOR EACH ROW EXECUTE FUNCTION capture_order_workflow();
