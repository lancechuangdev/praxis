-- Expand-only. Capture is opt-in per writer connection. The Go commit hook
-- validates and encodes the complete fact before the financial tx commits.
CREATE TABLE ledger_journal_outbox (
    event_id TEXT PRIMARY KEY,
    journal_id TEXT NOT NULL UNIQUE REFERENCES ledger_journals(id),
    payload BYTEA NOT NULL CHECK (octet_length(payload)>0 AND octet_length(payload)<=1048576),
    occurred_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TRIGGER ledger_journal_outbox_immutable BEFORE UPDATE OR DELETE ON ledger_journal_outbox
    FOR EACH ROW EXECUTE FUNCTION reject_ledger_mutation();

CREATE FUNCTION track_lifecycle_journal() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE previous TEXT;
BEGIN
    IF current_setting('praxis.ledger_lifecycle_capture',true) IS DISTINCT FROM 'on' THEN RETURN NEW; END IF;
    previous := NULLIF(current_setting('praxis.ledger_lifecycle_journal',true),'');
    IF previous IS NOT NULL AND previous<>NEW.id THEN
        RAISE EXCEPTION 'lifecycle capture requires one new journal per transaction';
    END IF;
    PERFORM set_config('praxis.ledger_lifecycle_journal',NEW.id,true);
    RETURN NEW;
END $$;
CREATE TRIGGER ledger_lifecycle_track_journal AFTER INSERT ON ledger_journals
    FOR EACH ROW EXECUTE FUNCTION track_lifecycle_journal();

CREATE FUNCTION track_lifecycle_balance() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE versions JSONB;
BEGIN
    IF current_setting('praxis.ledger_lifecycle_capture',true) IS DISTINCT FROM 'on' THEN RETURN NEW; END IF;
    IF NEW.user_asset_account_id<>OLD.user_asset_account_id OR NEW.version<=OLD.version THEN
        RAISE EXCEPTION 'lifecycle balance mutation requires a stable account and increasing version';
    END IF;
    IF OLD.version=0 AND (OLD.available_atomic<>0 OR OLD.reserved_atomic<>0 OR
       OLD.withdrawal_pending_atomic<>0 OR OLD.deposit_pending_atomic<>0 OR OLD.hold_atomic<>0) THEN
        RAISE EXCEPTION 'nonempty version-zero balance requires reviewed lifecycle bootstrap';
    END IF;
    versions := COALESCE(NULLIF(current_setting('praxis.ledger_lifecycle_before',true),'')::jsonb,'{}'::jsonb);
    IF NOT versions ? OLD.user_asset_account_id THEN
        versions := jsonb_set(versions,ARRAY[OLD.user_asset_account_id],to_jsonb(OLD.version::text));
        PERFORM set_config('praxis.ledger_lifecycle_before',versions::text,true);
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER ledger_lifecycle_track_balance BEFORE UPDATE ON user_asset_balances
    FOR EACH ROW EXECUTE FUNCTION track_lifecycle_balance();

CREATE FUNCTION require_lifecycle_fact() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF current_setting('praxis.ledger_lifecycle_capture',true)='on' AND
       NOT EXISTS(SELECT 1 FROM ledger_journal_outbox WHERE journal_id=NEW.id) THEN
        RAISE EXCEPTION 'captured journal requires an atomic lifecycle fact';
    END IF;
    RETURN NEW;
END $$;
CREATE CONSTRAINT TRIGGER ledger_lifecycle_require_fact AFTER INSERT ON ledger_journals
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION require_lifecycle_fact();
