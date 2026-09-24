CREATE TABLE assets (
    id TEXT PRIMARY KEY,
    code TEXT NOT NULL UNIQUE CHECK (code = upper(code) AND btrim(code) <> ''),
    name TEXT NOT NULL CHECK (btrim(name) <> ''),
    decimals SMALLINT NOT NULL CHECK (decimals BETWEEN 0 AND 30),
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','deposit_disabled','withdrawal_disabled','disabled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE networks (
    id TEXT PRIMARY KEY,
    code TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    native_asset_id TEXT NOT NULL REFERENCES assets(id)
);

CREATE TABLE asset_networks (
    id TEXT PRIMARY KEY,
    asset_id TEXT NOT NULL REFERENCES assets(id),
    network_id TEXT NOT NULL REFERENCES networks(id),
    token_standard TEXT NOT NULL,
    contract_address TEXT,
    contract_decimals SMALLINT NOT NULL CHECK (contract_decimals BETWEEN 0 AND 30),
    deposit_enabled BOOLEAN NOT NULL DEFAULT true,
    withdrawal_enabled BOOLEAN NOT NULL DEFAULT true,
    required_confirmations INTEGER NOT NULL CHECK (required_confirmations > 0),
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','disabled')),
    UNIQUE (asset_id, network_id),
    UNIQUE (network_id, contract_address)
);

INSERT INTO assets(id,code,name,decimals) VALUES
    ('asset_usdt','USDT','Tether USD',6),
    ('asset_eth','ETH','Ether',18);
INSERT INTO networks(id,code,name,native_asset_id) VALUES
    ('network_ethereum','ETHEREUM','Ethereum Mainnet','asset_eth');
INSERT INTO asset_networks(id,asset_id,network_id,token_standard,contract_address,contract_decimals,required_confirmations) VALUES
    ('asset_network_usdt_ethereum','asset_usdt','network_ethereum','ERC20','0x_usdt_contract_address',6,12);

CREATE TABLE ledger_accounts (
    id TEXT PRIMARY KEY,
    code TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    account_type TEXT NOT NULL CHECK (account_type IN ('asset','liability','equity','revenue','expense')),
    purpose TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO ledger_accounts (id,code,name,account_type,purpose) VALUES
    ('account_customer_available','customer:available','Customer available balances','liability','available'),
    ('account_customer_reserved','customer:reserved','Customer reserved balances','liability','reserved'),
    ('account_customer_withdrawal_pending','customer:withdrawal-pending','Customer pending withdrawals','liability','withdrawal_pending'),
    ('account_customer_deposit_pending','customer:deposit-pending','Customer pending deposits','liability','deposit_pending'),
    ('account_customer_hold','customer:hold','Customer held balances','liability','hold'),
    ('account_crypto_custody','asset:crypto-custody','Cryptocurrency assets in custody','asset','crypto_custody'),
    ('account_trading_fee_revenue','revenue:trading-fee','Trading fee revenue','revenue','trading_fee');

CREATE TABLE user_asset_accounts (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL CHECK (btrim(user_id) <> ''),
    asset_id TEXT NOT NULL REFERENCES assets(id),
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','frozen','closed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    closed_at TIMESTAMPTZ,
    UNIQUE (user_id, asset_id),
    CHECK ((status='closed' AND closed_at IS NOT NULL) OR (status<>'closed' AND closed_at IS NULL))
);

CREATE TABLE user_asset_balances (
    user_asset_account_id TEXT PRIMARY KEY REFERENCES user_asset_accounts(id),
    available_atomic NUMERIC(78,0) NOT NULL DEFAULT 0 CHECK (available_atomic >= 0),
    reserved_atomic NUMERIC(78,0) NOT NULL DEFAULT 0 CHECK (reserved_atomic >= 0),
    withdrawal_pending_atomic NUMERIC(78,0) NOT NULL DEFAULT 0 CHECK (withdrawal_pending_atomic >= 0),
    deposit_pending_atomic NUMERIC(78,0) NOT NULL DEFAULT 0 CHECK (deposit_pending_atomic >= 0),
    hold_atomic NUMERIC(78,0) NOT NULL DEFAULT 0 CHECK (hold_atomic >= 0),
    version BIGINT NOT NULL DEFAULT 0 CHECK (version >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE custody_positions (
    id TEXT PRIMARY KEY,
    asset_network_id TEXT NOT NULL REFERENCES asset_networks(id),
    wallet_id TEXT NOT NULL,
    address TEXT NOT NULL,
    wallet_purpose TEXT NOT NULL CHECK (wallet_purpose IN ('deposit','hot','warm','cold','gas')),
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','disabled')),
    UNIQUE (asset_network_id,wallet_id,address,wallet_purpose)
);

-- Development fixture matching the architecture walkthrough. Production
-- custody positions should be provisioned by controlled treasury tooling.
INSERT INTO custody_positions(id,asset_network_id,wallet_id,address,wallet_purpose) VALUES
    ('custody_position_alice_eth_usdt','asset_network_usdt_ethereum','wallet_alice_deposit','0x_alice_deposit_address','deposit');

CREATE TABLE ledger_journals (
    id TEXT PRIMARY KEY,
    reference_type TEXT NOT NULL,
    reference_id TEXT NOT NULL,
    journal_type TEXT NOT NULL,
    source_system TEXT NOT NULL,
    source_event_id TEXT NOT NULL,
    correlation_id TEXT,
    causation_id TEXT,
    reverses_journal_id TEXT REFERENCES ledger_journals(id),
    description TEXT,
    metadata JSONB NOT NULL DEFAULT '{}',
    occurred_at TIMESTAMPTZ NOT NULL,
    posted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (source_system,source_event_id),
    CHECK (btrim(reference_type)<>'' AND btrim(reference_id)<>'' AND btrim(journal_type)<>''),
    CHECK (btrim(source_system)<>'' AND btrim(source_event_id)<>''),
    CHECK (reverses_journal_id IS NULL OR reverses_journal_id<>id)
);

CREATE TABLE ledger_entries (
    id TEXT PRIMARY KEY,
    journal_id TEXT NOT NULL REFERENCES ledger_journals(id),
    ledger_account_id TEXT NOT NULL REFERENCES ledger_accounts(id),
    user_asset_account_id TEXT REFERENCES user_asset_accounts(id),
    asset_id TEXT NOT NULL REFERENCES assets(id),
    custody_position_id TEXT REFERENCES custody_positions(id),
    bucket TEXT CHECK (bucket IN ('available','reserved','withdrawal_pending','deposit_pending','hold')),
    side TEXT NOT NULL CHECK (side IN ('debit','credit')),
    amount_atomic NUMERIC(78,0) NOT NULL CHECK (amount_atomic>0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK ((user_asset_account_id IS NULL AND bucket IS NULL) OR (user_asset_account_id IS NOT NULL AND bucket IS NOT NULL)),
    CHECK (num_nonnulls(user_asset_account_id,custody_position_id)<=1)
);

CREATE TABLE fund_reservations (
    id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL UNIQUE,
    user_asset_account_id TEXT NOT NULL REFERENCES user_asset_accounts(id),
    asset_id TEXT NOT NULL REFERENCES assets(id),
    original_atomic NUMERIC(78,0) NOT NULL CHECK (original_atomic>0),
    remaining_atomic NUMERIC(78,0) NOT NULL CHECK (remaining_atomic>=0 AND remaining_atomic<=original_atomic),
    status TEXT NOT NULL CHECK (status IN ('active','partially_consumed','consumed','released')),
    version BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE ledger_symbol_offsets (
    engine_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    last_sequence_number BIGINT NOT NULL,
    PRIMARY KEY (engine_id, symbol)
);

CREATE TABLE inbox_events (
    consumer_name TEXT NOT NULL,
    event_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (consumer_name,event_id)
);

CREATE TABLE IF NOT EXISTS outbox_events (
    sequence_number BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    id TEXT PRIMARY KEY,
    topic TEXT NOT NULL,
    event_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    message_key TEXT NOT NULL,
    payload JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_error TEXT
);

CREATE INDEX ledger_entries_journal_idx ON ledger_entries(journal_id,id);
CREATE INDEX ledger_entries_user_idx ON ledger_entries(user_asset_account_id,bucket,id) WHERE user_asset_account_id IS NOT NULL;
CREATE INDEX ledger_entries_custody_idx ON ledger_entries(custody_position_id,id) WHERE custody_position_id IS NOT NULL;
CREATE INDEX ledger_journals_reference_idx ON ledger_journals(reference_type,reference_id,occurred_at,id);
CREATE INDEX outbox_pending_idx ON outbox_events(next_attempt_at,sequence_number) WHERE published_at IS NULL;

CREATE FUNCTION validate_ledger_entry_dimensions() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    v_asset_id TEXT;
    v_status TEXT;
    v_type TEXT;
    v_purpose TEXT;
BEGIN
    SELECT account_type,purpose INTO v_type,v_purpose FROM ledger_accounts WHERE id=NEW.ledger_account_id FOR SHARE;
    IF NOT FOUND THEN RAISE EXCEPTION 'unknown ledger account %',NEW.ledger_account_id USING ERRCODE='foreign_key_violation'; END IF;
    IF NEW.user_asset_account_id IS NOT NULL THEN
        SELECT asset_id,status INTO v_asset_id,v_status FROM user_asset_accounts WHERE id=NEW.user_asset_account_id FOR SHARE;
        IF v_status='closed' THEN RAISE EXCEPTION 'user asset account is closed' USING ERRCODE='check_violation'; END IF;
        IF v_asset_id<>NEW.asset_id OR v_type<>'liability' OR v_purpose<>NEW.bucket THEN
            RAISE EXCEPTION 'invalid user ledger entry dimensions' USING ERRCODE='check_violation';
        END IF;
    ELSIF NEW.custody_position_id IS NOT NULL THEN
        SELECT an.asset_id,cp.status INTO v_asset_id,v_status FROM custody_positions cp JOIN asset_networks an ON an.id=cp.asset_network_id WHERE cp.id=NEW.custody_position_id FOR SHARE OF cp,an;
        IF v_status<>'active' OR v_asset_id<>NEW.asset_id OR v_type<>'asset' THEN
            RAISE EXCEPTION 'invalid custody ledger entry dimensions' USING ERRCODE='check_violation';
        END IF;
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER ledger_entries_validate BEFORE INSERT ON ledger_entries FOR EACH ROW EXECUTE FUNCTION validate_ledger_entry_dimensions();

CREATE FUNCTION reject_ledger_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only; % is not allowed',TG_TABLE_NAME,TG_OP USING ERRCODE='integrity_constraint_violation';
END $$;
CREATE TRIGGER ledger_journals_immutable BEFORE UPDATE OR DELETE ON ledger_journals FOR EACH ROW EXECUTE FUNCTION reject_ledger_mutation();
CREATE TRIGGER ledger_entries_immutable BEFORE UPDATE OR DELETE ON ledger_entries FOR EACH ROW EXECUTE FUNCTION reject_ledger_mutation();
