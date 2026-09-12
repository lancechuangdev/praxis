CREATE OR REPLACE FUNCTION reserve_for_order(
    p_user_id TEXT,
    p_asset_id TEXT,
    p_amount_atomic NUMERIC,
    p_order_id TEXT,
    p_command_id TEXT,
    p_correlation_id TEXT,
    p_causation_id TEXT,
    p_occurred_at TIMESTAMPTZ,
    p_reservation_id TEXT,
    p_journal_id TEXT,
    p_event_id TEXT,
    p_outbox_payload JSONB
) RETURNS TABLE (
    result_outcome TEXT,
    result_reservation_id TEXT,
    result_order_id TEXT,
    result_status TEXT,
    result_original_atomic TEXT,
    result_remaining_atomic TEXT,
    result_balance_version BIGINT
) LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    v_user_asset_account_id TEXT;
    v_inserted_journal_id TEXT;
    v_existing_reference_id TEXT;
    v_existing_journal_type TEXT;
BEGIN
    result_reservation_id := '';
    result_order_id := '';
    result_status := '';
    result_original_atomic := '';
    result_remaining_atomic := '';
    result_balance_version := 0;

    SELECT u.id
    INTO v_user_asset_account_id
    FROM user_asset_accounts AS u
    JOIN user_asset_balances AS b ON b.user_asset_account_id = u.id
    WHERE u.user_id = p_user_id
      AND u.asset_id = p_asset_id
      AND u.status = 'active';

    IF NOT FOUND THEN
        result_outcome := 'not_found';
        RETURN NEXT;
        RETURN;
    END IF;

    -- The exception block is a subtransaction. Raising the private R0010
    -- condition rolls back the inserted journal when the balance update fails,
    -- while allowing the function to return a typed business outcome.
    BEGIN
        INSERT INTO ledger_journals(
            id, reference_type, reference_id, journal_type, source_system,
            source_event_id, correlation_id, causation_id, description,
            metadata, occurred_at
        ) VALUES (
            p_journal_id, 'order', p_order_id, 'trading_funds_reserved',
            'order-service', p_command_id, NULLIF(p_correlation_id, ''),
            NULLIF(p_causation_id, ''), 'reserve funds for order',
            'null'::jsonb, p_occurred_at
        )
        ON CONFLICT DO NOTHING
        RETURNING id INTO v_inserted_journal_id;

        IF v_inserted_journal_id IS NULL THEN
            SELECT j.reference_id, j.journal_type
            INTO STRICT v_existing_reference_id, v_existing_journal_type
            FROM ledger_journals AS j
            WHERE j.source_system = 'order-service'
              AND j.source_event_id = p_command_id;

            IF v_existing_reference_id <> p_order_id
               OR v_existing_journal_type <> 'trading_funds_reserved' THEN
                result_outcome := 'conflict';
                RETURN NEXT;
                RETURN;
            END IF;

            SELECT r.id, r.order_id, r.status, r.original_atomic::text,
                   r.remaining_atomic::text, b.version
            INTO STRICT result_reservation_id, result_order_id, result_status,
                        result_original_atomic, result_remaining_atomic,
                        result_balance_version
            FROM fund_reservations AS r
            JOIN user_asset_balances AS b
              ON b.user_asset_account_id = r.user_asset_account_id
            WHERE r.order_id = p_order_id;

            result_outcome := 'replay';
            RETURN NEXT;
            RETURN;
        END IF;

        UPDATE user_asset_balances
        SET available_atomic = available_atomic - p_amount_atomic,
            reserved_atomic = reserved_atomic + p_amount_atomic,
            version = version + 1,
            updated_at = now()
        WHERE user_asset_account_id = v_user_asset_account_id
          AND available_atomic >= p_amount_atomic
        RETURNING version INTO result_balance_version;

        IF NOT FOUND THEN
            RAISE EXCEPTION USING
                ERRCODE = 'R0010',
                MESSAGE = 'insufficient funds';
        END IF;

        INSERT INTO fund_reservations(
            id, order_id, user_asset_account_id, asset_id, original_atomic,
            remaining_atomic, status, created_at, updated_at
        ) VALUES (
            p_reservation_id, p_order_id, v_user_asset_account_id, p_asset_id,
            p_amount_atomic, p_amount_atomic, 'active', now(), now()
        );

        INSERT INTO ledger_entries(
            id, journal_id, ledger_account_id, user_asset_account_id, asset_id,
            bucket, side, amount_atomic
        ) VALUES
            (p_journal_id || ':available', p_journal_id,
             'account_customer_available', v_user_asset_account_id, p_asset_id,
             'available', 'debit', p_amount_atomic),
            (p_journal_id || ':reserved', p_journal_id,
             'account_customer_reserved', v_user_asset_account_id, p_asset_id,
             'reserved', 'credit', p_amount_atomic);

        INSERT INTO outbox_events(
            id, topic, event_type, aggregate_id, message_key, payload,
            occurred_at
        ) VALUES (
            p_event_id, 'ledger-events', 'FundsReserved', p_order_id,
            v_user_asset_account_id, p_outbox_payload, now()
        );

        result_outcome := 'reserved';
        result_reservation_id := p_reservation_id;
        result_order_id := p_order_id;
        result_status := 'active';
        result_original_atomic := p_amount_atomic::text;
        result_remaining_atomic := p_amount_atomic::text;
        RETURN NEXT;
        RETURN;
    EXCEPTION
        WHEN SQLSTATE 'R0010' THEN
            result_outcome := 'insufficient_funds';
            result_reservation_id := '';
            result_order_id := '';
            result_status := '';
            result_original_atomic := '';
            result_remaining_atomic := '';
            result_balance_version := 0;
            RETURN NEXT;
            RETURN;
    END;
END $$;
