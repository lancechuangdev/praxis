\set ON_ERROR_STOP on

BEGIN;

TRUNCATE TABLE
    ledger_entries,
    fund_reservations,
    ledger_journals,
    user_asset_balances,
    user_asset_accounts,
    ledger_engine_offsets,
    inbox_events,
    outbox_events
RESTART IDENTITY;

COMMIT;
