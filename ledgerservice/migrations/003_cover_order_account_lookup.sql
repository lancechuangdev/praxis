-- ReserveForOrder resolves an active account by (user_id, asset_id) and needs
-- id and status. Accounts are stable during order admission, so this covering
-- index lets PostgreSQL satisfy that lookup without visiting the heap once the
-- visibility map is current. The unique constraint remains authoritative.
CREATE INDEX user_asset_accounts_order_lookup_idx
    ON user_asset_accounts (user_id, asset_id)
    INCLUDE (id, status);
