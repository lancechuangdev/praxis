\if :{?user_count}
\else
\set user_count 10000
\endif

\if :{?available_atomic}
\else
\set available_atomic 1000000000
\endif

BEGIN;

WITH generated_users AS (
    SELECT
        'load-user-' || lpad(n::text, 6, '0') AS user_id,
        'load-uaa-' || lpad(n::text, 6, '0') AS account_id
    FROM generate_series(1, :user_count::integer) AS n
)
INSERT INTO user_asset_accounts (id, user_id, asset_id)
SELECT account_id, user_id, 'asset_usdt'
FROM generated_users
ON CONFLICT (user_id, asset_id) DO NOTHING;

WITH generated_users AS (
    SELECT 'load-user-' || lpad(n::text, 6, '0') AS user_id
    FROM generate_series(1, :user_count::integer) AS n
)
INSERT INTO user_asset_balances (user_asset_account_id, available_atomic)
SELECT account.id, :'available_atomic'::numeric
FROM generated_users generated
JOIN user_asset_accounts account
  ON account.user_id = generated.user_id
 AND account.asset_id = 'asset_usdt'
ON CONFLICT (user_asset_account_id) DO NOTHING;

COMMIT;

