\pset pager off
\echo '== relation statistics =='
SELECT relname AS table_name, n_live_tup, n_dead_tup, vacuum_count,
       autovacuum_count, analyze_count, autoanalyze_count
FROM pg_stat_user_tables
WHERE relname IN ('user_asset_accounts', 'user_asset_balances',
                  'ledger_journals', 'fund_reservations')
ORDER BY relname;

\echo '== index definitions, sizes, and usage =='
SELECT s.relname AS table_name, i.relname AS index_name,
       pg_size_pretty(pg_relation_size(i.oid)) AS index_size,
       s.idx_scan, s.idx_tup_read, s.idx_tup_fetch,
       pg_get_indexdef(i.oid) AS definition
FROM pg_stat_user_indexes s
JOIN pg_class i ON i.oid = s.indexrelid
WHERE s.relname IN ('user_asset_accounts', 'user_asset_balances',
                    'ledger_journals', 'fund_reservations')
ORDER BY s.relname, i.relname;

\echo '== representative account lookup plan =='
SELECT u.user_id AS sample_user_id, u.asset_id AS sample_asset_id
FROM user_asset_accounts u
JOIN user_asset_balances b ON b.user_asset_account_id = u.id
WHERE u.status = 'active'
ORDER BY u.user_id, u.asset_id
LIMIT 1
\gset

EXPLAIN (ANALYZE, BUFFERS, WAL, SETTINGS)
SELECT u.id
FROM user_asset_accounts u
JOIN user_asset_balances b ON b.user_asset_account_id = u.id
WHERE u.user_id = :'sample_user_id'
  AND u.asset_id = :'sample_asset_id'
  AND u.status = 'active';

\echo '== reservation idempotency lookup plan =='
EXPLAIN (COSTS, SETTINGS)
SELECT id, user_asset_account_id, asset_id, original_atomic,
       remaining_atomic, status, version
FROM fund_reservations
WHERE order_id = 'phase4-plan-probe';

\echo '== journal idempotency lookup plan =='
EXPLAIN (COSTS, SETTINGS)
SELECT id
FROM ledger_journals
WHERE source_system = 'order-service'
  AND source_event_id = 'phase4-plan-probe';
