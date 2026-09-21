-- Run with psql as the RDS admin after both one-off migration tasks succeed.
-- Passwords are prompted interactively; store the same values in two separate
-- Secrets Manager secrets whose ARNs are passed to Terraform.
\set ON_ERROR_STOP on
\prompt 'Ledger runtime password: ' ledger_password
\prompt 'Outbox runtime password: ' outbox_password

SELECT 'CREATE ROLE ledger_runtime LOGIN'
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ledger_runtime') \gexec
SELECT 'CREATE ROLE outbox_runtime LOGIN'
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'outbox_runtime') \gexec

SELECT format('ALTER ROLE ledger_runtime LOGIN PASSWORD %L', :'ledger_password') \gexec
SELECT format('ALTER ROLE outbox_runtime LOGIN PASSWORD %L', :'outbox_password') \gexec
ALTER ROLE ledger_runtime NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
ALTER ROLE outbox_runtime NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
\unset ledger_password
\unset outbox_password

SELECT format('GRANT CONNECT ON DATABASE %I TO ledger_runtime, outbox_runtime', current_database()) \gexec
GRANT USAGE ON SCHEMA public TO ledger_runtime, outbox_runtime;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;

-- Ledger needs DML and sequence usage, but not CREATE/ALTER/DROP privileges.
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO ledger_runtime;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO ledger_runtime;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE ON TABLES TO ledger_runtime;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO ledger_runtime;

-- The relay reads and updates only committed outbox rows and verifies its own
-- migration record; it cannot insert ledger entries or alter the schema.
GRANT SELECT, UPDATE ON outbox_events TO outbox_runtime;
GRANT SELECT ON outbox_relay_schema_migrations TO outbox_runtime;
