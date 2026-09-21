package migrations

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// BootstrapRuntimeRoles runs after the Ledger and Outbox schemas are migrated.
// Passwords come from ECS-injected Secrets Manager values, never Terraform state.
func BootstrapRuntimeRoles(ctx context.Context, db *pgxpool.Pool, ledgerPassword, outboxPassword string) error {
	if ledgerPassword == "" || outboxPassword == "" {
		return errors.New("both runtime passwords are required")
	}
	if ledgerPassword == outboxPassword {
		return errors.New("Ledger and Outbox must have distinct runtime passwords")
	}
	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin runtime role bootstrap: %w", err)
	}
	defer tx.Rollback(ctx)
	// Serialize repeated migration tasks. This is the same lock as both schema runners.
	const lockID int64 = 6417757361906963521
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, lockID); err != nil {
		return fmt.Errorf("lock runtime role bootstrap: %w", err)
	}
	var ready bool
	if err := tx.QueryRow(ctx, `SELECT to_regclass('public.ledger_schema_migrations') IS NOT NULL AND to_regclass('public.outbox_relay_schema_migrations') IS NOT NULL AND to_regclass('public.outbox_events') IS NOT NULL`).Scan(&ready); err != nil {
		return fmt.Errorf("check migration schemas: %w", err)
	}
	if !ready {
		return errors.New("Ledger and Outbox migrations must finish before runtime role bootstrap")
	}
	for _, role := range []struct{ name, password string }{
		{"ledger_runtime", ledgerPassword},
		{"outbox_runtime", outboxPassword},
	} {
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=$1)`, role.name).Scan(&exists); err != nil {
			return fmt.Errorf("check %s role: %w", role.name, err)
		}
		if !exists {
			if _, err := tx.Exec(ctx, `CREATE ROLE `+role.name+` LOGIN`); err != nil {
				return fmt.Errorf("create %s role: %w", role.name, err)
			}
		}
		// PostgreSQL does not accept a bind parameter in ALTER ROLE PASSWORD.
		// Let PostgreSQL quote the literal, and never include the resulting SQL in errors.
		var passwordSQL string
		if err := tx.QueryRow(ctx, `SELECT format('ALTER ROLE %I LOGIN PASSWORD %L', $1::text, $2::text)`, role.name, role.password).Scan(&passwordSQL); err != nil {
			return fmt.Errorf("prepare %s password: %w", role.name, err)
		}
		if _, err := tx.Exec(ctx, passwordSQL); err != nil {
			return fmt.Errorf("set %s password: %w", role.name, err)
		}
		if _, err := tx.Exec(ctx, `ALTER ROLE `+role.name+` NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION`); err != nil {
			return fmt.Errorf("restrict %s role: %w", role.name, err)
		}
	}
	statements := []string{
		`GRANT CONNECT ON DATABASE ` + pgx.Identifier{db.Config().ConnConfig.Database}.Sanitize() + ` TO ledger_runtime, outbox_runtime`,
		`GRANT USAGE ON SCHEMA public TO ledger_runtime, outbox_runtime`,
		`REVOKE CREATE ON SCHEMA public FROM PUBLIC`,
		`GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO ledger_runtime`,
		`GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO ledger_runtime`,
		`ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE ON TABLES TO ledger_runtime`,
		`ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO ledger_runtime`,
		`GRANT SELECT, UPDATE ON outbox_events TO outbox_runtime`,
		`GRANT SELECT ON outbox_relay_schema_migrations TO outbox_runtime`,
	}
	for _, statement := range statements {
		if _, err := tx.Exec(ctx, statement); err != nil {
			return fmt.Errorf("grant runtime privileges: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit runtime role bootstrap: %w", err)
	}
	return nil
}
