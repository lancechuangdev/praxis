package migrations

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed *.sql
var files embed.FS

func Apply(ctx context.Context, db *pgxpool.Pool) error {
	conn, err := db.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	// Ledger and Outbox share an outbox table, so both runners use the same lock.
	const lockID int64 = 6417757361906963521
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, lockID); err != nil {
		return err
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var unlocked bool
		if err := conn.QueryRow(unlockCtx, `SELECT pg_advisory_unlock($1)`, lockID).Scan(&unlocked); err != nil || !unlocked {
			_ = conn.Hijack().Close(context.Background())
		}
	}()
	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS ledger_schema_migrations(version TEXT PRIMARY KEY,checksum TEXT NOT NULL,applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	names, err := fs.Glob(files, "*.sql")
	if err != nil {
		return err
	}
	sort.Strings(names)
	for _, version := range names {
		body, e := files.ReadFile(version)
		if e != nil {
			return e
		}
		sum := sha256.Sum256(body)
		checksum := hex.EncodeToString(sum[:])
		var old string
		e = conn.QueryRow(ctx, `SELECT checksum FROM ledger_schema_migrations WHERE version=$1`, version).Scan(&old)
		if e == nil {
			if old != checksum {
				return fmt.Errorf("migration %s checksum changed", version)
			}
			continue
		}
		if e != pgx.ErrNoRows {
			return e
		}
		tx, e := conn.Begin(ctx)
		if e != nil {
			return e
		}
		if _, e = tx.Exec(ctx, string(body)); e == nil {
			_, e = tx.Exec(ctx, `INSERT INTO ledger_schema_migrations(version,checksum) VALUES($1,$2)`, version, checksum)
		}
		if e != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("apply %s: %w", version, e)
		}
		if e = tx.Commit(ctx); e != nil {
			return e
		}
	}
	return nil
}

// Verify ensures the database has every migration embedded in this image.
func Verify(ctx context.Context, db *pgxpool.Pool) error {
	names, err := fs.Glob(files, "*.sql")
	if err != nil {
		return err
	}
	for _, version := range names {
		body, err := files.ReadFile(version)
		if err != nil {
			return err
		}
		want := sha256.Sum256(body)
		var got string
		err = db.QueryRow(ctx, `SELECT checksum FROM ledger_schema_migrations WHERE version=$1`, version).Scan(&got)
		if err == pgx.ErrNoRows {
			return fmt.Errorf("ledger migration %s is not applied", version)
		}
		if err != nil {
			return fmt.Errorf("verify ledger migration %s: %w", version, err)
		}
		if got != hex.EncodeToString(want[:]) {
			return fmt.Errorf("ledger migration %s checksum changed", version)
		}
	}
	return nil
}
