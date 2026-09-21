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
	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS outbox_relay_schema_migrations(version TEXT PRIMARY KEY,checksum TEXT NOT NULL,applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	names, err := fs.Glob(files, "*.sql")
	if err != nil {
		return err
	}
	sort.Strings(names)
	for _, version := range names {
		body, err := files.ReadFile(version)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(body)
		checksum := hex.EncodeToString(sum[:])
		var previous string
		err = conn.QueryRow(ctx, `SELECT checksum FROM outbox_relay_schema_migrations WHERE version=$1`, version).Scan(&previous)
		if err == nil {
			if previous != checksum {
				return fmt.Errorf("migration %s checksum changed", version)
			}
			continue
		}
		if err != pgx.ErrNoRows {
			return err
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, string(body)); err == nil {
			_, err = tx.Exec(ctx, `INSERT INTO outbox_relay_schema_migrations(version,checksum) VALUES($1,$2)`, version, checksum)
		}
		if err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("apply %s: %w", version, err)
		}
		if err = tx.Commit(ctx); err != nil {
			return err
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
		err = db.QueryRow(ctx, `SELECT checksum FROM outbox_relay_schema_migrations WHERE version=$1`, version).Scan(&got)
		if err == pgx.ErrNoRows {
			return fmt.Errorf("outbox migration %s is not applied", version)
		}
		if err != nil {
			return fmt.Errorf("verify outbox migration %s: %w", version, err)
		}
		if got != hex.EncodeToString(want[:]) {
			return fmt.Errorf("outbox migration %s checksum changed", version)
		}
	}
	return nil
}
