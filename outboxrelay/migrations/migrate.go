package migrations

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed *.sql
var files embed.FS

func Apply(ctx context.Context, db *pgxpool.Pool) error {
	if _, err := db.Exec(ctx, `CREATE TABLE IF NOT EXISTS outbox_relay_schema_migrations(version TEXT PRIMARY KEY,checksum TEXT NOT NULL,applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
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
		err = db.QueryRow(ctx, `SELECT checksum FROM outbox_relay_schema_migrations WHERE version=$1`, version).Scan(&previous)
		if err == nil {
			if previous != checksum {
				return fmt.Errorf("migration %s checksum changed", version)
			}
			continue
		}
		if err != pgx.ErrNoRows {
			return err
		}
		tx, err := db.Begin(ctx)
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
