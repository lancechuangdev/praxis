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
	if _, err := db.Exec(ctx, `CREATE TABLE IF NOT EXISTS ledger_schema_migrations(version TEXT PRIMARY KEY,checksum TEXT NOT NULL,applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
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
		e = db.QueryRow(ctx, `SELECT checksum FROM ledger_schema_migrations WHERE version=$1`, version).Scan(&old)
		if e == nil {
			if old != checksum {
				return fmt.Errorf("migration %s checksum changed", version)
			}
			continue
		}
		if e != pgx.ErrNoRows {
			return e
		}
		tx, e := db.Begin(ctx)
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
