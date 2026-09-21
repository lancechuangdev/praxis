package migrations

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestApplyVerifyAndConcurrentRunners(t *testing.T) {
	url := os.Getenv("LEDGER_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("LEDGER_TEST_DATABASE_URL is not set")
	}
	db, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- Apply(context.Background(), db)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := Verify(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	var original string
	if err := db.QueryRow(context.Background(), `SELECT checksum FROM ledger_schema_migrations WHERE version='001_init.sql'`).Scan(&original); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(context.Background(), `UPDATE ledger_schema_migrations SET checksum='invalid' WHERE version='001_init.sql'`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = db.Exec(context.Background(), `UPDATE ledger_schema_migrations SET checksum=$1 WHERE version='001_init.sql'`, original)
	}()
	if err := Verify(context.Background(), db); err == nil {
		t.Fatal("Verify accepted a changed checksum")
	}
	if err := Apply(context.Background(), db); err == nil {
		t.Fatal("Apply accepted a changed checksum")
	}
	if _, err := db.Exec(context.Background(), `UPDATE ledger_schema_migrations SET checksum=$1 WHERE version='001_init.sql'`, original); err != nil {
		t.Fatal(err)
	}

	conn, err := db.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	const lockID int64 = 6417757361906963521
	if _, err := conn.Exec(context.Background(), `SELECT pg_advisory_lock($1)`, lockID); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := Apply(ctx, db); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Apply while locked = %v; want deadline exceeded", err)
	}
	if _, err := conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, lockID); err != nil {
		t.Fatal(err)
	}
	if err := Apply(context.Background(), db); err != nil {
		t.Fatal(err)
	}
}
