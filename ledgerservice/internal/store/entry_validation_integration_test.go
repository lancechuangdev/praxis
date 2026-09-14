package store_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"praxis/ledgerservice/migrations"
)

func TestLedgerEntryStatementValidation(t *testing.T) {
	url := os.Getenv("LEDGER_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("LEDGER_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	db, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = migrations.Apply(ctx, db); err != nil {
		t.Fatal(err)
	}

	prefix := fmt.Sprintf("phase6-%d", time.Now().UnixNano())
	accountID := prefix + "-account"
	if _, err = db.Exec(ctx, `INSERT INTO user_asset_accounts(id,user_id,asset_id) VALUES($1,$2,'asset_usdt')`, accountID, prefix+"-user"); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name      string
		journal   string
		entrySQL  string
		arguments []any
	}{
		{
			name:      "one invalid user row rejects whole multi-row statement",
			journal:   prefix + "-user-journal",
			entrySQL:  `INSERT INTO ledger_entries(id,journal_id,ledger_account_id,user_asset_account_id,asset_id,bucket,side,amount_atomic) VALUES ($1||'-valid',$1,'account_customer_available',$2,'asset_usdt','available','debit',1), ($1||'-invalid',$1,'account_customer_reserved',$2,'asset_usdt','available','credit',1)`,
			arguments: []any{prefix + "-user-journal", accountID},
		},
		{
			name:      "invalid custody dimensions",
			journal:   prefix + "-custody-journal",
			entrySQL:  `INSERT INTO ledger_entries(id,journal_id,ledger_account_id,asset_id,custody_position_id,side,amount_atomic) VALUES ($1||'-invalid',$1,'account_customer_available','asset_usdt','custody_position_alice_eth_usdt','debit',1)`,
			arguments: []any{prefix + "-custody-journal"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, createErr := db.Exec(ctx, `INSERT INTO ledger_journals(id,reference_type,reference_id,journal_type,source_system,source_event_id,occurred_at) VALUES($1,'phase6',$1,'validation_test','phase6',$1,now())`, test.journal); createErr != nil {
				t.Fatal(createErr)
			}
			_, insertErr := db.Exec(ctx, test.entrySQL, test.arguments...)
			var pgErr *pgconn.PgError
			if !errors.As(insertErr, &pgErr) || pgErr.Code != "23514" {
				t.Fatalf("expected check violation, got %v", insertErr)
			}
			var count int
			if countErr := db.QueryRow(ctx, `SELECT count(*) FROM ledger_entries WHERE journal_id=$1`, test.journal).Scan(&count); countErr != nil || count != 0 {
				t.Fatalf("persisted entries=%d err=%v", count, countErr)
			}
		})
	}
}
