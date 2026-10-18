package store_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	lifecycle "praxis/api/events/lifecycle/v1"
	"praxis/ledgerservice/internal/ledger"
	"praxis/ledgerservice/internal/store"
	"praxis/ledgerservice/migrations"
)

func lifecycleStore(t *testing.T) (*store.Postgres, *pgxpool.Pool, string) {
	t.Helper()
	url := os.Getenv("LEDGER_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("LEDGER_TEST_DATABASE_URL is not set")
	}
	db, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := migrations.Apply(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 1
	cfg.ConnConfig.RuntimeParams["praxis.ledger_lifecycle_capture"] = "on"
	captured, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(captured.Close)
	s := store.New(captured, captured)
	s.LifecycleCapture = true
	return s, captured, fmt.Sprintf("lifecycle-it-%d", time.Now().UnixNano())
}

func TestLedgerLifecycleAtomicDepositHoldReservationAndRelease(t *testing.T) {
	s, db, prefix := lifecycleStore(t)
	ctx := context.Background()
	deposit := ledger.PostDeposit{CommandID: prefix + "-deposit", DepositID: prefix + "-deposit", UserID: prefix, AssetID: "asset_usdt", CustodyPositionID: "custody_position_alice_eth_usdt", AmountAtomic: "9007199254740993", TargetBucket: "hold", SourceSystem: "deposit-service", OccurredAt: time.Now().UTC()}
	result, err := s.PostDeposit(ctx, deposit)
	if err != nil {
		t.Fatal(err)
	}
	if replay, err := s.PostDeposit(ctx, deposit); err != nil || !replay.Replay || replay.JournalID != result.JournalID {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	if _, err := s.ReleaseHold(ctx, ledger.ReleaseHold{CommandID: prefix + "-hold", DepositID: deposit.DepositID, UserID: prefix, AssetID: "asset_usdt", SourceSystem: "deposit-service", AmountAtomic: "100", OccurredAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	reserve := ledger.ReserveOrder{CommandID: prefix + "-reserve", OrderID: prefix + "-order", UserID: prefix, AssetID: "asset_usdt", AmountAtomic: "50", OccurredAt: time.Now().UTC()}
	if _, err := s.ReserveForOrder(ctx, reserve); err != nil {
		t.Fatal(err)
	}
	if replay, err := s.ReserveForOrder(ctx, reserve); err != nil || !replay.Replay {
		t.Fatalf("reserve replay=%+v err=%v", replay, err)
	}
	if _, err := s.ReleaseOrderReservation(ctx, ledger.ReleaseOrderReservation{CommandID: prefix + "-release", OrderID: reserve.OrderID, Reason: "execution_complete"}); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(ctx, `SELECT o.payload FROM ledger_journal_outbox o JOIN ledger_journals j ON j.id=o.journal_id WHERE j.source_event_id LIKE $1 ORDER BY o.created_at`, prefix+"%")
	if err != nil {
		t.Fatal(err)
	}
	versions := map[uint64]bool{}
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			t.Fatal(err)
		}
		event, err := lifecycle.Decode(raw)
		if err != nil {
			t.Fatal(err)
		}
		journal := event.GetLedgerJournal()
		if len(journal.Entries) != 2 || len(journal.Balances) != 1 {
			t.Fatal("incomplete journal dimensions")
		}
		balance := journal.Balances[0]
		if balance.PreviousVersion+1 != balance.Version || balance.UserId != prefix {
			t.Fatal("incorrect customer version chain")
		}
		versions[balance.Version] = true
		if balance.Version == 1 && journal.Entries[0].AmountAtomic != "9007199254740993" {
			t.Fatal("lost exact quantity")
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	if len(versions) != 4 || !versions[1] || !versions[2] || !versions[3] || !versions[4] {
		t.Fatalf("versions=%v", versions)
	}
	bad := deposit
	bad.CommandID = prefix + "-invalid"
	bad.DepositID = bad.CommandID
	bad.CorrelationID = strings.Repeat("x", 257)
	if _, err := s.PostDeposit(ctx, bad); err == nil {
		t.Fatal("invalid lifecycle metadata committed")
	}
	var count int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM ledger_journals WHERE source_event_id=$1`, bad.CommandID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("failed capture retained journal count=%d err=%v", count, err)
	}
	balance, err := s.GetBalance(ctx, prefix, "asset_usdt")
	if err != nil || balance.Version != 4 {
		t.Fatalf("failed capture mutated balance: %+v err=%v", balance, err)
	}
}

func TestLedgerLifecycleRejectsUncapturedAndMultipleJournals(t *testing.T) {
	_, db, prefix := lifecycleStore(t)
	ctx := context.Background()
	insert := `INSERT INTO ledger_journals(id,reference_type,reference_id,journal_type,source_system,source_event_id,occurred_at) VALUES($1,'test',$1,'test','test',$1,now())`
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, insert, prefix+"-missing"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err == nil {
		t.Fatal("uncaptured journal committed")
	}
	tx, err = db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, insert, prefix+"-first"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, insert, prefix+"-second"); err == nil {
		t.Fatal("ambiguous multi-journal transaction accepted")
	}
	_ = tx.Rollback(ctx)
}
