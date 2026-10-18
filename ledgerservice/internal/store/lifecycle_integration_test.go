package store_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
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

func TestLedgerLifecycleTradeAndCancellationFacts(t *testing.T) {
	for _, selfTrade := range []bool{false, true} {
		t.Run(fmt.Sprintf("self_trade_%v", selfTrade), func(t *testing.T) {
			s, db, prefix := lifecycleStore(t)
			ctx := context.Background()
			buyer, seller := prefix+"-buyer", prefix+"-seller"
			if selfTrade {
				seller = buyer
			}
			custody := prefix + "-custody"
			if _, err := db.Exec(ctx, `INSERT INTO networks(id,code,name,native_asset_id) VALUES($1,$1,'Lifecycle test','asset_eth')`, prefix+"-chain"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(ctx, `INSERT INTO asset_networks(id,asset_id,network_id,token_standard,contract_decimals,required_confirmations) VALUES($1,'asset_eth',$2,'NATIVE',18,12)`, prefix+"-network", prefix+"-chain"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(ctx, `INSERT INTO custody_positions(id,asset_network_id,wallet_id,address,wallet_purpose) VALUES($1,$2,$3,$3,'deposit')`, custody, prefix+"-network", prefix+"-wallet"); err != nil {
				t.Fatal(err)
			}
			for _, deposit := range []ledger.PostDeposit{
				{CommandID: prefix + "-quote", DepositID: prefix + "-quote", UserID: buyer, AssetID: "asset_usdt", CustodyPositionID: "custody_position_alice_eth_usdt", AmountAtomic: "1000", TargetBucket: "available", SourceSystem: "deposit-service"},
				{CommandID: prefix + "-base", DepositID: prefix + "-base", UserID: seller, AssetID: "asset_eth", CustodyPositionID: custody, AmountAtomic: "100", TargetBucket: "available", SourceSystem: "deposit-service"},
			} {
				if _, err := s.PostDeposit(ctx, deposit); err != nil {
					t.Fatal(err)
				}
			}
			for _, reserve := range []ledger.ReserveOrder{
				{CommandID: prefix + "-reserve-buy", OrderID: prefix + "-buy", UserID: buyer, AssetID: "asset_usdt", AmountAtomic: "500"},
				{CommandID: prefix + "-reserve-sell", OrderID: prefix + "-sell", UserID: seller, AssetID: "asset_eth", AmountAtomic: "50"},
			} {
				if _, err := s.ReserveForOrder(ctx, reserve); err != nil {
					t.Fatal(err)
				}
			}
			trade := ledger.TradeExecuted{EventID: prefix + "-trade", TradeID: prefix + "-trade", EngineID: prefix + "-engine", Symbol: "ETH-USDT", SequenceNumber: 1, BuyerOrderID: prefix + "-buy", SellerOrderID: prefix + "-sell", BuyerUserID: buyer, SellerUserID: seller, BaseAssetID: "asset_eth", QuoteAssetID: "asset_usdt", BaseAmountAtomic: "10", QuoteAmountAtomic: "100", BuyerFeeQuoteAtomic: "1", SellerFeeBaseAtomic: "1", OccurredAt: time.Now().UTC()}
			inboxCtx := ledger.WithInboxMetadata(ctx, ledger.InboxMetadata{ConsumerName: prefix, EventID: trade.EventID, EventType: "TradeExecuted"})
			result, err := s.BookTrade(inboxCtx, trade)
			if err != nil {
				t.Fatal(err)
			}
			if replay, err := s.BookTrade(inboxCtx, trade); err != nil || !replay.Replay {
				t.Fatalf("trade replay=%+v err=%v", replay, err)
			}
			var payload []byte
			if err := db.QueryRow(ctx, `SELECT payload FROM ledger_journal_outbox WHERE journal_id=$1`, result.JournalID).Scan(&payload); err != nil {
				t.Fatal(err)
			}
			event, err := lifecycle.Decode(payload)
			if err != nil {
				t.Fatal(err)
			}
			journal := event.GetLedgerJournal()
			wantAccounts := 4
			if selfTrade {
				wantAccounts = 2
			}
			if len(journal.Entries) != 6 || len(journal.Balances) != wantAccounts {
				t.Fatalf("entries=%d accounts=%d", len(journal.Entries), len(journal.Balances))
			}
			for _, balance := range journal.Balances {
				if selfTrade && balance.Version-balance.PreviousVersion != 2 {
					t.Fatal("lost multiple updates to the same account")
				}
			}
			cancel := ledger.CancelOrder{EventID: prefix + "-cancel", OrderID: prefix + "-buy", EngineID: trade.EngineID, Symbol: trade.Symbol, SequenceNumber: 2}
			cancelled, err := s.CancelOrder(ctx, cancel)
			if err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(ctx, `SELECT payload FROM ledger_journal_outbox WHERE journal_id=$1`, cancelled.JournalID).Scan(&payload); err != nil {
				t.Fatal(err)
			}
			if _, err := lifecycle.Decode(payload); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(ctx, `UPDATE ledger_journal_outbox SET payload=payload WHERE journal_id=$1`, result.JournalID); err == nil {
				t.Fatal("immutable source fact was mutable")
			}
		})
	}
}

func TestLedgerLifecycleConcurrentAccountVersionChains(t *testing.T) {
	_, base, prefix := lifecycleStore(t)
	ctx := context.Background()
	cfg := base.Config()
	cfg.MaxConns = 4
	db, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := store.New(db, db)
	s.LifecycleCapture = true
	var group sync.WaitGroup
	failures := make(chan error, 4)
	for i := range 4 {
		group.Add(1)
		go func() {
			defer group.Done()
			_, err := s.PostDeposit(ctx, ledger.PostDeposit{CommandID: fmt.Sprintf("%s-%d", prefix, i), DepositID: fmt.Sprintf("%s-%d", prefix, i), UserID: prefix, AssetID: "asset_usdt", CustodyPositionID: "custody_position_alice_eth_usdt", AmountAtomic: "100", TargetBucket: "available", SourceSystem: "deposit-service"})
			failures <- err
		}()
	}
	group.Wait()
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	rows, err := db.Query(ctx, `SELECT o.payload FROM ledger_journal_outbox o JOIN ledger_journals j ON j.id=o.journal_id WHERE j.source_event_id LIKE $1`, prefix+"%")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	versions := map[uint64]bool{}
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			t.Fatal(err)
		}
		event, err := lifecycle.Decode(payload)
		if err != nil {
			t.Fatal(err)
		}
		b := event.GetLedgerJournal().Balances[0]
		if b.Version != b.PreviousVersion+1 || versions[b.Version] {
			t.Fatal("conflicting account version chain")
		}
		versions[b.Version] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(versions) != 4 || !versions[1] || !versions[4] {
		t.Fatal(versions)
	}
}

func TestLedgerLifecycleDisabledCaptureAndWriterMismatch(t *testing.T) {
	_, captured, prefix := lifecycleStore(t)
	ctx := context.Background()
	cfg := captured.Config()
	cfg.ConnConfig.RuntimeParams["praxis.ledger_lifecycle_capture"] = "off"
	db, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := store.New(db, db)
	deposit := ledger.PostDeposit{CommandID: prefix, DepositID: prefix, UserID: prefix, AssetID: "asset_usdt", CustodyPositionID: "custody_position_alice_eth_usdt", AmountAtomic: "100", TargetBucket: "available", SourceSystem: "deposit-service"}
	result, err := s.PostDeposit(ctx, deposit)
	if err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM ledger_journal_outbox WHERE journal_id=$1`, result.JournalID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("disabled capture count=%d err=%v", count, err)
	}
	s.LifecycleCapture = true
	deposit.CommandID += "-mismatch"
	deposit.DepositID = deposit.CommandID
	if _, err := s.PostDeposit(ctx, deposit); err == nil {
		t.Fatal("mismatched writer configuration committed")
	}
	if err := db.QueryRow(ctx, `SELECT count(*) FROM ledger_journals WHERE source_event_id=$1`, deposit.CommandID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("mismatch journal count=%d err=%v", count, err)
	}
}
