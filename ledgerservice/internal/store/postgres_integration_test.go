package store_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"praxis/ledgerservice/internal/ledger"
	"praxis/ledgerservice/internal/store"
	"praxis/ledgerservice/migrations"
)

func TestDepositReservationTradeAndCancellation(t *testing.T) {
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
	s := store.New(db)
	now := time.Now().UTC()
	deposit := ledger.PostDeposit{CommandID: "it-deposit-alice", DepositID: "it-deposit-alice", UserID: "alice-it", AssetID: "asset_usdt", CustodyPositionID: "custody_position_alice_eth_usdt", AmountAtomic: "1000000000", TargetBucket: "available", SourceSystem: "deposit-service", OccurredAt: now}
	if _, err = s.PostDeposit(ctx, deposit); err != nil {
		t.Fatal(err)
	}
	replay, err := s.PostDeposit(ctx, deposit)
	if err != nil || !replay.Replay {
		t.Fatalf("deposit replay: %+v %v", replay, err)
	}
	if _, err = s.ReserveForOrder(ctx, ledger.ReserveOrder{CommandID: "it-reserve-buy", OrderID: "it-buy", UserID: "alice-it", AssetID: "asset_usdt", AmountAtomic: "970000000", OccurredAt: now}); err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(ctx, `INSERT INTO asset_networks(id,asset_id,network_id,token_standard,contract_address,contract_decimals,required_confirmations) VALUES('it-eth-ethereum','asset_eth','network_ethereum','NATIVE',NULL,18,12) ON CONFLICT DO NOTHING;INSERT INTO custody_positions(id,asset_network_id,wallet_id,address,wallet_purpose) VALUES('it-eth-custody','it-eth-ethereum','it-wallet','0xit','deposit') ON CONFLICT DO NOTHING`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.PostDeposit(ctx, ledger.PostDeposit{CommandID: "it-deposit-bob", DepositID: "it-deposit-bob", UserID: "bob-it", AssetID: "asset_eth", CustodyPositionID: "it-eth-custody", AmountAtomic: "1000000000000000000", TargetBucket: "available", SourceSystem: "deposit-service", OccurredAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReserveForOrder(ctx, ledger.ReserveOrder{CommandID: "it-reserve-sell", OrderID: "it-sell", UserID: "bob-it", AssetID: "asset_eth", AmountAtomic: "1000000000000000000", OccurredAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.BookTrade(ctx, ledger.TradeExecuted{EventID: "it-trade-event", TradeID: "it-trade", EngineID: "it-engine", EnginePartition: 0, SequenceNumber: 1, BuyerOrderID: "it-buy", SellerOrderID: "it-sell", BuyerUserID: "alice-it", SellerUserID: "bob-it", BaseAssetID: "asset_eth", QuoteAssetID: "asset_usdt", BaseAmountAtomic: "400000000000000000", QuoteAmountAtomic: "960000000", BuyerFeeQuoteAtomic: "960000", SellerFeeBaseAtomic: "1000000000000000", OccurredAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.CancelOrder(ctx, ledger.CancelOrder{EventID: "it-cancel-buy", OrderID: "it-buy", EngineID: "it-engine", EnginePartition: 0, SequenceNumber: 2, OccurredAt: now}); err != nil {
		t.Fatal(err)
	}
	b, err := s.GetBalance(ctx, "alice-it", "asset_usdt")
	if err != nil {
		t.Fatal(err)
	}
	if b.AvailableAtomic != "39040000" || b.ReservedAtomic != "0" {
		t.Fatalf("unexpected Alice USDT balance: %+v", b)
	}
}

func TestReserveForOrderPhase1(t *testing.T) {
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

	prefix := fmt.Sprintf("phase1-%d", time.Now().UnixNano())
	createAccount := func(suffix string, available int) (string, string) {
		t.Helper()
		userID := prefix + "-user-" + suffix
		accountID := prefix + "-account-" + suffix
		_, createErr := db.Exec(ctx, `INSERT INTO user_asset_accounts(id,user_id,asset_id) VALUES($1,$2,'asset_usdt')`, accountID, userID)
		if createErr == nil {
			_, createErr = db.Exec(ctx, `INSERT INTO user_asset_balances(user_asset_account_id,available_atomic) VALUES($1,$2)`, accountID, available)
		}
		if createErr != nil {
			t.Fatal(createErr)
		}
		return userID, accountID
	}

	s := store.New(db)
	missing := ledger.ReserveOrder{CommandID: prefix + "-missing-command", OrderID: prefix + "-missing-order", UserID: prefix + "-missing-user", AssetID: "asset_usdt", AmountAtomic: "1"}
	if _, err = s.ReserveForOrder(ctx, missing); !errors.Is(err, ledger.ErrNotFound) {
		t.Fatalf("missing account error = %v, want %v", err, ledger.ErrNotFound)
	}
	var missingAccounts int
	if err = db.QueryRow(ctx, `SELECT count(*) FROM user_asset_accounts WHERE user_id=$1`, missing.UserID).Scan(&missingAccounts); err != nil || missingAccounts != 0 {
		t.Fatalf("reservation provisioned missing account: count=%d err=%v", missingAccounts, err)
	}

	userID, accountID := createAccount("sequential", 100)
	insufficient := ledger.ReserveOrder{CommandID: prefix + "-insufficient-command", OrderID: prefix + "-insufficient-order", UserID: userID, AssetID: "asset_usdt", AmountAtomic: "101"}
	if _, err = s.ReserveForOrder(ctx, insufficient); !errors.Is(err, ledger.ErrInsufficientFunds) {
		t.Fatalf("insufficient funds error = %v, want %v", err, ledger.ErrInsufficientFunds)
	}

	request := ledger.ReserveOrder{CommandID: prefix + "-command", OrderID: prefix + "-order", UserID: userID, AssetID: "asset_usdt", AmountAtomic: "40"}
	reservation, err := s.ReserveForOrder(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if reservation.OrderID != request.OrderID || reservation.Status != "active" || reservation.OriginalAtomic != "40" || reservation.RemainingAtomic != "40" || reservation.BalanceVersion != 1 || reservation.Replay {
		t.Fatalf("unexpected reservation: %+v", reservation)
	}

	replay, err := s.ReserveForOrder(ctx, request)
	if err != nil || !replay.Replay || replay.ID != reservation.ID || replay.BalanceVersion != 1 {
		t.Fatalf("unexpected replay: %+v err=%v", replay, err)
	}
	conflict := request
	conflict.OrderID = prefix + "-conflicting-order"
	if _, err = s.ReserveForOrder(ctx, conflict); !errors.Is(err, ledger.ErrConflict) {
		t.Fatalf("idempotency conflict error = %v, want %v", err, ledger.ErrConflict)
	}

	var available, reserved string
	var version int64
	if err = db.QueryRow(ctx, `SELECT available_atomic::text,reserved_atomic::text,version FROM user_asset_balances WHERE user_asset_account_id=$1`, accountID).Scan(&available, &reserved, &version); err != nil {
		t.Fatal(err)
	}
	if available != "60" || reserved != "40" || version != 1 {
		t.Fatalf("unexpected balance: available=%s reserved=%s version=%d", available, reserved, version)
	}
	var entries, outboxEvents int
	var debits, credits string
	if err = db.QueryRow(ctx, `SELECT count(*),COALESCE(sum(e.amount_atomic) FILTER (WHERE e.side='debit'),0)::text,COALESCE(sum(e.amount_atomic) FILTER (WHERE e.side='credit'),0)::text FROM ledger_entries e JOIN ledger_journals j ON j.id=e.journal_id WHERE j.reference_type='order' AND j.reference_id=$1`, request.OrderID).Scan(&entries, &debits, &credits); err != nil {
		t.Fatal(err)
	}
	if entries != 2 || debits != "40" || credits != "40" {
		t.Fatalf("unexpected entries: count=%d debits=%s credits=%s", entries, debits, credits)
	}
	if err = db.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE aggregate_id=$1 AND event_type='FundsReserved'`, request.OrderID).Scan(&outboxEvents); err != nil || outboxEvents != 1 {
		t.Fatalf("unexpected outbox count=%d err=%v", outboxEvents, err)
	}

	concurrentUser, concurrentAccount := createAccount("concurrent", 100)
	const attempts = 20
	var wg sync.WaitGroup
	results := make(chan error, attempts)
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, reserveErr := s.ReserveForOrder(ctx, ledger.ReserveOrder{CommandID: fmt.Sprintf("%s-concurrent-command-%d", prefix, i), OrderID: fmt.Sprintf("%s-concurrent-order-%d", prefix, i), UserID: concurrentUser, AssetID: "asset_usdt", AmountAtomic: "10"})
			results <- reserveErr
		}(i)
	}
	wg.Wait()
	close(results)
	successes, insufficientFunds := 0, 0
	for reserveErr := range results {
		switch {
		case reserveErr == nil:
			successes++
		case errors.Is(reserveErr, ledger.ErrInsufficientFunds):
			insufficientFunds++
		default:
			t.Fatalf("unexpected concurrent reservation error: %v", reserveErr)
		}
	}
	if successes != 10 || insufficientFunds != 10 {
		t.Fatalf("concurrent results: success=%d insufficient=%d", successes, insufficientFunds)
	}
	if err = db.QueryRow(ctx, `SELECT available_atomic::text,reserved_atomic::text,version FROM user_asset_balances WHERE user_asset_account_id=$1`, concurrentAccount).Scan(&available, &reserved, &version); err != nil {
		t.Fatal(err)
	}
	if available != "0" || reserved != "100" || version != 10 {
		t.Fatalf("unexpected concurrent balance: available=%s reserved=%s version=%d", available, reserved, version)
	}
}
