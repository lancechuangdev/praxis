package store_test

import (
	"context"
	"os"
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
