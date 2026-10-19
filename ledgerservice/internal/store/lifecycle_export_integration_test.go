package store_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	lifecycle "praxis/api/events/lifecycle/v1"
	"praxis/ledgerservice/internal/ledger"
	"praxis/ledgerservice/internal/store"
)

func TestJournalExportChecksAuthoritativeBalancesAndCaptureGaps(t *testing.T) {
	s, db, prefix := journalDispatchStore(t)
	ctx := context.Background()
	deposit := ledger.PostDeposit{CommandID: prefix, DepositID: prefix, UserID: prefix, AssetID: "asset_usdt", CustodyPositionID: "custody_position_alice_eth_usdt", AmountAtomic: "9007199254740993", TargetBucket: "available", SourceSystem: "deposit-service"}
	if _, err := s.PostDeposit(ctx, deposit); err != nil {
		t.Fatal(err)
	}
	var export bytes.Buffer
	if err := store.ExportJournals(ctx, db, &export); err != nil {
		t.Fatal(err)
	}
	var record struct {
		Topic   string `json:"topic"`
		Payload []byte `json:"payload"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(export.Bytes()), &record); err != nil {
		t.Fatal(err)
	}
	e, err := lifecycle.Decode(record.Payload)
	if err != nil || record.Topic != "ledger.journals.v1" || e.GetLedgerJournal().Balances[0].AvailableAtomic != "9007199254740993" {
		t.Fatal(e, err)
	}
	cfg := db.Config()
	cfg.ConnConfig.RuntimeParams["praxis.ledger_lifecycle_capture"] = "off"
	off, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer off.Close()
	s = store.New(off, off)
	deposit.CommandID += "-gap"
	deposit.DepositID = deposit.CommandID
	deposit.AmountAtomic = "1"
	if _, err := s.PostDeposit(ctx, deposit); err != nil {
		t.Fatal(err)
	}
	export.Reset()
	if err := store.ExportJournals(ctx, db, &export); err == nil {
		t.Fatal("capture gap passed authoritative export")
	}
}

func TestJournalExportUsesAccountDependenciesInsteadOfJournalIDs(t *testing.T) {
	s, db, prefix := journalDispatchStore(t)
	ctx := context.Background()
	for i := 0; i < 8; i++ {
		id := fmt.Sprintf("%s-%d", prefix, i)
		if _, err := s.PostDeposit(ctx, ledger.PostDeposit{CommandID: id, DepositID: id, UserID: prefix, AssetID: "asset_usdt", CustodyPositionID: "custody_position_alice_eth_usdt", AmountAtomic: "1", TargetBucket: "available", SourceSystem: "deposit-service"}); err != nil {
			t.Fatal(err)
		}
	}
	var output bytes.Buffer
	if err := store.ExportJournals(ctx, db, &output); err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(&output)
	version := uint64(0)
	for decoder.More() {
		var record struct {
			Payload []byte `json:"payload"`
		}
		if err := decoder.Decode(&record); err != nil {
			t.Fatal(err)
		}
		event, err := lifecycle.Decode(record.Payload)
		if err != nil {
			t.Fatal(err)
		}
		b := event.GetLedgerJournal().Balances[0]
		if b.PreviousVersion != version || b.Version != version+1 {
			t.Fatal("export requires unbounded reordering", version, b)
		}
		version = b.Version
	}
	if version != 8 {
		t.Fatal(version)
	}
}

func TestJournalCaptureConcurrentReplayOnExistingAccount(t *testing.T) {
	s, db, prefix := journalDispatchStore(t)
	ctx := context.Background()
	seed := ledger.PostDeposit{CommandID: prefix, DepositID: prefix, UserID: prefix, AssetID: "asset_usdt", CustodyPositionID: "custody_position_alice_eth_usdt", AmountAtomic: "100", TargetBucket: "available", SourceSystem: "deposit-service"}
	if _, err := s.PostDeposit(ctx, seed); err != nil {
		t.Fatal(err)
	}
	command := seed
	command.CommandID += "-replay"
	command.DepositID = command.CommandID
	command.AmountAtomic = "10"
	results := make(chan ledger.OperationResult, 8)
	failures := make(chan error, 8)
	var group sync.WaitGroup
	for range 8 {
		group.Add(1)
		go func() {
			defer group.Done()
			result, err := s.PostDeposit(ctx, command)
			results <- result
			failures <- err
		}()
	}
	group.Wait()
	close(results)
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	created := 0
	for result := range results {
		if !result.Replay {
			created++
		}
	}
	if created != 1 {
		t.Fatal("replay created multiple journals", created)
	}
	var version int64
	var available string
	if err := db.QueryRow(ctx, `SELECT b.version,b.available_atomic::text FROM user_asset_balances b JOIN user_asset_accounts a ON a.id=b.user_asset_account_id WHERE a.user_id=$1 AND a.asset_id='asset_usdt'`, prefix).Scan(&version, &available); err != nil || version != 2 || available != "110" {
		t.Fatal(version, available, err)
	}
}
