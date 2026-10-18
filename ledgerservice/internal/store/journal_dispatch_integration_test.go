package store_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"praxis/ledgerservice/internal/ledger"
	"praxis/ledgerservice/internal/store"
	"praxis/ledgerservice/migrations"
)

type journalPublisherFunc func(context.Context, store.JournalPublication) error

func (f journalPublisherFunc) PublishJournal(ctx context.Context, e store.JournalPublication) error {
	return f(ctx, e)
}

// Isolate the relay inventory from other integration tests' retained facts.
func journalDispatchStore(t *testing.T) (*store.Postgres, *pgxpool.Pool, string) {
	t.Helper()
	_, base, prefix := lifecycleStore(t)
	ctx := context.Background()
	schema := pgx.Identifier{prefix}.Sanitize()
	if _, err := base.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	cfg := base.Config()
	cfg.MaxConns = 4
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	db, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close(); _, _ = base.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE") })
	if err := migrations.Apply(ctx, db); err != nil {
		t.Fatal(err)
	}
	s := store.New(db, db)
	s.LifecycleCapture = true
	return s, db, prefix
}

func TestJournalDispatchRetryAcknowledgementAndCompetingWorkers(t *testing.T) {
	s, db, prefix := journalDispatchStore(t)
	ctx := context.Background()
	result, err := s.PostDeposit(ctx, ledger.PostDeposit{CommandID: prefix, DepositID: prefix, UserID: prefix, AssetID: "asset_usdt", CustodyPositionID: "custody_position_alice_eth_usdt", AmountAtomic: "9007199254740993", TargetBucket: "available", SourceSystem: "deposit-service"})
	if err != nil {
		t.Fatal(err)
	}
	var original []byte
	if err := db.QueryRow(ctx, `SELECT payload FROM ledger_journal_outbox WHERE journal_id=$1`, result.JournalID).Scan(&original); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{}, 1)
	defer close(release)
	brokerErr := errors.New("lost acknowledgement")
	d := store.JournalDispatcher{DB: db, Timeout: 5 * time.Second, Publisher: journalPublisherFunc(func(ctx context.Context, e store.JournalPublication) error {
		if e.JournalID != result.JournalID || !bytes.Equal(e.Payload, original) {
			t.Error("changed source fact")
		}
		close(entered)
		select {
		case <-release:
			return brokerErr
		case <-ctx.Done():
			return ctx.Err()
		}
	})}
	done := make(chan error, 1)
	go func() { _, err := d.DispatchOnce(ctx); done <- err }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("publisher not entered")
	}
	second := store.JournalDispatcher{DB: db, Timeout: time.Second, Publisher: journalPublisherFunc(func(_ context.Context, e store.JournalPublication) error {
		if !bytes.Equal(e.Payload, original) {
			t.Error("retry changed source bytes")
		}
		return nil
	})}
	if found, err := second.DispatchOnce(ctx); found || err != nil {
		t.Fatalf("locked fact dispatched: %v %v", found, err)
	}
	// Cancel only the simulated acknowledgement wait, retaining a usable tx so
	// the relay can persist its bounded backoff.
	release <- struct{}{}
	if err := <-done; !errors.Is(err, brokerErr) {
		t.Fatal(err)
	}
	if found, err := second.DispatchOnce(ctx); found || err != nil {
		t.Fatalf("backoff ignored: %v %v", found, err)
	}
	if _, err := db.Exec(ctx, `UPDATE ledger_journal_delivery SET next_attempt_at=now()`); err != nil {
		t.Fatal(err)
	}
	if found, err := second.DispatchOnce(ctx); !found || err != nil {
		t.Fatalf("retry failed: %v %v", found, err)
	}
	if found, err := second.DispatchOnce(ctx); found || err != nil {
		t.Fatalf("acknowledged fact repeated: %v %v", found, err)
	}
	var attempts int
	var acknowledged bool
	if err := db.QueryRow(ctx, `SELECT attempt_count,published_at IS NOT NULL FROM ledger_journal_delivery`).Scan(&attempts, &acknowledged); err != nil || attempts != 2 || !acknowledged {
		t.Fatalf("attempts=%d ack=%v err=%v", attempts, acknowledged, err)
	}
	var retained []byte
	if err := db.QueryRow(ctx, `SELECT payload FROM ledger_journal_outbox`).Scan(&retained); err != nil || !bytes.Equal(original, retained) {
		t.Fatal("source fact changed", err)
	}
}

func TestJournalDispatchRejectsInvalidSourceAndCancelledContext(t *testing.T) {
	_, db, _ := journalDispatchStore(t)
	ctx := context.Background()
	// Capture off permits a deliberately malformed fixture, as a database
	// migration or historical writer could have supplied invalid bytes.
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	for _, sql := range []string{
		`SET LOCAL praxis.ledger_lifecycle_capture='off'`,
		`INSERT INTO ledger_journals(id,reference_type,reference_id,journal_type,source_system,source_event_id,occurred_at) VALUES('bad','test','test','test','test','test',now())`,
		`INSERT INTO ledger_journal_outbox(event_id,journal_id,payload,occurred_at) VALUES('bad','bad',decode('01','hex'),now())`,
	} {
		if _, err := tx.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	d := store.JournalDispatcher{DB: db, Timeout: time.Second, Publisher: journalPublisherFunc(func(context.Context, store.JournalPublication) error { t.Error("invalid fact published"); return nil })}
	if found, err := d.DispatchOnce(ctx); !found || err == nil {
		t.Fatalf("invalid fact accepted: %v %v", found, err)
	}
	var pending bool
	if err := db.QueryRow(ctx, `SELECT published_at IS NULL FROM ledger_journal_delivery`).Scan(&pending); err != nil || !pending {
		t.Fatal("invalid fact acknowledged", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := d.DispatchOnce(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestJournalDispatchTimeoutLeavesFactPending(t *testing.T) {
	s, db, prefix := journalDispatchStore(t)
	ctx := context.Background()
	if _, err := s.PostDeposit(ctx, ledger.PostDeposit{CommandID: prefix, DepositID: prefix, UserID: prefix, AssetID: "asset_usdt", CustodyPositionID: "custody_position_alice_eth_usdt", AmountAtomic: "100", TargetBucket: "available", SourceSystem: "deposit-service"}); err != nil {
		t.Fatal(err)
	}
	d := store.JournalDispatcher{DB: db, Timeout: 50 * time.Millisecond, Publisher: journalPublisherFunc(func(ctx context.Context, _ store.JournalPublication) error {
		<-ctx.Done()
		return ctx.Err()
	})}
	if found, err := d.DispatchOnce(ctx); !found || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout found=%v err=%v", found, err)
	}
	var pending bool
	if err := db.QueryRow(ctx, `SELECT published_at IS NULL FROM ledger_journal_delivery`).Scan(&pending); err != nil || !pending {
		t.Fatal("timed-out fact acknowledged", err)
	}
	d.Timeout = time.Second
	d.Publisher = journalPublisherFunc(func(context.Context, store.JournalPublication) error { return nil })
	if found, err := d.DispatchOnce(ctx); !found || err != nil {
		t.Fatalf("timeout retry found=%v err=%v", found, err)
	}
}
