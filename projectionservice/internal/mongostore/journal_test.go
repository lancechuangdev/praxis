package mongostore

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	lifecycle "praxis/api/events/lifecycle/v1"
	wire "praxis/api/gen/events/lifecycle/v1"
	"praxis/projectionservice/internal/journal"
)

func journalFact(id string, previous, version uint64) *wire.LifecycleEvent {
	return &wire.LifecycleEvent{EventId: "event-" + id, EventType: lifecycle.LedgerJournalType, SchemaVersion: 1, Producer: "ledger-service", CorrelationId: "request", CausationId: "command", OccurredAt: timestamppb.New(time.Unix(1700000000, 0)), Payload: &wire.LifecycleEvent_LedgerJournal{LedgerJournal: &wire.LedgerJournalPosted{JournalId: id, BusinessType: "transfer", ReferenceId: "reference", CommandId: "command", SourceSystem: "ledger-service", Entries: []*wire.JournalEntry{
		{EntryId: id + "-1", AccountId: "customer", AssetId: "USD", Side: wire.PostingSide_POSTING_SIDE_DEBIT, AmountAtomic: "9007199254740993", UserAssetAccountId: "a", Bucket: "available"},
		{EntryId: id + "-2", AccountId: "customer", AssetId: "USD", Side: wire.PostingSide_POSTING_SIDE_CREDIT, AmountAtomic: "9007199254740993", UserAssetAccountId: "b", Bucket: "available"},
	}, Balances: []*wire.AccountBalanceSnapshot{
		{UserAssetAccountId: "a", UserId: "alice", AssetId: "USD", PreviousVersion: previous, Version: version, AvailableAtomic: "9007199254740993", ReservedAtomic: "0", HoldAtomic: "0", DepositPendingAtomic: "0", WithdrawalPendingAtomic: "0"},
		{UserAssetAccountId: "b", UserId: "bob", AssetId: "USD", PreviousVersion: previous, Version: version, AvailableAtomic: "9007199254740993", ReservedAtomic: "0", HoldAtomic: "0", DepositPendingAtomic: "0", WithdrawalPendingAtomic: "0"},
	}}}}
}
func TestAccountDocumentCoordinatesAndExactAmounts(t *testing.T) {
	b := journalFact("first", 0, 2).GetLedgerJournal().Balances[0]
	doc, err := encodeAccount(b)
	if err != nil {
		t.Fatal(err)
	}
	state, err := decodeAccount(doc)
	if err != nil || state.Version != 2 || state.UserID != "alice" {
		t.Fatal(state, err)
	}
	var decoded wire.AccountBalanceSnapshot
	if err := proto.Unmarshal(doc.Payload, &decoded); err != nil || !proto.Equal(b, &decoded) {
		t.Fatal("changed atomic amounts", err)
	}
	for _, change := range []func(*accountDocument){func(d *accountDocument) { d.ID = "other" }, func(d *accountDocument) { d.UserID = "other" }, func(d *accountDocument) { d.AssetID = "other" }, func(d *accountDocument) { d.Version = 3 }, func(d *accountDocument) { d.Payload = []byte("bad") }} {
		bad := doc
		change(&bad)
		if _, err := decodeAccount(bad); err == nil {
			t.Fatal("accepted corrupt account")
		}
	}
	b.AvailableAtomic = "-1"
	if _, err := encodeAccount(b); err == nil {
		t.Fatal("accepted negative account balance")
	}
}
func integrationJournal(t *testing.T) *JournalStore {
	t.Helper()
	workflowStore := integrationStore(t)
	s, err := NewJournal(workflowStore.client, workflowStore.db.Name(), journal.Target{Generation: "journals-v1", Mode: journal.Shadow}, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s
}
func applyJournal(t *testing.T, p *journal.Processor, event *wire.LifecycleEvent) (journal.Outcome, error) {
	t.Helper()
	raw, err := lifecycle.Encode(event)
	if err != nil {
		t.Fatal(err)
	}
	return p.Process(context.Background(), "ledger.journals.v1", []byte(event.GetLedgerJournal().JournalId), raw)
}
func TestMongoJournalConcurrencyGapRecoveryAndIdentity(t *testing.T) {
	s := integrationJournal(t)
	p, err := journal.New(s.target, s)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := applyJournal(t, p, journalFact("later", 2, 5)); !errors.Is(err, journal.ErrGap) {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	outcomes := make(chan journal.Outcome, 8)
	failures := make(chan error, 8)
	for range 8 {
		group.Add(1)
		go func() {
			defer group.Done()
			out, err := applyJournal(t, p, journalFact("first", 0, 2))
			outcomes <- out
			failures <- err
		}()
	}
	group.Wait()
	close(outcomes)
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	applied := 0
	for out := range outcomes {
		if out == journal.Applied {
			applied++
		} else if out != journal.Duplicate {
			t.Fatal(out)
		}
	}
	if applied != 1 {
		t.Fatal("applied", applied)
	}
	if out, err := applyJournal(t, p, journalFact("later", 2, 5)); out != journal.Applied || err != nil {
		t.Fatal(out, err)
	}
	for _, collection := range []*mongo.Collection{s.accounts, s.journals, s.inbox} {
		count, err := collection.CountDocuments(context.Background(), bson.D{})
		if err != nil || count != 2 {
			t.Fatal(collection.Name(), count, err)
		}
	}
	wrong := journalFact("wrong-owner", 5, 6)
	wrong.GetLedgerJournal().Balances[1].UserId = "mallory"
	if _, err := applyJournal(t, p, wrong); !errors.Is(err, journal.ErrConflict) {
		t.Fatal(err)
	}
	var account accountDocument
	if err := s.accounts.FindOne(context.Background(), bson.D{{Key: "_id", Value: "a"}}).Decode(&account); err != nil || account.Version != 5 {
		t.Fatal("partial account update", account.Version, err)
	}
}
func TestMongoJournalRollbackAndPreflight(t *testing.T) {
	s := integrationJournal(t)
	ctx := context.Background()
	event := journalFact("first", 0, 1)
	// Return an error after account and journal writes; every collection must roll back.
	failure := errors.New("injected transaction failure")
	err := s.Within(ctx, s.target, func(tx journal.Transaction) error {
		for _, b := range event.GetLedgerJournal().Balances {
			if _, err := tx.LockAccount(ctx, b.UserAssetAccountId); err != nil {
				return err
			}
			if err := tx.PutAccount(ctx, b); err != nil {
				return err
			}
		}
		if err := tx.PutJournal(ctx, event); err != nil {
			return err
		}
		return failure
	})
	if !errors.Is(err, failure) {
		t.Fatal(err)
	}
	for _, collection := range []*mongo.Collection{s.accounts, s.journals, s.inbox} {
		count, err := collection.CountDocuments(ctx, bson.D{})
		if err != nil || count != 0 {
			t.Fatal("partial transaction", collection.Name(), count, err)
		}
	}
	if _, err := s.journals.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "occurred_at", Value: 1}}, Options: options.Index().SetName("unsafe_ttl").SetExpireAfterSeconds(0)}); err != nil {
		t.Fatal(err)
	}
	if err := s.Verify(ctx); err == nil {
		t.Fatal("journal retention drift accepted")
	}
	p, err := journal.New(s.target, s)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := applyJournal(t, p, event); !errors.Is(err, ErrUnverified) {
		t.Fatal(err)
	}
	if err := s.journals.Indexes().DropOne(ctx, "unsafe_ttl"); err != nil {
		t.Fatal(err)
	}
	if err := s.Verify(ctx); err != nil {
		t.Fatal(err)
	}
	if out, err := applyJournal(t, p, event); out != journal.Applied || err != nil {
		t.Fatal(out, err)
	}
}

func TestMongoJournalCompetingAccountChainsCannotOverwrite(t *testing.T) {
	s := integrationJournal(t)
	p, err := journal.New(s.target, s)
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	results := make(chan error, 2)
	for _, id := range []string{"first", "competing"} {
		group.Add(1)
		go func(id string) {
			defer group.Done()
			_, err := applyJournal(t, p, journalFact(id, 0, 2))
			results <- err
		}(id)
	}
	group.Wait()
	close(results)
	applied, stale := 0, 0
	for err := range results {
		if err == nil {
			applied++
		} else if errors.Is(err, journal.ErrStale) {
			stale++
		} else {
			t.Fatal(err)
		}
	}
	if applied != 1 || stale != 1 {
		t.Fatal("conflicting chains accepted", applied, stale)
	}
	for _, collection := range []*mongo.Collection{s.journals, s.inbox} {
		count, err := collection.CountDocuments(context.Background(), bson.D{})
		if err != nil || count != 1 {
			t.Fatal("competing journal leaked", count, err)
		}
	}
}
