package mongostore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	lifecycle "praxis/api/events/lifecycle/v1"
	"praxis/projectionservice/internal/workflow"
)

func integrationStore(t *testing.T) *Store {
	t.Helper()
	uri := os.Getenv("PROJECTION_TEST_MONGODB_URI")
	if uri == "" {
		t.Skip("PROJECTION_TEST_MONGODB_URI is not set (replica set required)")
	}
	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetMaxPoolSize(8).SetServerSelectionTimeout(5 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	database := fmt.Sprintf("praxis_projection_it_%d", time.Now().UnixNano())
	s, err := New(client, database, workflow.Target{Generation: "orders-v1", Mode: workflow.Shadow}, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := s.db.Drop(ctx); err != nil {
			t.Errorf("drop isolated test database: %v", err)
		}
		_ = client.Disconnect(ctx)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.Prepare(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Prepare(ctx); err != nil {
		t.Fatal("prepare is not repeatable:", err)
	}
	return s
}
func process(t *testing.T, p *workflow.Processor, version uint64) (workflow.Outcome, error) {
	t.Helper()
	event := testEvent(version)
	event.EventId = fmt.Sprintf("event-%d", version)
	raw, err := lifecycle.Encode(event)
	if err != nil {
		t.Fatal(err)
	}
	return p.Process(context.Background(), "order.events.v1", []byte("order-1"), raw)
}

func TestMongoConcurrentFirstFactAndContiguousUpdates(t *testing.T) {
	s := integrationStore(t)
	p, err := workflow.New(s.target, s)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := lifecycle.Encode(testEvent(1))
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan workflow.Outcome, 8)
	failures := make(chan error, 8)
	var group sync.WaitGroup
	for range 8 {
		group.Add(1)
		go func() {
			defer group.Done()
			outcome, err := p.Process(context.Background(), "order.events.v1", []byte("order-1"), raw)
			results <- outcome
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
	var applied, duplicates int
	for result := range results {
		if result == workflow.Applied {
			applied++
		} else if result == workflow.Duplicate {
			duplicates++
		}
	}
	if applied != 1 || duplicates != 7 {
		t.Fatalf("applied=%d duplicates=%d", applied, duplicates)
	}
	if _, err := process(t, p, 3); !errors.Is(err, workflow.ErrGap) {
		t.Fatal(err)
	}
	if result, err := process(t, p, 2); result != workflow.Applied || err != nil {
		t.Fatalf("%s %v", result, err)
	}
	if result, err := process(t, p, 1); result != workflow.Duplicate || err != nil {
		t.Fatalf("%s %v", result, err)
	}
	var doc snapshotDocument
	if err := s.snapshots.FindOne(context.Background(), bson.D{{Key: "_id", Value: "order-1"}}).Decode(&doc); err != nil || doc.Version != 2 {
		t.Fatalf("version=%d err=%v", doc.Version, err)
	}
	count, err := s.inbox.CountDocuments(context.Background(), bson.D{})
	if err != nil || count != 2 {
		t.Fatalf("inbox=%d err=%v", count, err)
	}
}

type abortAfterInbox struct{ workflow.Transaction }

var injectedAbort = errors.New("injected abort after inbox write")

func (t abortAfterInbox) PutInbox(ctx context.Context, id string, receipt workflow.Receipt) error {
	if err := t.Transaction.PutInbox(ctx, id, receipt); err != nil {
		return err
	}
	return injectedAbort
}

type abortStore struct{ *Store }

func (s abortStore) Within(ctx context.Context, target workflow.Target, fn func(workflow.Transaction) error) error {
	return s.Store.Within(ctx, target, func(tx workflow.Transaction) error { return fn(abortAfterInbox{tx}) })
}

func TestMongoAtomicRollbackAndTargetIsolation(t *testing.T) {
	s := integrationStore(t)
	p, err := workflow.New(s.target, abortStore{s})
	if err != nil {
		t.Fatal(err)
	}
	if result, err := process(t, p, 1); result != "" || !errors.Is(err, injectedAbort) {
		t.Fatalf("%s %v", result, err)
	}
	for _, collection := range []*mongo.Collection{s.snapshots, s.inbox} {
		count, err := collection.CountDocuments(context.Background(), bson.D{})
		if err != nil || count != 0 {
			t.Fatalf("partial commit: count=%d err=%v", count, err)
		}
	}
	p, err = workflow.New(s.target, s)
	if err != nil {
		t.Fatal(err)
	}
	if result, err := process(t, p, 1); result != workflow.Applied || err != nil {
		t.Fatalf("%s %v", result, err)
	}
	for _, target := range []workflow.Target{{Generation: "orders-v2", Mode: workflow.Shadow}, {Generation: "orders-v1", Mode: workflow.Rebuild}} {
		other, err := New(s.client, s.db.Name(), target, 10*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if err := other.Prepare(context.Background()); err != nil {
			t.Fatal(err)
		}
		p, err := workflow.New(target, other)
		if err != nil {
			t.Fatal(err)
		}
		if result, err := process(t, p, 1); result != workflow.Applied || err != nil {
			t.Fatalf("isolated target: %s %v", result, err)
		}
	}
}
