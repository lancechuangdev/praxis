package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"google.golang.org/protobuf/types/known/timestamppb"

	lifecycle "praxis/api/events/lifecycle/v1"
	wire "praxis/api/gen/events/lifecycle/v1"
	"praxis/projectionservice/internal/config"
	"praxis/projectionservice/internal/mongostore"
	"praxis/projectionservice/internal/runtime"
	"praxis/projectionservice/internal/workflow"
)

func TestAdminReplayReconcileAndCutoverWithoutKafka(t *testing.T) {
	uri := os.Getenv("PROJECTION_TEST_MONGODB_URI")
	if uri == "" {
		t.Skip("replica-set URI is not configured")
	}
	ctx := context.Background()
	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetServerSelectionTimeout(time.Second * 5))
	if err != nil {
		t.Fatal(err)
	}
	database := fmt.Sprintf("praxis_admin_it_%d", time.Now().UnixNano())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = client.Database(database).Drop(ctx)
		_ = client.Disconnect(ctx)
	})
	cfg := config.Config{Enabled: true, Stream: "journal", Database: database, Target: workflow.Target{Generation: "rebuild-v1", Mode: workflow.Rebuild}, TransactionTimeout: 10 * time.Second}
	if _, close, err := configure(ctx, cfg, client, true, false); err != nil {
		t.Fatal(err)
	} else {
		close()
	}
	worker, closeWorker, err := configure(ctx, cfg, client, false, false)
	if err != nil {
		t.Fatal(err)
	}
	defer closeWorker()
	event := &wire.LifecycleEvent{EventId: "event", EventType: lifecycle.LedgerJournalType, SchemaVersion: 1, Producer: "ledger-service", CorrelationId: "request", CausationId: "command", OccurredAt: timestamppb.New(time.Unix(1700000000, 0)), Payload: &wire.LifecycleEvent_LedgerJournal{LedgerJournal: &wire.LedgerJournalPosted{JournalId: "journal", BusinessType: "system_transfer", ReferenceId: "reference", CommandId: "command", SourceSystem: "ledger-service", Entries: []*wire.JournalEntry{{EntryId: "a", AccountId: "system-a", AssetId: "USD", Side: wire.PostingSide_POSTING_SIDE_DEBIT, AmountAtomic: "9007199254740993"}, {EntryId: "b", AccountId: "system-b", AssetId: "USD", Side: wire.PostingSide_POSTING_SIDE_CREDIT, AmountAtomic: "9007199254740993"}}}}}
	raw, err := lifecycle.Encode(event)
	if err != nil {
		t.Fatal(err)
	}
	input := filepath.Join(t.TempDir(), "source.jsonl")
	record, _ := json.Marshal(mongostore.ExportRecord{Topic: "ledger.journals.v1", Key: []byte("journal"), Payload: raw})
	if err := os.WriteFile(input, append(record, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	queued := worker.(*runtime.Queued)
	if err := runAdmin(ctx, command{Mode: "replay", Input: input}, cfg, client, queued); err != nil {
		t.Fatal(err)
	}
	if err := runAdmin(ctx, command{Mode: "replay", Input: input}, cfg, client, queued); err != nil {
		t.Fatal("replay did not resume idempotently", err)
	}
	count, err := client.Database(database).Collection("projection_replays").CountDocuments(ctx, bson.D{{Key: "complete", Value: true}})
	if err != nil || count != 1 {
		t.Fatal("checkpoint not retained", count, err)
	}
	ops, err := mongostore.NewOperations(client, database, cfg.Target, cfg.Stream, time.Second*10)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(input)
	if err != nil {
		t.Fatal(err)
	}
	report, err := ops.Reconcile(ctx, file)
	_ = file.Close()
	if err != nil {
		t.Fatal(err)
	}
	if err := runAdmin(ctx, command{Mode: "activate", Report: report.ID, Revision: 0}, cfg, client, queued); err != nil {
		t.Fatal(err)
	}
	// Actual Kafka coordinates can overlap synthetic snapshot offsets without
	// replacing retained bytes or reapplying financial projections.
	if _, err := queued.Queue.ProcessRecord(ctx, kafka.Message{Topic: "ledger.journals.v1", Partition: 0, Offset: 0, Key: []byte("journal"), Value: raw}); err != nil {
		t.Fatal(err)
	}
	if err := drain(ctx, queued); err != nil {
		t.Fatal(err)
	}
	if queued.Source != nil {
		t.Fatal("offline admin commands constructed a Kafka consumer")
	}
}
