package mongostore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/v2/bson"

	lifecycle "praxis/api/events/lifecycle/v1"
	"praxis/projectionservice/internal/journal"
	"praxis/projectionservice/internal/readapi"
	"praxis/projectionservice/internal/workflow"
)

func TestMongoQueueRepairsReorderedJournalAndQuarantines(t *testing.T) {
	s := integrationJournal(t)
	ctx := context.Background()
	q, err := NewQueue(s.core.client, s.core.db.Name(), workflow.Target{Generation: s.target.Generation, Mode: workflow.Mode(s.target.Mode)}, "journal", time.Second*10)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Prepare(ctx); err != nil {
		t.Fatal(err)
	}
	p, err := journal.New(s.target, s)
	if err != nil {
		t.Fatal(err)
	}
	first, _ := lifecycle.Encode(journalFact("first", 0, 2))
	later, _ := lifecycle.Encode(journalFact("later", 2, 5))
	messages := []kafka.Message{{Topic: "ledger.journals.v1", Partition: 0, Offset: 0, Key: []byte("later"), Value: later}, {Topic: "ledger.journals.v1", Partition: 0, Offset: 1, Key: []byte("first"), Value: first}}
	for _, m := range messages {
		if _, err := q.ProcessRecord(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	records, err := q.Batch(ctx)
	if err != nil || len(records) != 2 {
		t.Fatal(records, err)
	}
	for _, r := range records {
		_, applyErr := p.Process(ctx, r.Topic, r.Key, r.Payload)
		if err := q.Finish(ctx, r, applyErr); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := q.records.UpdateMany(ctx, bson.D{{Key: "state", Value: "pending"}}, bson.D{{Key: "$set", Value: bson.D{{Key: "next_attempt_at", Value: time.Now().Add(-time.Second)}}}}); err != nil {
		t.Fatal(err)
	}
	records, err = q.Batch(ctx)
	if err != nil || len(records) != 1 {
		t.Fatal(records, err)
	}
	for _, r := range records {
		_, applyErr := p.Process(ctx, r.Topic, r.Key, r.Payload)
		if applyErr != nil {
			t.Fatal(applyErr)
		}
		if err := q.Finish(ctx, r, nil); err != nil {
			t.Fatal(err)
		}
		if err := q.Finish(ctx, r, nil); err != nil {
			t.Fatal(err)
		}
	}
	summary, err := q.Summary(ctx)
	if err != nil || summary.Active != 0 || summary.Bytes != 0 {
		t.Fatal(summary, err)
	}
	if out, err := q.ProcessRecord(ctx, messages[0]); out != workflow.Duplicate || err != nil {
		t.Fatal(out, err)
	}
	wrong := messages[0]
	wrong.Value = first
	if _, err := q.ProcessRecord(ctx, wrong); err == nil {
		t.Fatal("coordinate reuse accepted")
	}
	bad := kafka.Message{Topic: "ledger.journals.v1", Partition: 0, Offset: 2, Key: []byte("bad"), Value: []byte("bad")}
	if _, err := q.ProcessRecord(ctx, bad); err != nil {
		t.Fatal(err)
	}
	summary, err = q.Summary(ctx)
	if err != nil || summary.Active != 1 || summary.Quarantined != 1 {
		t.Fatal(summary, err)
	}
	items, next, pageErr := q.QuarantinePage(ctx, "")
	if pageErr != nil || len(items) != 1 || next != "" || items[0].Digest != payloadHash(bad.Value) {
		t.Fatal(items, next, pageErr)
	}
	metadata, _ := json.Marshal(items)
	if bytes.Contains(metadata, []byte("\"payload\"")) || bytes.Contains(metadata, []byte("\"key\"")) {
		t.Fatal("inspection metadata leaked source bytes")
	}
	if err := q.Retry(ctx, "ledger.journals.v1:0:2", payloadHash(bad.Value), "test repair reference"); err != nil {
		t.Fatal(err)
	}
	if _, err := q.quota.UpdateOne(ctx, bson.D{{Key: "_id", Value: "active"}}, bson.D{{Key: "$set", Value: bson.D{{Key: "records", Value: queueRecords}}}}); err != nil {
		t.Fatal(err)
	}
	bad.Offset = 3
	if _, err := q.ProcessRecord(ctx, bad); !errors.Is(err, ErrQueueFull) {
		t.Fatal(err)
	}
	count, err := q.records.CountDocuments(ctx, bson.D{{Key: "offset", Value: int64(3)}})
	if err != nil || count != 0 {
		t.Fatal("full queue inserted record", count, err)
	}
}
func TestMongoReconcileCutoverOwnershipAndRollback(t *testing.T) {
	s := integrationJournal(t)
	ctx := context.Background()
	target := workflow.Target{Generation: s.target.Generation, Mode: workflow.Mode(s.target.Mode)}
	q, err := NewQueue(s.core.client, s.core.db.Name(), target, "journal", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Prepare(ctx); err != nil {
		t.Fatal(err)
	}
	ops, err := NewOperations(s.core.client, s.core.db.Name(), target, "journal", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := ops.Prepare(ctx); err != nil {
		t.Fatal(err)
	}
	p, err := journal.New(s.target, s)
	if err != nil {
		t.Fatal(err)
	}
	event := journalFact("first", 0, 2)
	if _, err := applyJournal(t, p, event); err != nil {
		t.Fatal(err)
	}
	raw, _ := lifecycle.Encode(event)
	export, _ := json.Marshal(ExportRecord{Topic: "ledger.journals.v1", Key: []byte("first"), Payload: raw})
	report, err := ops.Reconcile(ctx, bytes.NewReader(export))
	if err != nil {
		t.Fatal(err)
	}
	route, err := ops.Activate(ctx, report.ID, 0)
	if err != nil || route.Revision != 1 {
		t.Fatal(route, err)
	}
	if _, err := ops.Activate(ctx, report.ID, 0); err == nil {
		t.Fatal("concurrent route overwrite accepted")
	}
	route, err = ops.Activate(ctx, report.ID, 1)
	if err != nil || route.Revision != 2 {
		t.Fatal(route, err)
	}
	reads, err := NewReads(s.core.client, s.core.db.Name(), target, "journal", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := reads.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	doc, err := reads.Get(ctx, "activity", "alice", "first")
	if err != nil || strings.Contains(string(doc.Data), "bob") || !strings.Contains(string(doc.Data), "9007199254740993") {
		t.Fatal(string(doc.Data), err)
	}
	if _, err := reads.Get(ctx, "activity", "mallory", "first"); !errors.Is(err, readapi.ErrNotFound) {
		t.Fatal(err)
	}
	page, err := reads.List(ctx, "activity", "alice", "", 1)
	if err != nil || len(page.Items) != 1 {
		t.Fatal(page, err)
	}
	if _, err := reads.List(ctx, "activity", "alice", "bad", 1); !errors.Is(err, readapi.ErrCursor) {
		t.Fatal(err)
	}
	// New effects invalidate an old report before another route CAS can occur.
	if _, err := applyJournal(t, p, journalFact("next", 2, 3)); err != nil {
		t.Fatal(err)
	}
	if _, err := ops.Activate(ctx, report.ID, 2); err == nil {
		t.Fatal("changed target activated using old report")
	}
	if _, err := ops.Reconcile(ctx, bytes.NewReader(export)); err == nil {
		t.Fatal("incomplete authoritative export accepted")
	}
}

func TestMongoWorkflowHistoryPaginationAndInboxReconciliation(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	p, err := workflow.New(s.target, s)
	if err != nil {
		t.Fatal(err)
	}
	q, err := NewQueue(s.client, s.db.Name(), s.target, "workflow", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Prepare(ctx); err != nil {
		t.Fatal(err)
	}
	var export bytes.Buffer
	for version := uint64(1); version <= 3; version++ {
		event := testEvent(version)
		event.EventId = fmt.Sprintf("event-%d", version)
		raw, err := lifecycle.Encode(event)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := p.Process(ctx, "order.events.v1", []byte("order-1"), raw); err != nil {
			t.Fatal(err)
		}
		if err := json.NewEncoder(&export).Encode(ExportRecord{Topic: "order.events.v1", Offset: int64(version - 1), Key: []byte("order-1"), Payload: raw}); err != nil {
			t.Fatal(err)
		}
	}
	reads, err := NewReads(s.client, s.db.Name(), s.target, "workflow", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := reads.Verify(ctx); err != nil {
		t.Fatal(err)
	}
	page, err := reads.List(ctx, "order-history:order-1", "alice", "", 2)
	if err != nil || len(page.Items) != 2 || page.Items[0].Version != "1" || page.Next == "" {
		t.Fatal(page, err)
	}
	next, err := reads.List(ctx, "order-history:order-1", "alice", page.Next, 2)
	if err != nil || len(next.Items) != 1 || next.Items[0].Version != "3" {
		t.Fatal(next, err)
	}
	if _, err := reads.List(ctx, "order-history:order-1", "bob", page.Next, 2); !errors.Is(err, readapi.ErrCursor) {
		t.Fatal("cross-owner cursor accepted", err)
	}
	hidden, err := reads.List(ctx, "order-history:order-1", "bob", "", 2)
	if err != nil || len(hidden.Items) != 0 {
		t.Fatal("cross-owner history exposed", hidden, err)
	}
	ops, err := NewOperations(s.client, s.db.Name(), s.target, "workflow", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := ops.Prepare(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ops.Reconcile(ctx, bytes.NewReader(export.Bytes())); err != nil {
		t.Fatal(err)
	}
	if _, err := s.inbox.DeleteOne(ctx, bson.D{{Key: "_id", Value: "event-1"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := ops.Reconcile(ctx, bytes.NewReader(export.Bytes())); err == nil {
		t.Fatal("missing replay receipt was not detected")
	}
}

func TestMongoReadRouteSwitchAndRollbackRetainGenerations(t *testing.T) {
	old := integrationJournal(t)
	ctx := context.Background()
	event := journalFact("first", 0, 1)
	raw, _ := lifecycle.Encode(event)
	input, _ := json.Marshal(ExportRecord{Topic: "ledger.journals.v1", Key: []byte("first"), Payload: raw})
	prepare := func(store *JournalStore) (*Operations, *ReadStore, Report) {
		p, err := journal.New(store.target, store)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := applyJournal(t, p, event); err != nil {
			t.Fatal(err)
		}
		target := workflow.Target{Generation: store.target.Generation, Mode: workflow.Mode(store.target.Mode)}
		q, err := NewQueue(store.core.client, store.core.db.Name(), target, "journal", time.Second*10)
		if err != nil {
			t.Fatal(err)
		}
		if err := q.Prepare(ctx); err != nil {
			t.Fatal(err)
		}
		ops, err := NewOperations(store.core.client, store.core.db.Name(), target, "journal", time.Second*10)
		if err != nil {
			t.Fatal(err)
		}
		if err := ops.Prepare(ctx); err != nil {
			t.Fatal(err)
		}
		report, err := ops.Reconcile(ctx, bytes.NewReader(input))
		if err != nil {
			t.Fatal(err)
		}
		reads, err := NewReads(store.core.client, store.core.db.Name(), target, "journal", time.Second)
		if err != nil {
			t.Fatal(err)
		}
		return ops, reads, report
	}
	oldOps, oldReads, oldReport := prepare(old)
	if _, err := oldOps.Activate(ctx, oldReport.ID, 0); err != nil {
		t.Fatal(err)
	}
	newer, err := NewJournal(old.core.client, old.core.db.Name(), journal.Target{Generation: "next-generation", Mode: journal.Shadow}, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := newer.Prepare(ctx); err != nil {
		t.Fatal(err)
	}
	newOps, newReads, newReport := prepare(newer)
	if _, err := newOps.Activate(ctx, newReport.ID, 1); err != nil {
		t.Fatal(err)
	}
	if err := oldReads.Ready(ctx); err == nil {
		t.Fatal("old generation remained readable after switch")
	}
	if err := newReads.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := oldOps.Activate(ctx, oldReport.ID, 2); err != nil {
		t.Fatal(err)
	}
	if err := oldReads.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	if err := newReads.Ready(ctx); err == nil {
		t.Fatal("new generation remained selected after rollback")
	}
	count, err := old.core.db.Collection("projection_route_history").CountDocuments(ctx, bson.D{})
	if err != nil || count != 3 {
		t.Fatal("cutover audit missing", count, err)
	}
	for _, store := range []*JournalStore{old, newer} {
		count, err := store.journals.CountDocuments(ctx, bson.D{})
		if err != nil || count != 1 {
			t.Fatal("rollback destroyed retained history", count, err)
		}
	}
}
