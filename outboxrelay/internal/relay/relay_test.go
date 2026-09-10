package relay

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

type fakeStore struct {
	events    []Event
	published []string
	failed    []string
}

func (f *fakeStore) Claim(context.Context, string, int, time.Duration) ([]Event, error) {
	return f.events, nil
}
func (f *fakeStore) MarkPublished(_ context.Context, id, _ string) error {
	f.published = append(f.published, id)
	return nil
}
func (f *fakeStore) MarkFailed(_ context.Context, id, _, _ string) error {
	f.failed = append(f.failed, id)
	return nil
}

type fakePublisher struct {
	calls    int
	messages []kafka.Message
	err      error
}

func (f *fakePublisher) WriteMessages(_ context.Context, messages ...kafka.Message) error {
	f.calls++
	f.messages = append(f.messages, messages...)
	return f.err
}
func (*fakePublisher) Close() error { return nil }

func TestRunOncePublishesClaimAsOneBatch(t *testing.T) {
	store := &fakeStore{events: []Event{
		{SequenceNumber: 3, ID: "e3", Topic: "ledger.events", MessageKey: "alice", Payload: []byte(`3`)},
		{SequenceNumber: 1, ID: "e1", Topic: "ledger.events", MessageKey: "alice", Payload: []byte(`1`)},
		{SequenceNumber: 2, ID: "e2", Topic: "ledger.events", MessageKey: "alice", Payload: []byte(`2`)},
	}}
	publisher := &fakePublisher{}
	r := &Relay{Store: store, Publisher: publisher, InstanceID: "test", ClaimSize: 500, LeaseDuration: time.Second, Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Metrics: &Metrics{}}
	n, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 || publisher.calls != 1 || len(publisher.messages) != 3 {
		t.Fatalf("n=%d calls=%d messages=%d", n, publisher.calls, len(publisher.messages))
	}
	if string(publisher.messages[0].Value) != "1" || string(publisher.messages[2].Value) != "3" {
		t.Fatalf("messages were not ordered by outbox sequence")
	}
	if len(store.published) != 3 || len(store.failed) != 0 {
		t.Fatalf("published=%v failed=%v", store.published, store.failed)
	}
}

func TestRunOnceHandlesPerMessageFailures(t *testing.T) {
	store := &fakeStore{events: []Event{{SequenceNumber: 1, ID: "e1", Topic: "events"}, {SequenceNumber: 2, ID: "e2", Topic: "events"}}}
	publisher := &fakePublisher{err: kafka.WriteErrors{nil, kafka.Unknown}}
	r := &Relay{Store: store, Publisher: publisher, InstanceID: "test", ClaimSize: 2, LeaseDuration: time.Second, Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Metrics: &Metrics{}}
	if _, err := r.RunOnce(context.Background()); err == nil {
		t.Fatal("expected batch error")
	}
	if len(store.published) != 1 || store.published[0] != "e1" {
		t.Fatalf("published=%v", store.published)
	}
	if len(store.failed) != 1 || store.failed[0] != "e2" {
		t.Fatalf("failed=%v", store.failed)
	}
}
