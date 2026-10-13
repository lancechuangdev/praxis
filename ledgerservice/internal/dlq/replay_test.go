package dlq

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	eventsv1 "praxis/api/events/v1"
)

func fixture(t *testing.T) eventsv1.DLQEnvelope {
	t.Helper()
	occurred := time.Date(2026, 10, 1, 1, 2, 3, 0, time.UTC)
	e, err := eventsv1.New("event-1", "PostDeposit", "deposit-1", occurred, map[string]string{"x": "y"})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(e)
	return eventsv1.DLQEnvelope{Version: 1, ID: "dlq:ledger.commands.v1:0:4", SourceTopic: "ledger.commands.v1", SourcePartition: 0, SourceOffset: 4, SourceKey: []byte("account-1"), SourceHeaders: []eventsv1.DLQHeader{{Key: "traceparent", Value: []byte("trace")}}, ConsumerGroup: "ledger", FailureClass: "deterministic", FailureCode: "invalid_event_payload", FailureReason: "bad", FailedAt: occurred, AttemptCount: 1, Original: raw}
}

func TestSelectionAndAuthorization(t *testing.T) {
	d := fixture(t)
	if !(Selection{FailureCode: "invalid_event_payload", EventID: "event-1", From: d.FailedAt.Add(-time.Second), Until: d.FailedAt}).Match(d) {
		t.Fatal("expected match")
	}
	if (Selection{EventID: "other"}).Match(d) {
		t.Fatal("unexpected match")
	}
	if err := Authorize("alice", []string{"bob", "alice"}); err != nil {
		t.Fatal(err)
	}
	if err := Authorize("mallory", []string{"alice"}); err == nil {
		t.Fatal("expected denial")
	}
}

func TestReplayMessagePreservesRecordAndAddsAuditHeaders(t *testing.T) {
	d := fixture(t)
	now := d.FailedAt.Add(time.Hour)
	m, err := ReplayMessage(d, kafka.Message{Topic: "ledger.commands.dlq.v1", Partition: 2, Offset: 9}, "job-1", "alice", now)
	if err != nil {
		t.Fatal(err)
	}
	if m.Topic != d.SourceTopic || string(m.Key) != string(d.SourceKey) || string(m.Value) != string(d.Original) || len(m.Headers) != 7 {
		t.Fatalf("unexpected replay: %+v", m)
	}
	if m.Headers[0].Key != "traceparent" || string(m.Headers[0].Value) != "trace" {
		t.Fatal("source headers not preserved")
	}
}
