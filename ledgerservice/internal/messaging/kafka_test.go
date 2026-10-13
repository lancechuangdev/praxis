package messaging

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func TestMalformedEnvelopeIsDeterministic(t *testing.T) {
	err := (&Consumer{}).handle(context.Background(), []byte(`not-json`))
	var deterministic deterministicError
	if !errors.As(err, &deterministic) {
		t.Fatalf("error=%v, want deterministic classification", err)
	}
	if deterministic.code != "invalid_envelope" {
		t.Fatalf("code=%q, want invalid_envelope", deterministic.code)
	}
}

func TestUnsupportedTypeIsDeterministic(t *testing.T) {
	raw := []byte(`{"version":1,"id":"event-1","type":"Unknown","aggregate_id":"aggregate-1","occurred_at":"2026-01-02T03:04:05Z","data":{}}`)
	err := (&Consumer{}).handle(context.Background(), raw)
	var deterministic deterministicError
	if !errors.As(err, &deterministic) {
		t.Fatalf("error=%v, want deterministic classification", err)
	}
	if deterministic.code != "unsupported_event_type" {
		t.Fatalf("code=%q, want unsupported_event_type", deterministic.code)
	}
}

func TestBuildDLQEnvelopePreservesSourceRecord(t *testing.T) {
	sourceTime := time.Date(2026, time.October, 12, 10, 30, 0, 0, time.UTC)
	failedAt := sourceTime.Add(time.Minute)
	message := kafka.Message{
		Topic:     "ledger.commands.v1",
		Partition: 3,
		Offset:    4812,
		Time:      sourceTime,
		Key:       []byte{0xff, 0x00},
		Value:     []byte(`not-json`),
		Headers:   []kafka.Header{{Key: "traceparent", Value: []byte("00-trace")}, {Key: "binary", Value: []byte{0xff}}},
	}

	d, err := buildDLQEnvelope("cex-ledger-service", message, deterministic("invalid_envelope", errors.New("decode failed")), failedAt)
	if err != nil {
		t.Fatal(err)
	}
	if d.ID != "dlq:ledger.commands.v1:3:4812" || d.FailureCode != "invalid_envelope" || d.AttemptCount != 1 {
		t.Fatalf("unexpected DLQ metadata: %+v", d)
	}
	if !d.SourceTimestamp.Equal(sourceTime) || !d.FailedAt.Equal(failedAt) {
		t.Fatalf("unexpected timestamps: source=%v failed=%v", d.SourceTimestamp, d.FailedAt)
	}
	if !bytes.Equal(d.SourceKey, message.Key) || !bytes.Equal(d.Original, message.Value) {
		t.Fatal("source key or value was not preserved")
	}
	if len(d.SourceHeaders) != 2 || d.SourceHeaders[1].Key != "binary" || !bytes.Equal(d.SourceHeaders[1].Value, []byte{0xff}) {
		t.Fatalf("source headers were not preserved: %+v", d.SourceHeaders)
	}
}
