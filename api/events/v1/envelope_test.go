package eventsv1

import (
	"testing"
	"time"
)

func TestDecodeAcceptsLegacyEnvelopeAsVersionOne(t *testing.T) {
	raw := []byte(`{"id":"event-1","type":"TradeExecuted","aggregate_id":"trade-1","occurred_at":"2026-01-02T03:04:05Z","data":{"trade_id":"trade-1"}}`)
	e, err := Decode(raw)
	if err != nil || e.Version != Version {
		t.Fatalf("envelope=%+v error=%v", e, err)
	}
}

func TestDecodeRejectsUnsupportedVersion(t *testing.T) {
	raw := []byte(`{"version":2,"id":"event-1","type":"TradeExecuted","aggregate_id":"trade-1","occurred_at":"2026-01-02T03:04:05Z","data":{}}`)
	if _, err := Decode(raw); err == nil {
		t.Fatal("expected unsupported version error")
	}
}

func TestDLQEnvelopeValidation(t *testing.T) {
	d := DLQEnvelope{Version: Version, ID: "dlq-1", SourceTopic: "ledger.commands.v1", SourcePartition: 1, SourceOffset: 2, ConsumerGroup: "ledger", FailureClass: "deterministic", FailureReason: "invalid schema", FailedAt: time.Now(), Original: []byte(`not-json`)}
	if err := d.Validate(); err != nil {
		t.Fatal(err)
	}
}
