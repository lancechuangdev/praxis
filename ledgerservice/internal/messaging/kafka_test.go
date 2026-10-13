package messaging

import (
	"context"
	"errors"
	"testing"
)

func TestMalformedEnvelopeIsDeterministic(t *testing.T) {
	err := (&Consumer{}).handle(context.Background(), []byte(`not-json`))
	var deterministic deterministicError
	if !errors.As(err, &deterministic) {
		t.Fatalf("error=%v, want deterministic classification", err)
	}
}

func TestUnsupportedTypeIsDeterministic(t *testing.T) {
	raw := []byte(`{"version":1,"id":"event-1","type":"Unknown","aggregate_id":"aggregate-1","occurred_at":"2026-01-02T03:04:05Z","data":{}}`)
	err := (&Consumer{}).handle(context.Background(), raw)
	var deterministic deterministicError
	if !errors.As(err, &deterministic) {
		t.Fatalf("error=%v, want deterministic classification", err)
	}
}
