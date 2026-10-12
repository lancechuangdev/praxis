package store

import (
	"testing"
	"time"

	eventsv1 "praxis/api/events/v1"
)

func TestOutboxPayloadUsesCurrentEnvelopeVersion(t *testing.T) {
	raw, err := makeOutboxPayload("event-1", "FundsReserved", "order-1", "correlation-1", "causation-1", "", "", time.Now(), map[string]string{"reservation_id": "reservation-1"})
	if err != nil {
		t.Fatal(err)
	}
	e, err := eventsv1.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	if e.Version != eventsv1.Version || e.CorrelationID != "correlation-1" || e.CausationID != "causation-1" {
		t.Fatalf("envelope=%+v", e)
	}
}
