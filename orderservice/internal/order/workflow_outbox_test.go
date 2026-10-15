package order

import (
	"bytes"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
	lifecycle "praxis/api/events/lifecycle/v1"
	wire "praxis/api/gen/events/lifecycle/v1"
)

func TestWorkflowSourceEncodingIsStableAndStrict(t *testing.T) {
	event := &wire.LifecycleEvent{EventId: "event-1", EventType: lifecycle.OrderWorkflowType, SchemaVersion: 1, Producer: "order-service", CorrelationId: "request-1", CausationId: "request-1", OccurredAt: timestamppb.New(time.Unix(1700000000, 123)), Payload: &wire.LifecycleEvent_OrderWorkflow{OrderWorkflow: &wire.OrderWorkflowSnapshot{RequestId: "request-1", OrderId: "order-1", UserId: "alice", WorkflowVersion: 1, State: wire.OrderWorkflowState_ORDER_WORKFLOW_STATE_RESERVATION_PENDING, Symbol: "BTC-USDT", Side: "buy", OrderType: "market", Quantity: "9007199254740993", ReserveAssetId: "USDT", ReserveAmountAtomic: "123"}}}
	snapshot, err := protojson.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	first, err := EncodeWorkflowOutbox("event-1", "order-1", 1, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	second, err := EncodeWorkflowOutbox("event-1", "order-1", 1, snapshot)
	if err != nil || !bytes.Equal(first, second) {
		t.Fatal("source replay changes wire bytes")
	}
	decoded, err := lifecycle.Decode(first)
	if err != nil || decoded.GetOrderWorkflow().Quantity != "9007199254740993" {
		t.Fatal("lost exact source quantity")
	}
	for _, raw := range [][]byte{nil, []byte(`{"version":1,"type":"OrderAccepted"}`), []byte(`{"unexpected":"field"}`), bytes.Repeat([]byte(" "), lifecycle.MaxEventBytes+1)} {
		if _, err := EncodeWorkflowOutbox("event-1", "order-1", 1, raw); err == nil {
			t.Fatal("invalid source snapshot accepted")
		}
	}
	for _, version := range []int64{0, 2} {
		if _, err := EncodeWorkflowOutbox("event-1", "order-1", version, snapshot); err == nil {
			t.Fatal("wrong source version accepted")
		}
	}
}
