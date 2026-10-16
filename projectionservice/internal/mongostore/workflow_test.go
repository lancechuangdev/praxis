package mongostore

import (
	"bytes"
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/types/known/timestamppb"
	lifecycle "praxis/api/events/lifecycle/v1"
	wire "praxis/api/gen/events/lifecycle/v1"
	"praxis/projectionservice/internal/workflow"
)

func testEvent(version uint64) *wire.LifecycleEvent {
	return &wire.LifecycleEvent{EventId: "event-1", EventType: lifecycle.OrderWorkflowType, SchemaVersion: 1, Producer: "order-service", CorrelationId: "request-1", CausationId: "request-1", OccurredAt: timestamppb.New(time.Unix(1700000000, 123)), Payload: &wire.LifecycleEvent_OrderWorkflow{OrderWorkflow: &wire.OrderWorkflowSnapshot{RequestId: "request-1", OrderId: "order-1", UserId: "alice", WorkflowVersion: version, State: wire.OrderWorkflowState_ORDER_WORKFLOW_STATE_RESERVATION_PENDING, Symbol: "BTC-USDT", Side: "buy", OrderType: "market", Quantity: "9007199254740993", ReserveAssetId: "USDT", ReserveAmountAtomic: "123"}}}
}

func TestSnapshotBSONPreservesExactFactsAndUnknownFields(t *testing.T) {
	event := testEvent(1)
	unknown := protowire.AppendTag(nil, 100, protowire.BytesType)
	unknown = protowire.AppendString(unknown, "future field")
	event.ProtoReflect().SetUnknown(unknown)
	doc, err := encodeSnapshot(event)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	var decoded snapshotDocument
	if err := bson.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	fact, err := lifecycle.Decode(decoded.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Version != 1 || decoded.UserID != "alice" || fact.GetOrderWorkflow().Quantity != "9007199254740993" || !bytes.Equal(fact.ProtoReflect().GetUnknown(), unknown) || fact.OccurredAt.Nanos != 123 {
		t.Fatal("BSON lost workflow coordinates, exact values or additive fields")
	}
	if _, err := encodeSnapshot(nil); err == nil {
		t.Fatal("accepted invalid snapshot")
	}
}

func TestStoreConfigurationAndTargetIsolation(t *testing.T) {
	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://127.0.0.1:1").SetServerSelectionTimeout(time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Disconnect(context.Background())
	target := workflow.Target{Generation: "orders-v1", Mode: workflow.Shadow}
	for _, tc := range []struct {
		client  *mongo.Client
		db      string
		target  workflow.Target
		timeout time.Duration
	}{
		{nil, "test", target, time.Second}, {client, "", target, time.Second},
		{client, "test", workflow.Target{Generation: "../bad", Mode: workflow.Shadow}, time.Second},
		{client, "test", target, 0}, {client, "test", target, 31 * time.Second},
	} {
		if _, err := New(tc.client, tc.db, tc.target, tc.timeout); err == nil {
			t.Fatal("accepted invalid store configuration")
		}
	}
	s, err := New(client, "test", target, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	other, err := New(client, "test", workflow.Target{Generation: target.Generation, Mode: workflow.Rebuild}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if s.snapshots.Name() == other.snapshots.Name() || s.inbox.Name() == other.inbox.Name() || s.snapshots.Name() == s.inbox.Name() {
		t.Fatal("target collections overlap")
	}
	if err := s.Within(context.Background(), other.target, func(workflow.Transaction) error { t.Fatal("mismatched target entered transaction"); return nil }); err == nil {
		t.Fatal("accepted wrong target")
	}
	if err := s.Within(context.Background(), target, nil); err == nil {
		t.Fatal("accepted nil callback")
	}
}
