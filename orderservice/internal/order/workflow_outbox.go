package order

import (
	"errors"
	"fmt"

	lifecycle "praxis/api/events/lifecycle/v1"
	wire "praxis/api/gen/events/lifecycle/v1"

	"google.golang.org/protobuf/encoding/protojson"
)

// EncodeWorkflowOutbox converts only the new Order-owned source outbox's
// immutable ProtoJSON snapshot into its deterministic Protobuf wire record.
// It is deliberately separate from all transitional JSON event decoding.
func EncodeWorkflowOutbox(eventID, orderID string, version int64, snapshot []byte) ([]byte, error) {
	if len(snapshot) == 0 || len(snapshot) > lifecycle.MaxEventBytes {
		return nil, errors.New("invalid workflow source snapshot size")
	}
	event := new(wire.LifecycleEvent)
	if err := protojson.Unmarshal(snapshot, event); err != nil {
		return nil, fmt.Errorf("decode workflow source snapshot: %w", err)
	}
	workflow := event.GetOrderWorkflow()
	if workflow == nil || event.EventId != eventID || workflow.OrderId != orderID || version <= 0 || workflow.WorkflowVersion != uint64(version) {
		return nil, errors.New("workflow source coordinates do not match payload")
	}
	return lifecycle.Encode(event)
}
