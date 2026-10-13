package observability

import "testing"

func TestRecordDLQHandoffUsesBoundedFailureCodes(t *testing.T) {
	metrics := &Metrics{}
	metrics.RecordDLQHandoff("invalid_envelope")
	metrics.RecordDLQHandoff("invalid_event_payload")
	metrics.RecordDLQHandoff("unsupported_event_type")
	metrics.RecordDLQHandoff("unexpected_new_code")

	if metrics.DLQInvalidEnvelopes.Load() != 1 || metrics.DLQInvalidEventPayloads.Load() != 1 || metrics.DLQUnsupportedEventTypes.Load() != 1 || metrics.DLQUnknownDeterministicFailure.Load() != 1 {
		t.Fatalf("unexpected DLQ counters: %+v", metrics)
	}
}
