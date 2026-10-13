package observability

import "sync/atomic"

type Metrics struct {
	ReserveRequests                atomic.Uint64
	ReserveFailures                atomic.Uint64
	ReserveNS                      atomic.Uint64
	ReserveDuration                DurationHistogram
	DLQHandoffFailures             atomic.Uint64
	DLQInvalidEnvelopes            atomic.Uint64
	DLQInvalidEventPayloads        atomic.Uint64
	DLQUnsupportedEventTypes       atomic.Uint64
	DLQUnknownDeterministicFailure atomic.Uint64
}

func (m *Metrics) RecordDLQHandoff(failureCode string) {
	switch failureCode {
	case "invalid_envelope":
		m.DLQInvalidEnvelopes.Add(1)
	case "invalid_event_payload":
		m.DLQInvalidEventPayloads.Add(1)
	case "unsupported_event_type":
		m.DLQUnsupportedEventTypes.Add(1)
	default:
		m.DLQUnknownDeterministicFailure.Add(1)
	}
}
