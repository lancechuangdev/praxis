package transport

import "sync/atomic"

type Metrics struct {
	ReserveRequests atomic.Uint64
	ReserveFailures atomic.Uint64
	ReserveNS       atomic.Uint64
	ReserveDuration DurationHistogram
}
