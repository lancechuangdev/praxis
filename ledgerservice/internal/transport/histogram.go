package transport

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

var latencyBounds = [...]float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}

type DurationHistogram struct {
	buckets [len(latencyBounds)]atomic.Uint64
	count   atomic.Uint64
	sumNS   atomic.Uint64
}

func (h *DurationHistogram) Observe(d time.Duration) {
	for i, bound := range latencyBounds {
		if d.Seconds() <= bound {
			h.buckets[i].Add(1)
		}
	}
	h.count.Add(1)
	h.sumNS.Add(uint64(d))
}

func (h *DurationHistogram) Prometheus(name string) string {
	var b strings.Builder
	for i, bound := range latencyBounds {
		fmt.Fprintf(&b, "%s_bucket{le=\"%g\"} %d\n", name, bound, h.buckets[i].Load())
	}
	count := h.count.Load()
	fmt.Fprintf(&b, "%s_bucket{le=\"+Inf\"} %d\n%s_sum %.9f\n%s_count %d\n", name, count, name, float64(h.sumNS.Load())/1e9, name, count)
	return b.String()
}
