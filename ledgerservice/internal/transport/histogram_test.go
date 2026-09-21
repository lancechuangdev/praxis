package transport

import (
	"strings"
	"testing"
	"time"
)

func TestDurationHistogram(t *testing.T) {
	var h DurationHistogram
	h.Observe(10 * time.Millisecond)
	h.Observe(2 * time.Second)
	got := h.Prometheus("request_duration_seconds")
	for _, want := range []string{`request_duration_seconds_bucket{le="0.01"} 1`, `request_duration_seconds_bucket{le="2.5"} 2`, `request_duration_seconds_bucket{le="+Inf"} 2`, `request_duration_seconds_count 2`} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q in %q", want, got)
		}
	}
}
