package store

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// durableTraceContext captures the active operation context for publication
// after the database transaction and request process have ended.
func durableTraceContext(ctx context.Context) (string, string) {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier.Get("traceparent"), carrier.Get("tracestate")
}
