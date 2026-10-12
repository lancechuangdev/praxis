package store

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func TestDurableTraceContextUsesActiveSpan(t *testing.T) {
	previous := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}))
	t.Cleanup(func() { otel.SetTextMapPropagator(previous) })
	traceState, err := trace.ParseTraceState("vendor=value")
	if err != nil {
		t.Fatal(err)
	}
	spanContext := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, TraceFlags: trace.FlagsSampled,
		TraceState: traceState,
	})

	parent, state := durableTraceContext(trace.ContextWithSpanContext(context.Background(), spanContext))

	if parent != "00-01000000000000000000000000000000-0200000000000000-01" || state != "vendor=value" {
		t.Fatalf("durable context=%q,%q", parent, state)
	}
}
