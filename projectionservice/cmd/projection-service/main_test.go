package main

import (
	"context"
	"io"
	"log/slog"
	"testing"
)

func TestDisabledRuntimeStartsAndStopsWithoutExternalConfiguration(t *testing.T) {
	t.Setenv("PROJECTION_WORKFLOW_ENABLED", "false")
	t.Setenv("PROJECTION_HTTP_ADDRESS", "127.0.0.1:0")
	t.Setenv("PROJECTION_MONGODB_URI", "invalid")
	t.Setenv("PROJECTION_KAFKA_BROKERS", "invalid")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := run(ctx, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatal(err)
	}
}
