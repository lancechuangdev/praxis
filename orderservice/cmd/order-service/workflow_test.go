package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"praxis/orderservice/internal/config"
	"praxis/orderservice/internal/kafkaauth"
)

func TestWorkflowDispatchDisabledAndJoinedShutdown(t *testing.T) {
	worker, stop, err := startWorkflowDispatch(context.Background(), config.Config{DatabaseURL: "invalid"})
	if err != nil || worker != nil {
		t.Fatal(worker, err)
	}
	stop()
	cfg := config.Config{WorkflowDispatch: true, DatabaseURL: "postgres://unused:unused@127.0.0.1:1/unused?sslmode=disable", KafkaBrokers: []string{"127.0.0.1:1"}, KafkaAuth: kafkaauth.Config{Mode: "msk_iam", Region: "us-east-1"}}
	worker, stop, err = startWorkflowDispatch(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	done := make(chan struct{})
	go func() { stop(); stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("workflow shutdown did not join")
	}
	if !strings.Contains(worker.Prometheus(), "order_workflow_dispatch_active 0\n") {
		t.Fatal("worker still active")
	}
}
