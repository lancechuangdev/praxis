package kafkaconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/segmentio/kafka-go"
	"praxis/projectionservice/internal/workflow"
)

func TestFailureDiagnosticsRetainBlockedSourceAndPriorCommit(t *testing.T) {
	r := &fakeReader{messages: []kafka.Message{message(10), message(11), message(12)}}
	calls := 0
	c := consumer(r, func(context.Context, string, []byte, []byte) (workflow.Outcome, error) {
		calls++
		if calls == 1 {
			return workflow.Applied, nil
		}
		return "", fmt.Errorf("sensitive details: %w", workflow.ErrGap)
	})
	if err := c.Run(context.Background()); !errors.Is(err, workflow.ErrGap) {
		t.Fatal(err)
	}
	status := c.Status()
	if status.Stage != "stopped" || status.Current == nil || status.Current.Offset != 11 || status.LastCommitted == nil || status.LastCommitted.Offset != 10 || status.Failure == nil || status.Failure.Stage != "process" || status.Failure.Reason != "gap" {
		t.Fatalf("unexpected status %+v", status)
	}
	raw, err := json.Marshal(status)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "sensitive") || strings.Contains(string(raw), "order-1") || strings.Contains(string(raw), "fact") {
		t.Fatal("status leaked data")
	}
	if !strings.Contains(c.Prometheus(), `projection_workflow_failure{stage="process",reason="gap"} 1`) {
		t.Fatal(c.Prometheus())
	}
	// Caller mutation must not alter concurrently served internal diagnostics.
	status.Current.Offset = 999
	status.LastCommitted.Offset = 999
	status.Failure.Reason = "sensitive"
	if c.Status().Current.Offset != 11 || c.Status().LastCommitted.Offset != 10 || c.Status().Failure.Reason != "gap" {
		t.Fatal("status aliases caller memory")
	}
}

func TestCommitAmbiguityIsNotReportedAsAcknowledged(t *testing.T) {
	r := &fakeReader{messages: []kafka.Message{message(10)}, commit: func(context.Context, kafka.Message) error { return context.DeadlineExceeded }}
	c := consumer(r, func(context.Context, string, []byte, []byte) (workflow.Outcome, error) { return workflow.Applied, nil })
	_ = c.Run(context.Background())
	status := c.Status()
	if status.LastCommitted != nil || status.Current == nil || status.Current.Offset != 10 || status.Failure.Stage != "commit" || status.Failure.Reason != "deadline" {
		t.Fatalf("ambiguous commit status %+v", status)
	}
}

func TestFetchFailureDoesNotBlamePreviouslyCommittedRecord(t *testing.T) {
	r := &fakeReader{messages: []kafka.Message{message(10)}}
	c := consumer(r, func(context.Context, string, []byte, []byte) (workflow.Outcome, error) { return workflow.Applied, nil })
	_ = c.Run(context.Background())
	status := c.Status()
	if status.Current != nil || status.LastCommitted == nil || status.LastCommitted.Offset != 10 || status.Failure.Stage != "fetch" || status.Failure.Reason != "closed" {
		t.Fatalf("fetch failure status %+v", status)
	}
}

func TestFailureClassificationIsBoundedAndSanitized(t *testing.T) {
	for _, tc := range []struct {
		err    error
		reason string
	}{
		{workflow.ErrInvalid, "invalid"}, {workflow.ErrStale, "stale"}, {workflow.ErrConflict, "conflict"},
		{kafka.SASLAuthenticationFailed, "authentication"}, {kafka.GroupAuthorizationFailed, "authorization"}, {kafka.TopicAuthorizationFailed, "authorization"},
		{context.Canceled, "cancelled"}, {io.EOF, "closed"}, {errors.New("private secret"), "error"},
	} {
		if reason := failureReason(fmt.Errorf("sensitive: %w", tc.err)); reason != tc.reason {
			t.Fatalf("reason=%s", reason)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := consumer(&fakeReader{}, func(context.Context, string, []byte, []byte) (workflow.Outcome, error) {
		t.Fatal("unexpected call")
		return "", nil
	})
	_ = c.Run(ctx)
	if c.Status().Failure != nil || strings.Contains(c.Prometheus(), "projection_workflow_failure{") {
		t.Fatal("normal cancellation reported as failure")
	}
}
