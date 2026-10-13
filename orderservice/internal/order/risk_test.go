package order

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestEmbeddedRiskCheckerPreservesDeterministicPolicy(t *testing.T) {
	request := validRequest()
	approved, err := (EmbeddedRiskChecker{}).CheckOrder(context.Background(), request)
	if err != nil || !approved.Approved || approved.PolicyVersion != EmbeddedRiskPolicyVersion {
		t.Fatalf("approved=%+v err=%v", approved, err)
	}
	rejected, err := (EmbeddedRiskChecker{RejectBPS: 10000}).CheckOrder(context.Background(), request)
	if err != nil || rejected.Approved || rejected.ReasonCode != "configured_sample_rejection" {
		t.Fatalf("rejected=%+v err=%v", rejected, err)
	}
}

func TestEmbeddedRiskCheckerHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := (EmbeddedRiskChecker{Latency: time.Second}).CheckOrder(ctx, validRequest())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
}

func TestEmbeddedRiskPolicyGoldenDecisions(t *testing.T) {
	tests := []struct {
		id       string
		approved bool
	}{{"request-1", true}, {"request-2", true}, {"order-risk-alpha", true}, {"order-risk-beta", false}, {"order-risk-gamma", true}}
	checker := EmbeddedRiskChecker{RejectBPS: 5000}
	for _, test := range tests {
		t.Run(test.id, func(t *testing.T) {
			decision, err := checker.CheckOrder(context.Background(), Request{RequestID: test.id})
			if err != nil {
				t.Fatal(err)
			}
			if decision.Approved != test.approved {
				t.Fatalf("approved=%v want %v", decision.Approved, test.approved)
			}
		})
	}
}
