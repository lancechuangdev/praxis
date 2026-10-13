package order

import (
	"context"
	"errors"
	"testing"
	"time"
)

type stubRisk struct {
	decision RiskDecision
	err      error
	block    <-chan struct{}
}

func (s stubRisk) CheckOrder(context.Context, Request) (RiskDecision, error) {
	if s.block != nil {
		<-s.block
	}
	return s.decision, s.err
}

func TestShadowRiskMismatchDoesNotChangePrimaryDecision(t *testing.T) {
	metrics := &Metrics{}
	checker := NewShadowRiskChecker(stubRisk{decision: RiskDecision{Approved: true, PolicyVersion: "primary"}}, stubRisk{decision: RiskDecision{Approved: false, ReasonCode: "limit", PolicyVersion: "shadow"}}, 1, metrics, nil)
	decision, err := checker.CheckOrder(context.Background(), Request{RequestID: "request-1"})
	if err != nil || !decision.Approved {
		t.Fatalf("decision=%+v err=%v", decision, err)
	}
	eventually(t, func() bool { return metrics.RiskShadowMismatches.Load() == 1 })
}

func TestShadowRiskFailureAndSaturationDoNotAffectPrimary(t *testing.T) {
	metrics := &Metrics{}
	block := make(chan struct{})
	checker := NewShadowRiskChecker(stubRisk{decision: RiskDecision{Approved: true}}, stubRisk{err: errors.New("unavailable"), block: block}, 1, metrics, nil)
	for i := 0; i < 2; i++ {
		decision, err := checker.CheckOrder(context.Background(), Request{})
		if err != nil || !decision.Approved {
			t.Fatal("shadow affected primary")
		}
	}
	if metrics.RiskShadowDropped.Load() != 1 {
		t.Fatalf("dropped=%d", metrics.RiskShadowDropped.Load())
	}
	close(block)
	eventually(t, func() bool { return metrics.RiskShadowErrors.Load() == 1 })
}

func eventually(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met")
		}
		time.Sleep(time.Millisecond)
	}
}
