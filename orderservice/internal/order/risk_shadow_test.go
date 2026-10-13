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

func TestRiskModeSelectionSupportsImmediateRollback(t *testing.T) {
	embedded := stubRisk{decision: RiskDecision{Approved: true, PolicyVersion: "embedded"}}
	remote := stubRisk{decision: RiskDecision{Approved: false, PolicyVersion: "remote"}}
	authoritative, err := SelectRiskChecker("authoritative", embedded, remote, 1, &Metrics{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	decision, err := authoritative.CheckOrder(context.Background(), Request{})
	if err != nil || decision.Approved {
		t.Fatalf("authoritative=%+v err=%v", decision, err)
	}
	rolledBack, err := SelectRiskChecker("off", embedded, nil, 1, &Metrics{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	decision, err = rolledBack.CheckOrder(context.Background(), Request{})
	if err != nil || !decision.Approved || decision.PolicyVersion != "embedded" {
		t.Fatalf("rollback=%+v err=%v", decision, err)
	}
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
