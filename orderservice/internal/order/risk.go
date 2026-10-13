package order

import (
	"context"
	"hash/fnv"
	"time"
)

const EmbeddedRiskPolicyVersion = "embedded-v1"

type RiskDecision struct {
	Approved      bool
	ReasonCode    string
	PolicyVersion string
}

// RiskChecker is the internal seam used by Order admission. Implementations
// must be side-effect free: a decision happens before saga or financial state.
type RiskChecker interface {
	CheckOrder(context.Context, Request) (RiskDecision, error)
}

// EmbeddedRiskChecker preserves the existing deterministic development risk
// behavior behind the contract that a future remote Risk client will satisfy.
type EmbeddedRiskChecker struct {
	Latency   time.Duration
	RejectBPS int
}

func (r EmbeddedRiskChecker) CheckOrder(ctx context.Context, request Request) (RiskDecision, error) {
	if err := waitForRisk(ctx, r.Latency); err != nil {
		return RiskDecision{}, err
	}
	decision := RiskDecision{Approved: true, PolicyVersion: EmbeddedRiskPolicyVersion}
	if riskRejected(request.RequestID, r.RejectBPS) {
		decision.Approved = false
		decision.ReasonCode = "configured_sample_rejection"
	}
	return decision, nil
}

func riskRejected(key string, bps int) bool {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32()%10000) < bps
}

func waitForRisk(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
