package order

import (
	"context"
	"log/slog"
)

// ShadowRiskChecker always returns the primary decision. Shadow failures,
// latency, and disagreement are observed asynchronously and cannot affect
// admission. The semaphore prevents an unavailable shadow from accumulating
// unbounded goroutines.
type ShadowRiskChecker struct {
	Primary RiskChecker
	Shadow  RiskChecker
	Metrics *Metrics
	Log     *slog.Logger
	sem     chan struct{}
}

func NewShadowRiskChecker(primary, shadow RiskChecker, maxInFlight int, metrics *Metrics, log *slog.Logger) *ShadowRiskChecker {
	return &ShadowRiskChecker{Primary: primary, Shadow: shadow, Metrics: metrics, Log: log, sem: make(chan struct{}, maxInFlight)}
}

func (s *ShadowRiskChecker) CheckOrder(ctx context.Context, request Request) (RiskDecision, error) {
	primary, err := s.Primary.CheckOrder(ctx, request)
	if err != nil {
		return RiskDecision{}, err
	}
	select {
	case s.sem <- struct{}{}:
		s.Metrics.RiskShadowAttempts.Add(1)
		go s.compare(context.WithoutCancel(ctx), request, primary)
	default:
		s.Metrics.RiskShadowDropped.Add(1)
	}
	return primary, nil
}

func (s *ShadowRiskChecker) compare(ctx context.Context, request Request, primary RiskDecision) {
	defer func() { <-s.sem }()
	shadow, err := s.Shadow.CheckOrder(ctx, request)
	if err != nil {
		s.Metrics.RiskShadowErrors.Add(1)
		if s.Log != nil {
			s.Log.WarnContext(ctx, "risk shadow check failed", "request_id", request.RequestID, "error", err)
		}
		return
	}
	if primary.Approved == shadow.Approved && primary.ReasonCode == shadow.ReasonCode {
		s.Metrics.RiskShadowMatches.Add(1)
		return
	}
	s.Metrics.RiskShadowMismatches.Add(1)
	if s.Log != nil {
		s.Log.ErrorContext(ctx, "risk shadow decision mismatch", "request_id", request.RequestID, "primary_approved", primary.Approved, "primary_reason", primary.ReasonCode, "primary_policy", primary.PolicyVersion, "shadow_approved", shadow.Approved, "shadow_reason", shadow.ReasonCode, "shadow_policy", shadow.PolicyVersion, "shadow_decision_id", shadow.DecisionID)
	}
}
