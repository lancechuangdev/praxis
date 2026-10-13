package risk

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash/fnv"
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	riskv1 "praxis/api/gen/risk/v1"
)

type Metrics struct {
	Requests, Approved, Rejected, Failed atomic.Uint64
	DurationNS                           atomic.Uint64
}

type Service struct {
	riskv1.UnimplementedRiskServiceServer
	PolicyVersion string
	RejectBPS     int
	Latency       time.Duration
	Metrics       *Metrics
}

func (s *Service) CheckOrder(ctx context.Context, request *riskv1.CheckOrderRequest) (*riskv1.CheckOrderResponse, error) {
	started := time.Now()
	s.Metrics.Requests.Add(1)
	defer func() { s.Metrics.DurationNS.Add(uint64(time.Since(started))) }()
	if err := validate(request); err != nil {
		s.Metrics.Failed.Add(1)
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := wait(ctx, s.Latency); err != nil {
		s.Metrics.Failed.Add(1)
		return nil, status.FromContextError(err).Err()
	}
	decision := riskv1.RiskDecision_RISK_DECISION_APPROVED
	reason := ""
	if rejected(request.GetRequestId(), s.RejectBPS) {
		decision = riskv1.RiskDecision_RISK_DECISION_REJECTED
		reason = "configured_sample_rejection"
		s.Metrics.Rejected.Add(1)
	} else {
		s.Metrics.Approved.Add(1)
	}
	return &riskv1.CheckOrderResponse{Decision: decision, DecisionId: decisionID(s.PolicyVersion, request.GetRequestId()), ReasonCode: reason, PolicyVersion: s.PolicyVersion}, nil
}

func validate(r *riskv1.CheckOrderRequest) error {
	if r == nil {
		return errors.New("request is required")
	}
	values := []string{r.GetRequestId(), r.GetOrderId(), r.GetUserId(), r.GetSymbol(), r.GetSide(), r.GetOrderType(), r.GetQuantity(), r.GetReserveAssetId(), r.GetReserveAmountAtomic()}
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			return errors.New("required order fields must be non-empty")
		}
	}
	return nil
}

func rejected(key string, bps int) bool {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32()%10000) < bps
}
func decisionID(policy, request string) string {
	sum := sha256.Sum256([]byte(policy + "\x00" + request))
	return "risk_" + hex.EncodeToString(sum[:16])
}
func wait(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
