package risk

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	riskv1 "praxis/api/gen/risk/v1"
)

func request() *riskv1.CheckOrderRequest {
	return &riskv1.CheckOrderRequest{RequestId: "request-1", OrderId: "order-1", UserId: "alice", Symbol: "BTC-USDT", Side: "buy", OrderType: "limit", Quantity: "1", Price: "70000", ReserveAssetId: "asset_usdt", ReserveAmountAtomic: "70000"}
}

func TestCheckOrderDeterministicDecision(t *testing.T) {
	metrics := &Metrics{}
	service := &Service{PolicyVersion: "policy-1", Metrics: metrics}
	first, err := service.CheckOrder(context.Background(), request())
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.CheckOrder(context.Background(), request())
	if err != nil {
		t.Fatal(err)
	}
	if first.GetDecision() != riskv1.RiskDecision_RISK_DECISION_APPROVED || first.GetDecisionId() != second.GetDecisionId() || first.GetPolicyVersion() != "policy-1" {
		t.Fatalf("decisions=%v %v", first, second)
	}
	if metrics.Requests.Load() != 2 || metrics.Approved.Load() != 2 {
		t.Fatalf("metrics=%+v", metrics)
	}
}

func TestCheckOrderRejectsAndValidates(t *testing.T) {
	service := &Service{PolicyVersion: "policy-1", RejectBPS: 10000, Metrics: &Metrics{}}
	response, err := service.CheckOrder(context.Background(), request())
	if err != nil {
		t.Fatal(err)
	}
	if response.GetDecision() != riskv1.RiskDecision_RISK_DECISION_REJECTED || response.GetReasonCode() == "" {
		t.Fatalf("response=%v", response)
	}
	_, err = service.CheckOrder(context.Background(), &riskv1.CheckOrderRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("error=%v", err)
	}
}

func TestCheckOrderHonorsDeadline(t *testing.T) {
	service := &Service{PolicyVersion: "policy-1", Latency: time.Second, Metrics: &Metrics{}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	_, err := service.CheckOrder(ctx, request())
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("error=%v", err)
	}
}
