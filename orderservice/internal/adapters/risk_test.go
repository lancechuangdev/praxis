package adapters

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc"
	riskv1 "praxis/api/gen/risk/v1"
	"praxis/orderservice/internal/order"
)

type fakeRiskClient struct {
	response *riskv1.CheckOrderResponse
	request  *riskv1.CheckOrderRequest
}

func (f *fakeRiskClient) CheckOrder(_ context.Context, request *riskv1.CheckOrderRequest, _ ...grpc.CallOption) (*riskv1.CheckOrderResponse, error) {
	f.request = request
	return f.response, nil
}

func TestGRPCRiskMapsRequestAndDecision(t *testing.T) {
	client := &fakeRiskClient{response: &riskv1.CheckOrderResponse{Decision: riskv1.RiskDecision_RISK_DECISION_REJECTED, DecisionId: "decision-1", ReasonCode: "account_limit", PolicyVersion: "policy-1"}}
	risk := &GRPCRisk{client: client}
	decision, err := risk.CheckOrder(context.Background(), order.Request{RequestID: "request-1", OrderID: "order-1", UserID: "alice", Symbol: "BTC-USDT", Side: "buy", OrderType: "limit", Quantity: "1", Price: "70000", ReserveAssetID: "asset_usdt", ReserveAmountAtomic: "70000"})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Approved || decision.DecisionID != "decision-1" || client.request.GetCorrelationId() != "request-1" || client.request.GetReserveAmountAtomic() != "70000" {
		t.Fatalf("decision=%+v request=%v", decision, client.request)
	}
}

func TestGRPCRiskRejectsUnspecifiedDecision(t *testing.T) {
	risk := &GRPCRisk{client: &fakeRiskClient{response: &riskv1.CheckOrderResponse{}}}
	if _, err := risk.CheckOrder(context.Background(), order.Request{}); err == nil {
		t.Fatal("expected unspecified decision error")
	}
}

type deadlineRiskClient struct{}

func (deadlineRiskClient) CheckOrder(ctx context.Context, _ *riskv1.CheckOrderRequest, _ ...grpc.CallOption) (*riskv1.CheckOrderResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestGRPCRiskAppliesStrictDeadline(t *testing.T) {
	risk := &GRPCRisk{client: deadlineRiskClient{}, timeout: time.Millisecond}
	_, err := risk.CheckOrder(context.Background(), order.Request{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error=%v", err)
	}
}
