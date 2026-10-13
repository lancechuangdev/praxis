package adapters

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
	riskv1 "praxis/api/gen/risk/v1"
	"praxis/orderservice/internal/order"
)

type riskClient interface {
	CheckOrder(context.Context, *riskv1.CheckOrderRequest, ...grpc.CallOption) (*riskv1.CheckOrderResponse, error)
}

type GRPCRisk struct {
	connection *grpc.ClientConn
	client     riskClient
	health     grpc_health_v1.HealthClient
	timeout    time.Duration
}

func NewGRPCRisk(address string, timeout time.Duration) (*GRPCRisk, error) {
	connection, err := grpc.NewClient(address, grpcClientOptions()...)
	if err != nil {
		return nil, err
	}
	return &GRPCRisk{connection: connection, client: riskv1.NewRiskServiceClient(connection), health: grpc_health_v1.NewHealthClient(connection), timeout: timeout}, nil
}

func (r *GRPCRisk) CheckOrder(ctx context.Context, request order.Request) (order.RiskDecision, error) {
	callCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	correlationID, causationID := requestContext(request)
	response, err := r.client.CheckOrder(callCtx, &riskv1.CheckOrderRequest{RequestId: request.RequestID, OrderId: request.OrderID, UserId: request.UserID, Symbol: request.Symbol, Side: request.Side, OrderType: request.OrderType, Quantity: request.Quantity, Price: request.Price, ReserveAssetId: request.ReserveAssetID, ReserveAmountAtomic: request.ReserveAmountAtomic, CorrelationId: correlationID, CausationId: causationID})
	if err != nil {
		return order.RiskDecision{}, err
	}
	switch response.GetDecision() {
	case riskv1.RiskDecision_RISK_DECISION_APPROVED:
		return order.RiskDecision{Approved: true, DecisionID: response.GetDecisionId(), ReasonCode: response.GetReasonCode(), PolicyVersion: response.GetPolicyVersion()}, nil
	case riskv1.RiskDecision_RISK_DECISION_REJECTED:
		return order.RiskDecision{Approved: false, DecisionID: response.GetDecisionId(), ReasonCode: response.GetReasonCode(), PolicyVersion: response.GetPolicyVersion()}, nil
	default:
		return order.RiskDecision{}, errors.New("Risk returned an unspecified decision")
	}
}

func (r *GRPCRisk) Ready(ctx context.Context) error {
	callCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	response, err := r.health.Check(callCtx, &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		return err
	}
	if response.Status != grpc_health_v1.HealthCheckResponse_SERVING {
		return fmt.Errorf("risk health status is %s", response.Status)
	}
	return nil
}
func (r *GRPCRisk) Close() error { return r.connection.Close() }
