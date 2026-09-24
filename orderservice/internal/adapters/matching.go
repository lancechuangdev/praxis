package adapters

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	matchingv1 "praxis/orderservice/gen/matching/v1"
	"praxis/orderservice/internal/order"
)

type GRPCMatching struct {
	connection *grpc.ClientConn
	client     matchingv1.MatchingEngineClient
	health     grpc_health_v1.HealthClient
	timeout    time.Duration
}

func NewGRPCMatching(address string, timeout time.Duration) (*GRPCMatching, error) {
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
	if err != nil {
		return nil, err
	}
	return &GRPCMatching{connection: connection, client: matchingv1.NewMatchingEngineClient(connection), health: grpc_health_v1.NewHealthClient(connection), timeout: timeout}, nil
}

func (m *GRPCMatching) Ready(ctx context.Context) error {
	callCtx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()
	response, err := m.health.Check(callCtx, &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		return err
	}
	if response.Status != grpc_health_v1.HealthCheckResponse_SERVING {
		return fmt.Errorf("matching health status is %s", response.Status)
	}
	return nil
}

func (m *GRPCMatching) Submit(ctx context.Context, req order.Request, reservation order.Reservation) (order.MatchResult, error) {
	callCtx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()
	correlationID, causationID := requestContext(req)
	response, err := m.client.SubmitOrder(callCtx, &matchingv1.SubmitOrderRequest{
		RequestId: req.RequestID, OrderId: req.OrderID, UserId: req.UserID,
		Symbol: req.Symbol, Side: req.Side, OrderType: req.OrderType,
		Quantity: req.Quantity, Price: req.Price, ReservationId: reservation.ID,
		CorrelationId: correlationID,
		CausationId:   causationID,
		TraceParent:   req.TraceParent,
		TraceState:    req.TraceState,
	})
	if err != nil {
		return order.MatchResult{}, err
	}
	return order.MatchResult{Status: response.Status, EngineSequence: response.EngineSequence}, nil
}
func (m *GRPCMatching) Close() error { return m.connection.Close() }

type MockMatching struct {
	Latency  time.Duration
	sequence atomic.Int64
}

func (*MockMatching) Ready(context.Context) error { return nil }

func (m *MockMatching) Submit(ctx context.Context, _ order.Request, _ order.Reservation) (order.MatchResult, error) {
	if err := wait(ctx, m.Latency); err != nil {
		return order.MatchResult{}, err
	}
	return order.MatchResult{Status: "accepted", EngineSequence: m.sequence.Add(1)}, nil
}
func (*MockMatching) Close() error { return nil }
