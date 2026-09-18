package adapters

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	ledgerv1 "praxis/orderservice/gen/ledger/v1"
	"praxis/orderservice/internal/order"
)

type GRPCLedger struct {
	connection *grpc.ClientConn
	client     ledgerv1.LedgerServiceClient
	health     grpc_health_v1.HealthClient
	timeout    time.Duration
}

func NewGRPCLedger(address string, timeout time.Duration) (*GRPCLedger, error) {
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &GRPCLedger{connection: connection, client: ledgerv1.NewLedgerServiceClient(connection), health: grpc_health_v1.NewHealthClient(connection), timeout: timeout}, nil
}

func (l *GRPCLedger) Ready(ctx context.Context) error {
	callCtx, cancel := context.WithTimeout(ctx, l.timeout)
	defer cancel()
	response, err := l.health.Check(callCtx, &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		return err
	}
	if response.Status != grpc_health_v1.HealthCheckResponse_SERVING {
		return fmt.Errorf("ledger health status is %s", response.Status)
	}
	return nil
}

func (l *GRPCLedger) Reserve(ctx context.Context, req order.Request) (order.Reservation, error) {
	ctx = outgoingTraceContext(ctx, req.TraceParent, req.TraceState)
	callCtx, cancel := context.WithTimeout(ctx, l.timeout)
	defer cancel()
	correlationID, causationID := requestContext(req)
	response, err := l.client.ReserveForOrder(callCtx, &ledgerv1.ReserveForOrderRequest{
		CommandId: req.RequestID, OrderId: req.OrderID, UserId: req.UserID,
		AssetId: req.ReserveAssetID, AmountAtomic: req.ReserveAmountAtomic,
		CorrelationId: correlationID, CausationId: causationID,
		OccurredAt: time.Now().UTC().Format(time.RFC3339Nano), TraceParent: req.TraceParent, TraceState: req.TraceState,
	})
	if err != nil {
		return order.Reservation{}, err
	}
	return order.Reservation{ID: response.ReservationId, BalanceVersion: response.BalanceVersion, Replay: response.IdempotentReplay}, nil
}

func outgoingTraceContext(ctx context.Context, traceParent, traceState string) context.Context {
	if traceParent == "" {
		return ctx
	}
	pairs := []string{"traceparent", traceParent}
	if traceState != "" {
		pairs = append(pairs, "tracestate", traceState)
	}
	return metadata.AppendToOutgoingContext(ctx, pairs...)
}

func requestContext(req order.Request) (string, string) {
	correlationID := req.CorrelationID
	if correlationID == "" {
		correlationID = req.RequestID
	}
	causationID := req.CausationID
	if causationID == "" {
		causationID = req.RequestID
	}
	return correlationID, causationID
}
func (l *GRPCLedger) Close() error { return l.connection.Close() }

type MockLedger struct{ Latency time.Duration }

func (MockLedger) Ready(context.Context) error { return nil }

func (m MockLedger) Reserve(ctx context.Context, req order.Request) (order.Reservation, error) {
	if err := wait(ctx, m.Latency); err != nil {
		return order.Reservation{}, err
	}
	hash := sha256.Sum256([]byte(req.OrderID))
	return order.Reservation{ID: "mock_rsv_" + hex.EncodeToString(hash[:8]), BalanceVersion: 1}, nil
}
func (MockLedger) Close() error { return nil }

func wait(ctx context.Context, duration time.Duration) error {
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
