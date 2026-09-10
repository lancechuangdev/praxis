package adapters

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	ledgerv1 "praxis/orderservice/gen/ledger/v1"
	"praxis/orderservice/internal/order"
)

type GRPCLedger struct {
	connection *grpc.ClientConn
	client     ledgerv1.LedgerServiceClient
	timeout    time.Duration
}

func NewGRPCLedger(address string, timeout time.Duration) (*GRPCLedger, error) {
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &GRPCLedger{connection: connection, client: ledgerv1.NewLedgerServiceClient(connection), timeout: timeout}, nil
}

func (l *GRPCLedger) Reserve(ctx context.Context, req order.Request) (order.Reservation, error) {
	callCtx, cancel := context.WithTimeout(ctx, l.timeout)
	defer cancel()
	response, err := l.client.ReserveForOrder(callCtx, &ledgerv1.ReserveForOrderRequest{
		CommandId: req.RequestID, OrderId: req.OrderID, UserId: req.UserID,
		AssetId: req.ReserveAssetID, AmountAtomic: req.ReserveAmountAtomic,
		CorrelationId: req.RequestID, CausationId: req.RequestID,
		OccurredAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return order.Reservation{}, err
	}
	return order.Reservation{ID: response.ReservationId, BalanceVersion: response.BalanceVersion, Replay: response.IdempotentReplay}, nil
}
func (l *GRPCLedger) Close() error { return l.connection.Close() }

type MockLedger struct{ Latency time.Duration }

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
