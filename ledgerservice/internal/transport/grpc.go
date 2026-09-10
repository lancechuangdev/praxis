package transport

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	ledgerv1 "praxis/ledgerservice/gen/ledger/v1"
	"praxis/ledgerservice/internal/ledger"
)

type GRPC struct {
	ledgerv1.UnimplementedLedgerServiceServer
	Store ledger.Store
}

func eventTime(raw string) (time.Time, error) {
	if raw == "" {
		return time.Now().UTC(), nil
	}
	return time.Parse(time.RFC3339Nano, raw)
}
func grpcErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ledger.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, ledger.ErrInsufficientFunds):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, ledger.ErrConflict):
		return status.Error(codes.AlreadyExists, err.Error())
	default:
		return status.Error(codes.InvalidArgument, err.Error())
	}
}

func (s *GRPC) PostDeposit(ctx context.Context, r *ledgerv1.PostDepositRequest) (*ledgerv1.OperationResponse, error) {
	at, e := eventTime(r.OccurredAt)
	if e != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid occurred_at")
	}
	v, e := s.Store.PostDeposit(ctx, ledger.PostDeposit{CommandID: r.CommandId, DepositID: r.DepositId, UserID: r.UserId, AssetID: r.AssetId, CustodyPositionID: r.CustodyPositionId, AmountAtomic: r.AmountAtomic, TargetBucket: r.TargetBucket, SourceSystem: r.SourceSystem, CorrelationID: r.CorrelationId, CausationID: r.CausationId, OccurredAt: at})
	if e != nil {
		return nil, grpcErr(e)
	}
	return &ledgerv1.OperationResponse{JournalId: v.JournalID, IdempotentReplay: v.Replay}, nil
}
func (s *GRPC) ReleaseHold(ctx context.Context, r *ledgerv1.ReleaseHoldRequest) (*ledgerv1.OperationResponse, error) {
	at, e := eventTime(r.OccurredAt)
	if e != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid occurred_at")
	}
	v, e := s.Store.ReleaseHold(ctx, ledger.ReleaseHold{CommandID: r.CommandId, DepositID: r.DepositId, UserID: r.UserId, AssetID: r.AssetId, AmountAtomic: r.AmountAtomic, SourceSystem: r.SourceSystem, CorrelationID: r.CorrelationId, CausationID: r.CausationId, OccurredAt: at})
	if e != nil {
		return nil, grpcErr(e)
	}
	return &ledgerv1.OperationResponse{JournalId: v.JournalID, IdempotentReplay: v.Replay}, nil
}
func (s *GRPC) ReserveForOrder(ctx context.Context, r *ledgerv1.ReserveForOrderRequest) (*ledgerv1.ReservationResponse, error) {
	at, e := eventTime(r.OccurredAt)
	if e != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid occurred_at")
	}
	v, e := s.Store.ReserveForOrder(ctx, ledger.ReserveOrder{CommandID: r.CommandId, OrderID: r.OrderId, UserID: r.UserId, AssetID: r.AssetId, AmountAtomic: r.AmountAtomic, CorrelationID: r.CorrelationId, CausationID: r.CausationId, OccurredAt: at})
	if e != nil {
		return nil, grpcErr(e)
	}
	return reservation(v), nil
}
func (s *GRPC) GetBalance(ctx context.Context, r *ledgerv1.GetBalanceRequest) (*ledgerv1.BalanceResponse, error) {
	v, e := s.Store.GetBalance(ctx, r.UserId, r.AssetId)
	if e != nil {
		return nil, grpcErr(e)
	}
	return &ledgerv1.BalanceResponse{UserAssetAccountId: v.UserAssetAccountID, UserId: v.UserID, AssetId: v.AssetID, AvailableAtomic: v.AvailableAtomic, ReservedAtomic: v.ReservedAtomic, WithdrawalPendingAtomic: v.WithdrawalPendingAtomic, DepositPendingAtomic: v.DepositPendingAtomic, HoldAtomic: v.HoldAtomic, Version: v.Version}, nil
}
func (s *GRPC) GetReservation(ctx context.Context, r *ledgerv1.GetReservationRequest) (*ledgerv1.ReservationResponse, error) {
	v, e := s.Store.GetReservation(ctx, r.OrderId)
	if e != nil {
		return nil, grpcErr(e)
	}
	return reservation(v), nil
}
func reservation(v ledger.Reservation) *ledgerv1.ReservationResponse {
	return &ledgerv1.ReservationResponse{ReservationId: v.ID, OrderId: v.OrderID, Status: v.Status, OriginalAtomic: v.OriginalAtomic, RemainingAtomic: v.RemainingAtomic, BalanceVersion: v.BalanceVersion, IdempotentReplay: v.Replay}
}
