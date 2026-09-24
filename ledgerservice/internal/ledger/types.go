package ledger

import (
	"context"
	"errors"
	"math/big"
	"time"
)

var (
	ErrInsufficientFunds = errors.New("insufficient available funds")
	ErrNotFound          = errors.New("not found")
	ErrSequenceGap       = errors.New("matching engine sequence gap")
	ErrConflict          = errors.New("idempotency conflict")
)

type PostDeposit struct {
	CommandID, DepositID, UserID, AssetID, CustodyPositionID, AmountAtomic string
	TargetBucket, SourceSystem, CorrelationID, CausationID                 string
	OccurredAt                                                             time.Time
}

type ReleaseHold struct {
	CommandID, DepositID, UserID, AssetID, AmountAtomic string
	SourceSystem, CorrelationID, CausationID            string
	OccurredAt                                          time.Time
}

type ReserveOrder struct {
	CommandID, OrderID, UserID, AssetID, AmountAtomic   string
	CorrelationID, CausationID, TraceParent, TraceState string
	OccurredAt                                          time.Time
}

type TradeExecuted struct {
	EventID, TradeID, EngineID, Symbol       string
	SequenceNumber                           int64
	BuyerOrderID, SellerOrderID              string
	BuyerUserID, SellerUserID                string
	BaseAssetID, QuoteAssetID                string
	BaseAmountAtomic, QuoteAmountAtomic      string
	BuyerFeeQuoteAtomic, SellerFeeBaseAtomic string
	CorrelationID, CausationID               string
	OccurredAt                               time.Time
}

type CancelOrder struct {
	EventID, OrderID, EngineID, Symbol, CorrelationID, CausationID string
	SequenceNumber                                                 int64
	OccurredAt                                                     time.Time
}

type OperationResult struct {
	JournalID string `json:"journal_id"`
	Replay    bool   `json:"idempotent_replay"`
}

type Reservation struct {
	ID              string `json:"reservation_id"`
	OrderID         string `json:"order_id"`
	Status          string `json:"status"`
	OriginalAtomic  string `json:"original_atomic"`
	RemainingAtomic string `json:"remaining_atomic"`
	BalanceVersion  int64  `json:"balance_version"`
	Replay          bool   `json:"idempotent_replay"`
}

type Balance struct {
	UserAssetAccountID      string `json:"user_asset_account_id"`
	UserID                  string `json:"user_id"`
	AssetID                 string `json:"asset_id"`
	AvailableAtomic         string `json:"available_atomic"`
	ReservedAtomic          string `json:"reserved_atomic"`
	WithdrawalPendingAtomic string `json:"withdrawal_pending_atomic"`
	DepositPendingAtomic    string `json:"deposit_pending_atomic"`
	HoldAtomic              string `json:"hold_atomic"`
	Version                 int64  `json:"version"`
}

func positiveAtomic(value string) error {
	n, ok := new(big.Int).SetString(value, 10)
	if !ok || n.Sign() <= 0 {
		return errors.New("amount_atomic must be a positive base-10 integer")
	}
	return nil
}

func ValidateDeposit(v PostDeposit) error {
	if v.CommandID == "" || v.DepositID == "" || v.UserID == "" || v.AssetID == "" || v.CustodyPositionID == "" || v.SourceSystem == "" {
		return errors.New("command_id, deposit_id, user_id, asset_id, custody_position_id, and source_system are required")
	}
	if v.TargetBucket != "available" && v.TargetBucket != "hold" {
		return errors.New("target_bucket must be available or hold")
	}
	return positiveAtomic(v.AmountAtomic)
}

func ValidateReserve(v ReserveOrder) error {
	if v.CommandID == "" || v.OrderID == "" || v.UserID == "" || v.AssetID == "" {
		return errors.New("command_id, order_id, user_id, and asset_id are required")
	}
	return positiveAtomic(v.AmountAtomic)
}

func ValidateTrade(v TradeExecuted) error {
	if v.EventID == "" || v.TradeID == "" || v.EngineID == "" || v.Symbol == "" || v.SequenceNumber <= 0 || v.BuyerOrderID == "" || v.SellerOrderID == "" || v.BuyerUserID == "" || v.SellerUserID == "" || v.BaseAssetID == "" || v.QuoteAssetID == "" {
		return errors.New("trade identity, engine sequence, users, orders, and assets are required")
	}
	for _, amount := range []string{v.BaseAmountAtomic, v.QuoteAmountAtomic} {
		if err := positiveAtomic(amount); err != nil {
			return err
		}
	}
	for _, fee := range []string{v.BuyerFeeQuoteAtomic, v.SellerFeeBaseAtomic} {
		n, ok := new(big.Int).SetString(fee, 10)
		if !ok || n.Sign() < 0 {
			return errors.New("fees must be non-negative base-10 integers")
		}
	}
	return nil
}

type Store interface {
	PostDeposit(context.Context, PostDeposit) (OperationResult, error)
	ReleaseHold(context.Context, ReleaseHold) (OperationResult, error)
	ReserveForOrder(context.Context, ReserveOrder) (Reservation, error)
	BookTrade(context.Context, TradeExecuted) (OperationResult, error)
	CancelOrder(context.Context, CancelOrder) (OperationResult, error)
	GetBalance(context.Context, string, string) (Balance, error)
	GetReservation(context.Context, string) (Reservation, error)
}
