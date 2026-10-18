// Package lifecyclev1 validates projection-only lifecycle contracts. It never
// interprets the transitional JSON events or invokes an authoritative service.
package lifecyclev1

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"strings"

	wire "praxis/api/gen/events/lifecycle/v1"

	"google.golang.org/protobuf/proto"
)

const (
	SchemaVersion     = 1
	MaxEventBytes     = 1 << 20
	OrderWorkflowType = "OrderWorkflowSnapshot"
	LedgerJournalType = "LedgerJournalPosted"
)

var integer = regexp.MustCompile(`^(0|[1-9][0-9]*)$`)
var decimal = regexp.MustCompile(`^(0|[1-9][0-9]*)(\.[0-9]+)?$`)

func text(values ...string) bool {
	for _, value := range values {
		if strings.TrimSpace(value) == "" || len(value) > 256 {
			return false
		}
	}
	return true
}

func atomic(raw string, positive bool) (*big.Int, error) {
	if len(raw) > 78 || !integer.MatchString(raw) {
		return nil, errors.New("atomic quantity must be a canonical integer of at most 78 digits")
	}
	value, _ := new(big.Int).SetString(raw, 10)
	if positive && value.Sign() == 0 {
		return nil, errors.New("atomic quantity must be positive")
	}
	return value, nil
}

// Decode bounds allocation before Protobuf decoding and retains unknown fields.
// Unsupported schema versions and unknown payload variants fail closed.
func Decode(raw []byte) (*wire.LifecycleEvent, error) {
	if len(raw) == 0 || len(raw) > MaxEventBytes {
		return nil, errors.New("invalid lifecycle event size")
	}
	event := new(wire.LifecycleEvent)
	if err := proto.Unmarshal(raw, event); err != nil {
		return nil, fmt.Errorf("decode lifecycle event: %w", err)
	}
	if err := Validate(event); err != nil {
		return nil, err
	}
	return event, nil
}

func Encode(event *wire.LifecycleEvent) ([]byte, error) {
	if err := Validate(event); err != nil {
		return nil, err
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(event)
}

// Key returns the domain key only after validating the complete event.
func Key(event *wire.LifecycleEvent) (string, error) {
	if err := Validate(event); err != nil {
		return "", err
	}
	if snapshot := event.GetOrderWorkflow(); snapshot != nil {
		return snapshot.OrderId, nil
	}
	return event.GetLedgerJournal().JournalId, nil
}

func Validate(event *wire.LifecycleEvent) error {
	if event == nil || proto.Size(event) > MaxEventBytes {
		return errors.New("missing or oversized lifecycle event")
	}
	if event.SchemaVersion != SchemaVersion {
		return errors.New("unsupported lifecycle schema version")
	}
	if !text(event.EventId, event.EventType, event.Producer, event.CorrelationId, event.CausationId) || event.OccurredAt == nil || event.OccurredAt.CheckValid() != nil {
		return errors.New("complete lifecycle metadata and valid occurrence time are required")
	}
	switch payload := event.Payload.(type) {
	case *wire.LifecycleEvent_OrderWorkflow:
		if payload == nil {
			return errors.New("missing order payload")
		}
		if event.EventType != OrderWorkflowType || event.Producer != "order-service" {
			return errors.New("order payload has conflicting event type or producer")
		}
		return validateOrder(payload.OrderWorkflow)
	case *wire.LifecycleEvent_LedgerJournal:
		if payload == nil {
			return errors.New("missing journal payload")
		}
		if event.EventType != LedgerJournalType || event.Producer != "ledger-service" {
			return errors.New("journal payload has conflicting event type or producer")
		}
		return validateJournal(payload.LedgerJournal)
	default:
		return errors.New("known typed lifecycle payload is required")
	}
}

func validateOrder(order *wire.OrderWorkflowSnapshot) error {
	if order == nil || !text(order.RequestId, order.OrderId, order.UserId, order.Symbol, order.ReserveAssetId) || order.WorkflowVersion == 0 || order.WorkflowVersion > math.MaxInt64 {
		return errors.New("order identity and positive workflow version are required")
	}
	if _, err := atomic(order.Quantity, true); err != nil {
		return fmt.Errorf("quantity: %w", err)
	}
	if _, err := atomic(order.ReserveAmountAtomic, true); err != nil {
		return fmt.Errorf("reserve amount: %w", err)
	}
	if order.Side != "buy" && order.Side != "sell" {
		return errors.New("invalid order side")
	}
	if order.OrderType != "limit" && order.OrderType != "market" {
		return errors.New("invalid order type")
	}
	if order.OrderType == "limit" {
		if len(order.Price) > 128 || !decimal.MatchString(order.Price) {
			return errors.New("limit price must be an exact positive decimal")
		}
		price, ok := new(big.Rat).SetString(order.Price)
		if !ok || price.Sign() <= 0 {
			return errors.New("limit price must be positive")
		}
	} else if order.Price != "" {
		return errors.New("market orders must omit price")
	}
	switch order.State {
	case wire.OrderWorkflowState_ORDER_WORKFLOW_STATE_RESERVATION_PENDING:
		if order.ReservationId != "" {
			return errors.New("pending reservation cannot have a reservation id")
		}
	case wire.OrderWorkflowState_ORDER_WORKFLOW_STATE_RESERVED, wire.OrderWorkflowState_ORDER_WORKFLOW_STATE_ACCEPTED, wire.OrderWorkflowState_ORDER_WORKFLOW_STATE_RELEASE_PENDING, wire.OrderWorkflowState_ORDER_WORKFLOW_STATE_RELEASED:
		if !text(order.ReservationId) {
			return errors.New("reserved workflow requires a reservation id")
		}
	case wire.OrderWorkflowState_ORDER_WORKFLOW_STATE_DEAD_LETTER:
		if order.ReservationId != "" && !text(order.ReservationId) {
			return errors.New("invalid reservation id")
		}
	default:
		return errors.New("unsupported order workflow state")
	}
	releasing := order.State == wire.OrderWorkflowState_ORDER_WORKFLOW_STATE_RELEASE_PENDING || order.State == wire.OrderWorkflowState_ORDER_WORKFLOW_STATE_RELEASED || (order.State == wire.OrderWorkflowState_ORDER_WORKFLOW_STATE_DEAD_LETTER && order.ReservationId != "")
	if releasing {
		if order.ReleaseReason != "matching_failed" && order.ReleaseReason != "execution_complete" {
			return errors.New("release workflow requires a supported reason")
		}
	} else if order.ReleaseReason != "" {
		return errors.New("release reason on non-release workflow")
	}
	return nil
}

func validateJournal(journal *wire.LedgerJournalPosted) error {
	if journal == nil || !text(journal.JournalId, journal.BusinessType, journal.ReferenceId, journal.SourceSystem, journal.CommandId) || len(journal.Entries) < 2 || len(journal.Entries) > 1024 || len(journal.Balances) > 1024 {
		return errors.New("journal identity and 2..1024 complete entries are required")
	}
	accounts := map[string]*wire.AccountBalanceSnapshot{}
	for _, balance := range journal.Balances {
		if err := ValidateBalance(balance); err != nil {
			return err
		}
		if accounts[balance.UserAssetAccountId] != nil {
			return errors.New("duplicate account snapshot")
		}
		accounts[balance.UserAssetAccountId] = balance
	}
	ids := map[string]bool{}
	sums := map[string]*big.Int{}
	touched := map[string]bool{}
	for _, entry := range journal.Entries {
		if entry == nil || !text(entry.EntryId, entry.AccountId, entry.AssetId) || ids[entry.EntryId] {
			return errors.New("entry identity is missing or duplicated")
		}
		ids[entry.EntryId] = true
		amount, err := atomic(entry.AmountAtomic, true)
		if err != nil {
			return err
		}
		switch entry.Side {
		case wire.PostingSide_POSTING_SIDE_DEBIT:
		case wire.PostingSide_POSTING_SIDE_CREDIT:
			amount.Neg(amount)
		default:
			return errors.New("entry side must be debit or credit")
		}
		if sums[entry.AssetId] == nil {
			sums[entry.AssetId] = new(big.Int)
		}
		sums[entry.AssetId].Add(sums[entry.AssetId], amount)
		if entry.UserAssetAccountId != "" {
			balance := accounts[entry.UserAssetAccountId]
			if balance == nil || balance.AssetId != entry.AssetId || entry.CustodyPositionId != "" || !bucket(entry.Bucket) {
				return errors.New("entry has incomplete or inconsistent customer dimensions")
			}
			touched[entry.UserAssetAccountId] = true
		} else if entry.Bucket != "" || (entry.CustodyPositionId != "" && !text(entry.CustodyPositionId)) {
			return errors.New("entry has invalid non-customer dimensions")
		}
	}
	for _, sum := range sums {
		if sum.Sign() != 0 {
			return errors.New("journal debits and credits must balance independently per asset")
		}
	}
	if len(touched) != len(accounts) {
		return errors.New("account snapshot is not represented in journal entries")
	}
	return nil
}

func bucket(value string) bool {
	switch value {
	case "available", "reserved", "withdrawal_pending", "deposit_pending", "hold":
		return true
	}
	return false
}

// ValidateBalance checks a complete standalone customer balance snapshot.
func ValidateBalance(balance *wire.AccountBalanceSnapshot) error {
	if balance == nil || !text(balance.UserAssetAccountId, balance.UserId, balance.AssetId) || balance.Version == 0 || balance.Version > math.MaxInt64 || balance.PreviousVersion >= math.MaxInt64 || balance.Version <= balance.PreviousVersion {
		return errors.New("account snapshot requires identity and increasing before/after versions")
	}
	for _, amount := range []string{balance.AvailableAtomic, balance.ReservedAtomic, balance.WithdrawalPendingAtomic, balance.DepositPendingAtomic, balance.HoldAtomic} {
		if _, err := atomic(amount, false); err != nil {
			return err
		}
	}
	return nil
}
