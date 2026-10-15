package lifecyclev1

import (
	"bytes"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	wire "praxis/api/gen/events/lifecycle/v1"
)

func orderEvent() *wire.LifecycleEvent {
	return &wire.LifecycleEvent{EventId: "workflow-event-1", EventType: OrderWorkflowType, SchemaVersion: 1, OccurredAt: timestamppb.New(time.Unix(1700000000, 0)), CorrelationId: "request-1", CausationId: "command-1", Producer: "order-service", Payload: &wire.LifecycleEvent_OrderWorkflow{OrderWorkflow: &wire.OrderWorkflowSnapshot{RequestId: "request-1", OrderId: "order-1", UserId: "alice", WorkflowVersion: 1, State: wire.OrderWorkflowState_ORDER_WORKFLOW_STATE_RESERVATION_PENDING, Symbol: "BTC-USD", Side: "buy", OrderType: "limit", Quantity: "9007199254740993", Price: "0.00000001", ReserveAssetId: "USD", ReserveAmountAtomic: "900719925474099300"}}}
}
func journalEvent() *wire.LifecycleEvent {
	event := orderEvent()
	event.EventId = "journal-event-1"
	event.EventType = LedgerJournalType
	event.Producer = "ledger-service"
	event.Payload = &wire.LifecycleEvent_LedgerJournal{LedgerJournal: &wire.LedgerJournalPosted{JournalId: "journal-1", BusinessType: "deposit_confirmed", ReferenceId: "deposit-1", SourceSystem: "custody", CommandId: "command-1", Entries: []*wire.JournalEntry{
		{EntryId: "entry-1", AccountId: "custody", AssetId: "USD", Side: wire.PostingSide_POSTING_SIDE_DEBIT, AmountAtomic: "900719925474099300", CustodyPositionId: "custody-1"},
		{EntryId: "entry-2", AccountId: "customer_available", AssetId: "USD", Side: wire.PostingSide_POSTING_SIDE_CREDIT, AmountAtomic: "900719925474099300", UserAssetAccountId: "account-1", Bucket: "available"},
	}, Balances: []*wire.AccountBalanceSnapshot{{UserAssetAccountId: "account-1", UserId: "alice", AssetId: "USD", PreviousVersion: 0, Version: 1, AvailableAtomic: "900719925474099300", ReservedAtomic: "0", WithdrawalPendingAtomic: "0", DepositPendingAtomic: "0", HoldAtomic: "0"}}}}
	return event
}
func TestRoundTripExactValuesAndKeys(t *testing.T) {
	for _, event := range []*wire.LifecycleEvent{orderEvent(), journalEvent()} {
		first, err := Encode(event)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := Decode(first)
		if err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(event, decoded) {
			t.Fatal("round trip changed exact values")
		}
		second, err := Encode(decoded)
		if err != nil || !bytes.Equal(first, second) {
			t.Fatal("unstable replay bytes")
		}
		key, err := Key(decoded)
		if err != nil {
			t.Fatal(err)
		}
		if key != "order-1" && key != "journal-1" {
			t.Fatalf("wrong key %q", key)
		}
	}
}
func TestRejectMalformedContracts(t *testing.T) {
	cases := []struct {
		name   string
		change func(*wire.LifecycleEvent)
	}{
		{"schema", func(e *wire.LifecycleEvent) { e.SchemaVersion = 2 }},
		{"producer", func(e *wire.LifecycleEvent) { e.Producer = "matching-engine" }},
		{"type", func(e *wire.LifecycleEvent) { e.EventType = "OrderFilled" }},
		{"metadata", func(e *wire.LifecycleEvent) { e.CausationId = "" }},
		{"timestamp", func(e *wire.LifecycleEvent) { e.OccurredAt = &timestamppb.Timestamp{Seconds: 1 << 62} }},
		{"payload", func(e *wire.LifecycleEvent) { e.Payload = nil }},
		{"empty payload", func(e *wire.LifecycleEvent) { e.Payload = &wire.LifecycleEvent_OrderWorkflow{} }},
		{"nil wrapper", func(e *wire.LifecycleEvent) { e.Payload = (*wire.LifecycleEvent_OrderWorkflow)(nil) }},
		{"version", func(e *wire.LifecycleEvent) { e.GetOrderWorkflow().WorkflowVersion = 0 }},
		{"enum", func(e *wire.LifecycleEvent) { e.GetOrderWorkflow().State = 999 }},
		{"float quantity", func(e *wire.LifecycleEvent) { e.GetOrderWorkflow().Quantity = "1.5" }},
		{"leading zero", func(e *wire.LifecycleEvent) { e.GetOrderWorkflow().ReserveAmountAtomic = "01" }},
		{"negative", func(e *wire.LifecycleEvent) { e.GetOrderWorkflow().ReserveAmountAtomic = "-1" }},
		{"too many digits", func(e *wire.LifecycleEvent) {
			e.GetOrderWorkflow().ReserveAmountAtomic = string(bytes.Repeat([]byte("9"), 79))
		}},
		{"price exponent", func(e *wire.LifecycleEvent) { e.GetOrderWorkflow().Price = "1e-8" }},
		{"price zero", func(e *wire.LifecycleEvent) { e.GetOrderWorkflow().Price = "0.00" }},
		{"market price", func(e *wire.LifecycleEvent) { e.GetOrderWorkflow().OrderType = "market" }},
		{"reservation", func(e *wire.LifecycleEvent) {
			e.GetOrderWorkflow().State = wire.OrderWorkflowState_ORDER_WORKFLOW_STATE_ACCEPTED
		}},
		{"release reason", func(e *wire.LifecycleEvent) { e.GetOrderWorkflow().ReleaseReason = "execution_complete" }},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			event := orderEvent()
			tt.change(event)
			if _, err := Encode(event); err == nil {
				t.Fatal("invalid event accepted")
			}
		})
	}
}
func TestRejectIncompleteOrUnbalancedJournal(t *testing.T) {
	cases := []struct {
		name   string
		change func(*wire.LedgerJournalPosted)
	}{
		{"imbalance", func(j *wire.LedgerJournalPosted) { j.Entries[0].AmountAtomic = "1" }},
		{"cross asset", func(j *wire.LedgerJournalPosted) { j.Entries[0].AssetId = "BTC" }},
		{"duplicate entry", func(j *wire.LedgerJournalPosted) { j.Entries[0].EntryId = j.Entries[1].EntryId }},
		{"unknown side", func(j *wire.LedgerJournalPosted) { j.Entries[0].Side = 99 }},
		{"missing snapshot", func(j *wire.LedgerJournalPosted) { j.Balances = nil }},
		{"extra snapshot", func(j *wire.LedgerJournalPosted) {
			extra := proto.Clone(j.Balances[0]).(*wire.AccountBalanceSnapshot)
			extra.UserAssetAccountId = "other"
			j.Balances = append(j.Balances, extra)
		}},
		{"duplicate snapshot", func(j *wire.LedgerJournalPosted) { j.Balances = append(j.Balances, j.Balances[0]) }},
		{"owner", func(j *wire.LedgerJournalPosted) { j.Balances[0].UserId = "" }},
		{"snapshot asset", func(j *wire.LedgerJournalPosted) { j.Balances[0].AssetId = "BTC" }},
		{"versions", func(j *wire.LedgerJournalPosted) { j.Balances[0].PreviousVersion = 1 }},
		{"negative balance", func(j *wire.LedgerJournalPosted) { j.Balances[0].HoldAtomic = "-1" }},
		{"missing bucket", func(j *wire.LedgerJournalPosted) { j.Entries[1].Bucket = "" }},
		{"custody and user", func(j *wire.LedgerJournalPosted) { j.Entries[1].CustodyPositionId = "custody-1" }},
		{"noncustomer bucket", func(j *wire.LedgerJournalPosted) { j.Entries[0].Bucket = "available" }},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			event := journalEvent()
			tt.change(event.GetLedgerJournal())
			if err := Validate(event); err == nil {
				t.Fatal("invalid journal accepted")
			}
		})
	}
}
func TestUnknownFieldsSurviveCompatibilityWindow(t *testing.T) {
	raw, _ := Encode(orderEvent())
	raw = protowire.AppendTag(raw, 100, protowire.BytesType)
	raw = protowire.AppendString(raw, "future additive field")
	decoded, err := Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := Encode(decoded)
	if err != nil {
		t.Fatal(err)
	}
	again, err := Decode(encoded)
	if err != nil || !bytes.Equal(decoded.ProtoReflect().GetUnknown(), again.ProtoReflect().GetUnknown()) {
		t.Fatal("unknown fields lost")
	}
}
func TestDecoderRejectsLegacyMalformedAndOversizedInput(t *testing.T) {
	for _, raw := range [][]byte{nil, []byte(`{"version":1,"type":"FundsReserved"}`), {0xff}, bytes.Repeat([]byte{0}, MaxEventBytes+1)} {
		if _, err := Decode(raw); err == nil {
			t.Fatal("invalid input accepted")
		}
	}
}
func FuzzDecode(f *testing.F) {
	seed, _ := Encode(orderEvent())
	f.Add(seed)
	f.Add([]byte("legacy"))
	f.Fuzz(func(t *testing.T, raw []byte) {
		event, err := Decode(raw)
		if err == nil {
			if _, err := Encode(event); err != nil {
				t.Fatal(err)
			}
		}
	})
}
