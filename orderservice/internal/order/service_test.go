package order

import (
	"context"
	"errors"
	"testing"
)

type fakeLedger struct {
	reservation Reservation
	err         error
	calls       int
}

func (f *fakeLedger) Reserve(context.Context, Request) (Reservation, error) {
	f.calls++
	return f.reservation, f.err
}
func (*fakeLedger) Close() error { return nil }

type fakeMatching struct {
	err   error
	calls int
}

func (f *fakeMatching) Submit(context.Context, Request, Reservation) (MatchResult, error) {
	f.calls++
	return MatchResult{Status: "accepted", EngineSequence: 42}, f.err
}
func (*fakeMatching) Close() error { return nil }

func validRequest() Request {
	return Request{RequestID: "request-1", OrderID: "order-1", UserID: "alice", Symbol: "BTC-USDT", Side: "buy", OrderType: "limit", Quantity: "1000", Price: "70000", ReserveAssetID: "asset_usdt", ReserveAmountAtomic: "70000000"}
}

func TestAdmitRunsReservationBeforeMatching(t *testing.T) {
	ledger := &fakeLedger{reservation: Reservation{ID: "reservation-1", BalanceVersion: 2}}
	matching := &fakeMatching{}
	service := &Service{Ledger: ledger, Matching: matching, Metrics: &Metrics{}}
	response, err := service.Admit(context.Background(), validRequest())
	if err != nil {
		t.Fatal(err)
	}
	if response.Status != "accepted" || response.Reservation.ID != "reservation-1" {
		t.Fatalf("response=%+v", response)
	}
	if response.EngineSequence != 42 {
		t.Fatalf("engine sequence=%d", response.EngineSequence)
	}
	if ledger.calls != 1 || matching.calls != 1 {
		t.Fatalf("ledger calls=%d matching calls=%d", ledger.calls, matching.calls)
	}
}

func TestAdmitStopsAfterRiskRejection(t *testing.T) {
	ledger := &fakeLedger{}
	matching := &fakeMatching{}
	service := &Service{Ledger: ledger, Matching: matching, RiskRejectBPS: 10000, Metrics: &Metrics{}}
	_, err := service.Admit(context.Background(), validRequest())
	if !errors.Is(err, ErrRiskRejected) {
		t.Fatalf("error=%v", err)
	}
	if ledger.calls != 0 || matching.calls != 0 {
		t.Fatalf("downstream called after rejection")
	}
}

func TestValidateRejectsInvalidAmount(t *testing.T) {
	request := validRequest()
	request.ReserveAmountAtomic = "0"
	if err := Validate(request); err == nil {
		t.Fatal("expected validation error")
	}
}
