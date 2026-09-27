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
	releases    []ReleaseRequest
	releaseErr  error
}

func (f *fakeLedger) Reserve(context.Context, Request) (Reservation, error) {
	f.calls++
	return f.reservation, f.err
}
func (f *fakeLedger) Release(_ context.Context, request ReleaseRequest) (Reservation, error) {
	f.releases = append(f.releases, request)
	return Reservation{}, f.releaseErr
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

type fakeSagas struct {
	saga     Saga
	state    string
	failures int
}

func (f *fakeSagas) Start(context.Context, Request) error { f.state = "started"; return nil }
func (f *fakeSagas) Reserved(_ context.Context, requestID, reservationID string) error {
	f.state = "reserved"
	f.saga.RequestID = requestID
	f.saga.ReservationID = reservationID
	return nil
}
func (f *fakeSagas) Accepted(context.Context, string) error { f.state = "accepted"; return nil }
func (f *fakeSagas) RequestRelease(_ context.Context, orderID, reason string) (Saga, error) {
	f.state = "release_pending"
	f.saga.OrderID = orderID
	f.saga.ReleaseReason = reason
	return f.saga, nil
}
func (f *fakeSagas) Released(context.Context, string) error             { f.state = "released"; return nil }
func (f *fakeSagas) ReleaseFailed(context.Context, string, error) error { f.failures++; return nil }
func (*fakeSagas) DueReleases(context.Context, int) ([]Saga, error)     { return nil, nil }

func TestMatchingFailureCompensatesReservation(t *testing.T) {
	ledger := &fakeLedger{reservation: Reservation{ID: "reservation-1"}}
	sagas := &fakeSagas{}
	service := &Service{Ledger: ledger, Matching: &fakeMatching{err: errors.New("rejected")}, Sagas: sagas, Metrics: &Metrics{}}
	if _, err := service.Admit(context.Background(), validRequest()); err == nil {
		t.Fatal("expected matching failure")
	}
	if len(ledger.releases) != 1 || ledger.releases[0].Reason != "matching_failed" {
		t.Fatalf("releases=%+v", ledger.releases)
	}
	if sagas.state != "released" {
		t.Fatalf("saga state=%s", sagas.state)
	}
}

func TestExecutionCompletionReleasesOnlyLedgerRemainder(t *testing.T) {
	ledger := &fakeLedger{}
	sagas := &fakeSagas{saga: Saga{RequestID: "request-1", OrderID: "order-1"}, state: "accepted"}
	service := &Service{Ledger: ledger, Sagas: sagas, Metrics: &Metrics{}}
	if err := service.CompleteExecution(context.Background(), "order-1"); err != nil {
		t.Fatal(err)
	}
	if len(ledger.releases) != 1 || ledger.releases[0].Reason != "execution_complete" {
		t.Fatalf("releases=%+v", ledger.releases)
	}
	if sagas.state != "released" {
		t.Fatalf("saga state=%s", sagas.state)
	}
}
