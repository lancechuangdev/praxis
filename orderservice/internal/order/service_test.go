package order

import (
	"context"
	"errors"
	grpcCodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	saga                  Saga
	state                 string
	failures              int
	deadLetterOnRelease   bool
	deadLetterReservation bool
}

func (f *fakeSagas) Start(_ context.Context, req Request) error {
	f.state = "reservation_pending"
	f.saga.Request = req
	return nil
}
func (f *fakeSagas) Reserved(_ context.Context, requestID, reservationID string) error {
	f.state = "reserved"
	f.saga.Request.RequestID = requestID
	f.saga.ReservationID = reservationID
	return nil
}
func (f *fakeSagas) Accepted(context.Context, string) error { f.state = "accepted"; return nil }
func (f *fakeSagas) ReservationFailed(context.Context, string, error, bool) (bool, error) {
	if f.deadLetterReservation {
		f.state = "dead_letter"
	} else {
		f.state = "reservation_pending"
	}
	return f.deadLetterReservation, nil
}
func (f *fakeSagas) RequestRelease(_ context.Context, orderID, reason string) (Saga, error) {
	f.state = "release_pending"
	f.saga.Request.OrderID = orderID
	f.saga.ReleaseReason = reason
	return f.saga, nil
}
func (f *fakeSagas) Released(context.Context, string) error { f.state = "released"; return nil }
func (f *fakeSagas) ReleaseFailed(context.Context, string, error) (bool, error) {
	f.failures++
	if f.deadLetterOnRelease {
		f.state = "dead_letter"
	}
	return f.deadLetterOnRelease, nil
}
func (*fakeSagas) DueReleases(context.Context, int) ([]Saga, error)     { return nil, nil }
func (*fakeSagas) DueReservations(context.Context, int) ([]Saga, error) { return nil, nil }

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
	sagas := &fakeSagas{saga: Saga{Request: Request{RequestID: "request-1", OrderID: "order-1"}}, state: "accepted"}
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

func TestTransientReservationFailureRemainsPending(t *testing.T) {
	ledger := &fakeLedger{err: errors.New("ledger unavailable")}
	sagas := &fakeSagas{}
	service := &Service{Ledger: ledger, Matching: &fakeMatching{}, Sagas: sagas, Metrics: &Metrics{}}
	if _, err := service.Admit(context.Background(), validRequest()); err == nil {
		t.Fatal("expected reservation failure")
	}
	if sagas.state != "reservation_pending" {
		t.Fatalf("saga state=%s", sagas.state)
	}
	if service.Metrics.DeadLettered.Load() != 0 {
		t.Fatalf("dead letters=%d", service.Metrics.DeadLettered.Load())
	}
}

func TestPermanentReservationFailureIsDeadLettered(t *testing.T) {
	ledger := &fakeLedger{err: status.Error(grpcCodes.FailedPrecondition, "insufficient funds")}
	sagas := &fakeSagas{deadLetterReservation: true}
	service := &Service{Ledger: ledger, Matching: &fakeMatching{}, Sagas: sagas, Metrics: &Metrics{}}
	if _, err := service.Admit(context.Background(), validRequest()); err == nil {
		t.Fatal("expected reservation failure")
	}
	if sagas.state != "dead_letter" || service.Metrics.DeadLettered.Load() != 1 {
		t.Fatalf("state=%s dead letters=%d", sagas.state, service.Metrics.DeadLettered.Load())
	}
}

func TestReservationRetryResumesMatching(t *testing.T) {
	req := validRequest()
	ledger := &fakeLedger{reservation: Reservation{ID: "reservation-1"}}
	matching := &fakeMatching{}
	sagas := &fakeSagas{saga: Saga{Request: req}, state: "reservation_pending"}
	service := &Service{Ledger: ledger, Matching: matching, Sagas: sagas, Metrics: &Metrics{}}
	if err := service.resumeReservation(context.Background(), sagas.saga); err != nil {
		t.Fatal(err)
	}
	if ledger.calls != 1 || matching.calls != 1 || sagas.state != "accepted" {
		t.Fatalf("ledger=%d matching=%d state=%s", ledger.calls, matching.calls, sagas.state)
	}
}

func TestExhaustedReleaseRetriesIncrementDeadLetterMetric(t *testing.T) {
	ledger := &fakeLedger{releaseErr: errors.New("ledger unavailable")}
	sagas := &fakeSagas{saga: Saga{Request: Request{RequestID: "request-1", OrderID: "order-1"}}, state: "accepted", deadLetterOnRelease: true}
	service := &Service{Ledger: ledger, Sagas: sagas, Metrics: &Metrics{}}
	if err := service.CompleteExecution(context.Background(), "order-1"); err == nil {
		t.Fatal("expected release failure")
	}
	if sagas.state != "dead_letter" || service.Metrics.DeadLettered.Load() != 1 {
		t.Fatalf("state=%s dead letters=%d", sagas.state, service.Metrics.DeadLettered.Load())
	}
}
