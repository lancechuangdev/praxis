package order

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	grpcCodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	dueReservationPanics  int
	dueReservations       []Saga
	reservedOwner         string
	acceptedOwner         string
	backlog               SagaBacklog
}

func (f *fakeSagas) Backlog(context.Context) (SagaBacklog, error) { return f.backlog, nil }

func (f *fakeSagas) Start(_ context.Context, req Request) error {
	f.state = "reservation_pending"
	f.saga.Request = req
	return nil
}
func (f *fakeSagas) Reserved(_ context.Context, requestID, reservationID, owner string) error {
	f.state = "reserved"
	f.reservedOwner = owner
	f.saga.Request.RequestID = requestID
	f.saga.ReservationID = reservationID
	return nil
}
func (f *fakeSagas) Accepted(_ context.Context, _ string, owner string) error {
	f.state, f.acceptedOwner = "accepted", owner
	return nil
}
func (f *fakeSagas) ReservationFailed(context.Context, string, error, bool, string) (bool, error) {
	if f.deadLetterReservation {
		f.state = "dead_letter"
	} else {
		f.state = "reservation_pending"
	}
	return f.deadLetterReservation, nil
}
func (f *fakeSagas) RequestRelease(_ context.Context, orderID, reason, claimOwner string) (Saga, error) {
	f.state = "release_pending"
	f.saga.Request.OrderID = orderID
	f.saga.ReleaseReason = reason
	f.saga.ClaimOwner = claimOwner
	return f.saga, nil
}
func (f *fakeSagas) Released(context.Context, string, string) error { f.state = "released"; return nil }
func (f *fakeSagas) ReleaseFailed(context.Context, string, error, string) (bool, error) {
	f.failures++
	if f.deadLetterOnRelease {
		f.state = "dead_letter"
	}
	return f.deadLetterOnRelease, nil
}
func (*fakeSagas) DueReleases(context.Context, int, string, time.Duration) ([]Saga, error) {
	return nil, nil
}

func TestSagaRecoveryPropagatesClaimOwnership(t *testing.T) {
	req := validRequest()
	sagas := &fakeSagas{dueReservations: []Saga{{Request: req, ClaimOwner: "worker-a", LeaseExpired: true}}, backlog: SagaBacklog{DueCount: 3, OldestAge: 2500 * time.Millisecond}}
	service := &Service{Ledger: &fakeLedger{}, Matching: &fakeMatching{}, Sagas: sagas, Metrics: &Metrics{}, RecoveryOwner: "worker-a"}
	service.recoverSagas(context.Background())
	if sagas.reservedOwner != "worker-a" || sagas.acceptedOwner != "worker-a" {
		t.Fatalf("owners reserved=%q accepted=%q", sagas.reservedOwner, sagas.acceptedOwner)
	}
	if got := service.Metrics.SagaClaims.Load(); got != 1 {
		t.Fatalf("claims=%d, want 1", got)
	}
	if got := service.Metrics.SagaLeaseExpirations.Load(); got != 1 {
		t.Fatalf("lease expirations=%d, want 1", got)
	}
	if got := service.Metrics.SagaDue.Load(); got != 3 {
		t.Fatalf("due=%d, want 3", got)
	}
	if got := service.Metrics.SagaOldestDueAgeMillis.Load(); got != 2500 {
		t.Fatalf("oldest due age=%dms, want 2500ms", got)
	}
}

func TestRecoverySpanStartsNewTraceLinkedToOriginalRequest(t *testing.T) {
	previousPropagator := otel.GetTextMapPropagator()
	previousTracer := tracer
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	tracer = provider.Tracer("test")
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		tracer = previousTracer
		otel.SetTextMapPropagator(previousPropagator)
		_ = provider.Shutdown(context.Background())
	})
	original := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, TraceFlags: trace.FlagsSampled, Remote: true})
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(trace.ContextWithRemoteSpanContext(context.Background(), original), carrier)
	saga := Saga{Request: Request{RequestID: "request-1", CorrelationID: "correlation-1", TraceParent: carrier.Get("traceparent")}}

	_, span := (&Service{}).startRecoverySpan(context.Background(), saga, "reservation")
	span.End()

	ended := recorder.Ended()
	if len(ended) != 1 {
		t.Fatalf("spans=%d, want 1", len(ended))
	}
	if len(ended[0].Links()) != 1 {
		t.Fatalf("links=%d, want 1", len(ended[0].Links()))
	}
	linked := ended[0].Links()[0].SpanContext
	if ended[0].SpanContext().TraceID() == original.TraceID() || linked.TraceID() != original.TraceID() || linked.SpanID() != original.SpanID() {
		t.Fatal("recovery span must use a new trace linked to the original context")
	}
}
func (f *fakeSagas) DueReservations(context.Context, int, string, time.Duration) ([]Saga, error) {
	if f.dueReservationPanics > 0 {
		f.dueReservationPanics--
		panic("corrupt saga")
	}
	return f.dueReservations, nil
}

func TestSagaRecoveryStopsAndBecomesUnreadyAfterConsecutivePanics(t *testing.T) {
	sagas := &fakeSagas{dueReservationPanics: 2}
	service := &Service{Sagas: sagas, Metrics: &Metrics{}}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	err := service.RunSagaRecoverySupervised(context.Background(), time.Nanosecond, 0, 2, log)
	if !errors.Is(err, ErrSagaRecoveryUnhealthy) {
		t.Fatalf("error = %v, want unhealthy saga recovery", err)
	}
	if err := service.SagaRecoveryReady(); !errors.Is(err, ErrSagaRecoveryUnhealthy) {
		t.Fatalf("readiness error = %v", err)
	}
	if got := service.Metrics.SagaRecoveryPanics.Load(); got != 2 {
		t.Fatalf("panic count = %d, want 2", got)
	}
}

func TestSuccessfulSagaRecoveryCycleResetsConsecutivePanics(t *testing.T) {
	sagas := &alternatingPanicSagas{}
	service := &Service{Sagas: sagas, Metrics: &Metrics{}}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	if err := service.RunSagaRecoverySupervised(ctx, time.Microsecond, 0, 2, nil); err != nil {
		t.Fatalf("supervisor stopped after non-consecutive panics: %v", err)
	}
	if err := service.SagaRecoveryReady(); err != nil {
		t.Fatalf("readiness error = %v", err)
	}
	if service.Metrics.SagaRecoveryPanics.Load() == 0 {
		t.Fatal("expected recovered panics")
	}
}

type alternatingPanicSagas struct{ calls int }

func (*alternatingPanicSagas) Start(context.Context, Request) error                   { return nil }
func (*alternatingPanicSagas) Reserved(context.Context, string, string, string) error { return nil }
func (*alternatingPanicSagas) Accepted(context.Context, string, string) error         { return nil }
func (*alternatingPanicSagas) ReservationFailed(context.Context, string, error, bool, string) (bool, error) {
	return false, nil
}
func (*alternatingPanicSagas) RequestRelease(context.Context, string, string, string) (Saga, error) {
	return Saga{}, nil
}
func (*alternatingPanicSagas) Released(context.Context, string, string) error { return nil }
func (*alternatingPanicSagas) ReleaseFailed(context.Context, string, error, string) (bool, error) {
	return false, nil
}
func (*alternatingPanicSagas) DueReleases(context.Context, int, string, time.Duration) ([]Saga, error) {
	return nil, nil
}
func (s *alternatingPanicSagas) DueReservations(context.Context, int, string, time.Duration) ([]Saga, error) {
	s.calls++
	if s.calls%2 == 1 {
		panic("intermittent")
	}
	return nil, nil
}

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
