package order

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
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

func TestRunBoundedLimitsRecoverySurge(t *testing.T) {
	const (
		limit = 3
		count = 20
	)
	sagas := make([]Saga, count)
	for i := range sagas {
		sagas[i].Request.RequestID = fmt.Sprintf("request-%d", i)
	}
	service := &Service{Metrics: &Metrics{}}
	started := make(chan struct{}, count)
	release := make(chan struct{})
	var active, peak atomic.Int64
	done := make(chan struct{})

	go func() {
		service.runBounded(sagas, limit, func(Saga) {
			current := active.Add(1)
			// update peak safely among concurrent workers
			for {
				previous := peak.Load()
				if current <= previous || peak.CompareAndSwap(previous, current) {
					break
				}
			}
			started <- struct{}{}
			<-release
			active.Add(-1)
		})
		close(done)
	}()

	for range limit {
		<-started
	}
	select {
	case <-started:
		t.Fatal("recovery exceeded its concurrency limit")
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("recovery surge did not drain")
	}
	if got := peak.Load(); got != limit {
		t.Fatalf("peak concurrency=%d, want %d", got, limit)
	}
	if got := service.Metrics.SagaRetryAttempts.Load(); got != count {
		t.Fatalf("retry attempts=%d, want %d", got, count)
	}
	if got := service.Metrics.SagaActiveClaims.Load(); got != 0 {
		t.Fatalf("active claims after drain=%d, want 0", got)
	}
}

type fakeLedger struct {
	reservation Reservation
	err         error
	calls       int
	releases    []ReleaseRequest
	releaseErr  error
	release     Reservation
}

func (f *fakeLedger) Reserve(context.Context, Request) (Reservation, error) {
	f.calls++
	return f.reservation, f.err
}
func (f *fakeLedger) Release(_ context.Context, request ReleaseRequest) (Reservation, error) {
	f.releases = append(f.releases, request)
	return f.release, f.releaseErr
}
func (*fakeLedger) Close() error { return nil }

type fakeMatching struct {
	err      error
	calls    int
	response MatchResult
}

func (f *fakeMatching) Submit(context.Context, Request, Reservation) (MatchResult, error) {
	f.calls++
	response := f.response
	if response.Status == "" {
		response = MatchResult{Status: "accepted", EngineSequence: 42}
	}
	return response, f.err
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

func TestReservationRecoveryCountsLedgerReplay(t *testing.T) {
	req := validRequest()
	ledger := &fakeLedger{reservation: Reservation{ID: "reservation-1", Replay: true}}
	sagas := &fakeSagas{saga: Saga{Request: req, ClaimOwner: "worker-a"}, state: "reservation_pending"}
	service := &Service{Ledger: ledger, Matching: &fakeMatching{}, Sagas: sagas, Metrics: &Metrics{}}

	if err := service.resumeReservation(context.Background(), sagas.saga); err != nil {
		t.Fatal(err)
	}
	if got := service.Metrics.SagaRecoveryReplays.Load(); got != 1 {
		t.Fatalf("recovery replays=%d, want 1", got)
	}
}

func TestReservationRecoveryCountsMatchingReplay(t *testing.T) {
	req := validRequest()
	ledger := &fakeLedger{reservation: Reservation{ID: "reservation-1"}}
	matching := &fakeMatching{response: MatchResult{Status: "accepted", EngineSequence: 42, Replay: true}}
	sagas := &fakeSagas{saga: Saga{Request: req, ClaimOwner: "worker-a"}, state: "reservation_pending"}
	service := &Service{Ledger: ledger, Matching: matching, Sagas: sagas, Metrics: &Metrics{}}

	if err := service.resumeReservation(context.Background(), sagas.saga); err != nil {
		t.Fatal(err)
	}
	if got := service.Metrics.SagaRecoveryReplays.Load(); got != 1 {
		t.Fatalf("recovery replays=%d, want 1", got)
	}
}

func TestReleaseRecoveryCountsLedgerReplay(t *testing.T) {
	ledger := &fakeLedger{release: Reservation{Replay: true}}
	sagas := &fakeSagas{saga: Saga{Request: Request{RequestID: "request-1", OrderID: "order-1"}, ClaimOwner: "worker-a", ReleaseReason: "execution_complete"}, state: "release_pending"}
	service := &Service{Ledger: ledger, Sagas: sagas, Metrics: &Metrics{}}

	if err := service.release(context.Background(), sagas.saga); err != nil {
		t.Fatal(err)
	}
	if got := service.Metrics.SagaRecoveryReplays.Load(); got != 1 {
		t.Fatalf("recovery replays=%d, want 1", got)
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

type outageLedger struct {
	available atomic.Bool
	calls     atomic.Int64
}

func (l *outageLedger) Reserve(_ context.Context, req Request) (Reservation, error) {
	l.calls.Add(1)
	if !l.available.Load() {
		return Reservation{}, errors.New("ledger unavailable")
	}
	return Reservation{ID: req.OrderID + "-reservation"}, nil
}
func (*outageLedger) Release(context.Context, ReleaseRequest) (Reservation, error) {
	return Reservation{}, nil
}
func (*outageLedger) Close() error { return nil }

type recoveryBacklogStore struct {
	mu       sync.Mutex
	sagas    []Saga
	states   map[string]string
	owners   map[string]string
	attempts map[string]int
}

func newRecoveryBacklogStore(count int) *recoveryBacklogStore {
	store := &recoveryBacklogStore{states: make(map[string]string, count), owners: make(map[string]string, count), attempts: make(map[string]int, count)}
	for i := range count {
		requestID := fmt.Sprintf("outage-request-%d", i)
		store.sagas = append(store.sagas, Saga{Request: Request{RequestID: requestID, OrderID: fmt.Sprintf("outage-order-%d", i), UserID: "alice", Symbol: "BTC-USDT", Side: "buy", OrderType: "market", Quantity: "1", ReserveAssetID: "asset_usdt", ReserveAmountAtomic: "1"}})
		store.states[requestID] = "reservation_pending"
	}
	return store
}

func (*recoveryBacklogStore) Start(context.Context, Request) error { return nil }
func (s *recoveryBacklogStore) Reserved(_ context.Context, requestID, _ string, owner string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.owners[requestID] != owner {
		return errors.New("stale reservation owner")
	}
	s.states[requestID] = "reserved"
	return nil
}
func (s *recoveryBacklogStore) Accepted(_ context.Context, requestID, owner string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.owners[requestID] != owner || s.states[requestID] != "reserved" {
		return errors.New("stale acceptance owner")
	}
	s.states[requestID] = "accepted"
	delete(s.owners, requestID)
	return nil
}
func (s *recoveryBacklogStore) ReservationFailed(_ context.Context, requestID string, _ error, _ bool, owner string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.owners[requestID] != owner {
		return false, errors.New("stale failure owner")
	}
	s.attempts[requestID]++
	delete(s.owners, requestID)
	return false, nil
}
func (*recoveryBacklogStore) RequestRelease(context.Context, string, string, string) (Saga, error) {
	return Saga{}, errors.New("unexpected release")
}
func (*recoveryBacklogStore) Released(context.Context, string, string) error { return nil }
func (*recoveryBacklogStore) ReleaseFailed(context.Context, string, error, string) (bool, error) {
	return false, nil
}
func (*recoveryBacklogStore) DueReleases(context.Context, int, string, time.Duration) ([]Saga, error) {
	return nil, nil
}
func (s *recoveryBacklogStore) DueReservations(_ context.Context, limit int, owner string, _ time.Duration) ([]Saga, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	claimed := make([]Saga, 0, limit)
	for _, saga := range s.sagas {
		if len(claimed) == limit {
			break
		}
		if s.states[saga.Request.RequestID] != "reservation_pending" || s.owners[saga.Request.RequestID] != "" {
			continue
		}
		saga.ClaimOwner = owner
		s.owners[saga.Request.RequestID] = owner
		claimed = append(claimed, saga)
	}
	return claimed, nil
}
func (s *recoveryBacklogStore) accepted() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	accepted := 0
	for _, state := range s.states {
		if state == "accepted" {
			accepted++
		}
	}
	return accepted
}

type countingMatching struct {
	mu    sync.Mutex
	calls map[string]int
}

func (m *countingMatching) Submit(_ context.Context, req Request, _ Reservation) (MatchResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls[req.RequestID]++
	return MatchResult{Status: "accepted"}, nil
}
func (*countingMatching) Close() error { return nil }

func TestRecoveryStaysBoundedThroughDependencyOutageAndDrainsAfterward(t *testing.T) {
	const backlog = 9
	const limit = 3
	ledger := &outageLedger{}
	store := newRecoveryBacklogStore(backlog)
	matching := &countingMatching{calls: make(map[string]int, backlog)}
	service := &Service{Ledger: ledger, Matching: matching, Sagas: store, Metrics: &Metrics{}, RecoveryOwner: "worker-a", ReservationRetryConcurrency: limit}

	for range 3 {
		before := ledger.calls.Load()
		service.recoverSagas(context.Background())
		if cycleCalls := ledger.calls.Load() - before; cycleCalls > limit {
			t.Fatalf("outage cycle made %d calls, limit %d", cycleCalls, limit)
		}
	}
	if accepted := store.accepted(); accepted != 0 {
		t.Fatalf("accepted during outage=%d", accepted)
	}
	if service.Metrics.DeadLettered.Load() != 0 {
		t.Fatal("bounded transient outage unexpectedly dead-lettered work")
	}

	ledger.available.Store(true)
	for cycle := 0; cycle < backlog/limit; cycle++ {
		before := ledger.calls.Load()
		service.recoverSagas(context.Background())
		if cycleCalls := ledger.calls.Load() - before; cycleCalls > limit {
			t.Fatalf("recovery cycle made %d calls, limit %d", cycleCalls, limit)
		}
	}
	if accepted := store.accepted(); accepted != backlog {
		t.Fatalf("accepted after recovery=%d, want %d", accepted, backlog)
	}
	matching.mu.Lock()
	defer matching.mu.Unlock()
	for requestID, calls := range matching.calls {
		if calls != 1 {
			t.Fatalf("matching calls for %s=%d, want 1", requestID, calls)
		}
	}
}
