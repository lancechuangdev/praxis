package order

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestLocalTokenBucketsObserveWithoutRejecting(t *testing.T) {
	now := time.Unix(100, 0)
	limiter := NewLocalTokenBuckets(1, 1, true)
	limiter.now = func() time.Time { return now }
	req := Request{UserID: "alice", Symbol: "BTC-USDT"}
	first, _ := limiter.Check(context.Background(), req)
	second, _ := limiter.Check(context.Background(), req)
	if !first.Allowed || !second.Allowed || !second.WouldReject || second.RetryAfter <= 0 {
		t.Fatalf("first=%+v second=%+v", first, second)
	}
}

func TestConcurrencyGateRejectsWhenQueueIsFull(t *testing.T) {
	gate := NewConcurrencyGate(1, 1)
	release, err := gate.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waiting := make(chan error, 1)
	go func() { _, acquireErr := gate.Acquire(ctx); waiting <- acquireErr }()
	deadline := time.Now().Add(time.Second)
	for gate.waiting.Load() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if gate.waiting.Load() != 1 {
		t.Fatal("first waiter did not enter the queue")
	}
	if _, err = gate.Acquire(context.Background()); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("error=%v, want overload", err)
	}
	cancel()
	if err = <-waiting; !errors.Is(err, context.Canceled) {
		t.Fatalf("waiting error=%v", err)
	}
}

type rejectingAdmission struct{}

func (rejectingAdmission) Check(context.Context, Request) (AdmissionDecision, error) {
	return AdmissionDecision{RetryAfter: time.Second}, nil
}

func TestAdmissionRejectionPrecedesFinancialSideEffects(t *testing.T) {
	ledger := &fakeLedger{}
	sagas := &fakeSagas{}
	service := &Service{Ledger: ledger, Matching: &fakeMatching{}, Sagas: sagas, Metrics: &Metrics{}, Admission: rejectingAdmission{}}
	_, err := service.Admit(context.Background(), validRequest())
	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("error=%v", err)
	}
	if ledger.calls != 0 || sagas.state != "" {
		t.Fatalf("ledger calls=%d saga state=%q", ledger.calls, sagas.state)
	}
}
