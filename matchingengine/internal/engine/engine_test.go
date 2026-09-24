package engine

import (
	"context"
	"errors"
	matchingv1 "praxis/matchingengine/gen/matching/v1"
	"testing"
)

type fakeStore struct {
	response *matchingv1.SubmitOrderResponse
	err      error
	calls    int
}

func (f *fakeStore) Submit(context.Context, *matchingv1.SubmitOrderRequest) (*matchingv1.SubmitOrderResponse, error) {
	f.calls++
	return f.response, f.err
}
func TestSubmitDelegatesToDurableStore(t *testing.T) {
	store := &fakeStore{response: &matchingv1.SubmitOrderResponse{OrderId: "order-1", Status: "accepted", EngineSequence: 7}}
	service := &Service{Store: store, Metrics: &Metrics{}}
	got, err := service.SubmitOrder(context.Background(), &matchingv1.SubmitOrderRequest{RequestId: "request-1", OrderId: "order-1", UserId: "alice", Symbol: "BTC-USDT", ReservationId: "reservation-1"})
	if err != nil {
		t.Fatal(err)
	}
	if got.EngineSequence != 7 || store.calls != 1 {
		t.Fatalf("response=%v calls=%d", got, store.calls)
	}
}
func TestSubmitRejectsInvalidRequestBeforeStore(t *testing.T) {
	store := &fakeStore{err: errors.New("should not be called")}
	service := &Service{Store: store, Metrics: &Metrics{}}
	if _, err := service.SubmitOrder(context.Background(), &matchingv1.SubmitOrderRequest{}); err == nil {
		t.Fatal("expected validation error")
	}
	if store.calls != 0 {
		t.Fatalf("calls=%d", store.calls)
	}
}
func TestFingerprintDetectsChangedPayload(t *testing.T) {
	a := &matchingv1.SubmitOrderRequest{RequestId: "r", OrderId: "o", Symbol: "BTC-USDT", Quantity: "1"}
	b := &matchingv1.SubmitOrderRequest{RequestId: "r", OrderId: "o", Symbol: "BTC-USDT", Quantity: "2"}
	af, _ := requestFingerprint(a)
	bf, _ := requestFingerprint(b)
	if af == bf {
		t.Fatal("fingerprints should differ")
	}
}
