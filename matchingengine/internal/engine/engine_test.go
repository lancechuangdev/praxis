package engine

import (
	"context"
	"testing"

	matchingv1 "praxis/matchingengine/gen/matching/v1"
)

type fakePublisher struct {
	events []Event
	err    error
}

func (f *fakePublisher) Publish(_ context.Context, event Event) error {
	f.events = append(f.events, event)
	return f.err
}
func (*fakePublisher) Close() error { return nil }

func TestSubmitPublishesAndIsIdempotent(t *testing.T) {
	publisher := &fakePublisher{}
	service := &Service{Publisher: publisher, Metrics: &Metrics{}}
	request := &matchingv1.SubmitOrderRequest{RequestId: "request-1", OrderId: "order-1", UserId: "alice", Symbol: "BTC-USDT", ReservationId: "reservation-1", EnginePartition: 2}
	first, err := service.SubmitOrder(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.SubmitOrder(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.EngineSequence != 1 || second.EngineSequence != 1 {
		t.Fatalf("sequences=%d,%d", first.EngineSequence, second.EngineSequence)
	}
	if len(publisher.events) != 1 {
		t.Fatalf("events=%d", len(publisher.events))
	}
}

func TestSequencesAreIndependentPerPartition(t *testing.T) {
	service := &Service{Publisher: &fakePublisher{}, Metrics: &Metrics{}}
	request := func(id string, partition int32) *matchingv1.SubmitOrderRequest {
		return &matchingv1.SubmitOrderRequest{RequestId: id, OrderId: id, UserId: "alice", Symbol: "BTC-USDT", ReservationId: "r-" + id, EnginePartition: partition}
	}
	a, _ := service.SubmitOrder(context.Background(), request("a", 0))
	b, _ := service.SubmitOrder(context.Background(), request("b", 1))
	if a.EngineSequence != 1 || b.EngineSequence != 1 {
		t.Fatalf("sequences=%d,%d", a.EngineSequence, b.EngineSequence)
	}
}
