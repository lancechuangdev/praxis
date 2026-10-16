package workflow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	lifecycle "praxis/api/events/lifecycle/v1"
	wire "praxis/api/gen/events/lifecycle/v1"
)

// This transaction model exercises the processor protocol; it is not a
// deployable store and provides no evidence of MongoDB transaction behavior.
type memoryStore struct {
	mu                  sync.Mutex
	targets             map[Target]*memoryTransaction
	fail                string
	loseAcknowledgement bool
}
type memoryTransaction struct {
	workflows map[string]*wire.LifecycleEvent
	inbox     map[string]Receipt
	fail      string
}

var storageFailure = errors.New("storage unavailable")

func (s *memoryStore) Within(ctx context.Context, target Target, fn func(Transaction) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.targets == nil {
		s.targets = make(map[Target]*memoryTransaction)
	}
	next := &memoryTransaction{workflows: make(map[string]*wire.LifecycleEvent), inbox: make(map[string]Receipt), fail: s.fail}
	if prior := s.targets[target]; prior != nil {
		for k, v := range prior.workflows {
			next.workflows[k] = proto.Clone(v).(*wire.LifecycleEvent)
		}
		for k, v := range prior.inbox {
			next.inbox[k] = v
		}
	}
	if err := fn(next); err != nil {
		return err
	}
	if s.fail == "commit" {
		return storageFailure
	}
	s.targets[target] = next
	if s.loseAcknowledgement {
		s.loseAcknowledgement = false
		return storageFailure
	}
	return nil
}
func (t *memoryTransaction) LockWorkflow(_ context.Context, id string) (State, error) {
	if t.fail == "read" {
		return State{}, storageFailure
	}
	if event := t.workflows[id]; event != nil {
		snapshot := event.GetOrderWorkflow()
		return State{Version: snapshot.WorkflowVersion, UserID: snapshot.UserId}, nil
	}
	return State{}, nil
}
func (t *memoryTransaction) Inbox(_ context.Context, id string) (Receipt, bool, error) {
	if t.fail == "inbox-read" {
		return Receipt{}, false, storageFailure
	}
	receipt, exists := t.inbox[id]
	return receipt, exists, nil
}
func (t *memoryTransaction) PutWorkflow(_ context.Context, event *wire.LifecycleEvent) error {
	if t.fail == "write" {
		return storageFailure
	}
	t.workflows[event.GetOrderWorkflow().OrderId] = proto.Clone(event).(*wire.LifecycleEvent)
	return nil
}
func (t *memoryTransaction) PutInbox(_ context.Context, id string, receipt Receipt) error {
	if t.fail == "inbox-write" {
		return storageFailure
	}
	t.inbox[id] = receipt
	return nil
}

func fact(version uint64) *wire.LifecycleEvent {
	return &wire.LifecycleEvent{EventId: fmt.Sprintf("event-%d", version), EventType: lifecycle.OrderWorkflowType, SchemaVersion: 1, Producer: "order-service", CorrelationId: "request-1", CausationId: "request-1", OccurredAt: timestamppb.New(time.Unix(1700000000, 123)), Payload: &wire.LifecycleEvent_OrderWorkflow{OrderWorkflow: &wire.OrderWorkflowSnapshot{RequestId: "request-1", OrderId: "order-1", UserId: "alice", WorkflowVersion: version, State: wire.OrderWorkflowState_ORDER_WORKFLOW_STATE_RESERVATION_PENDING, Symbol: "BTC-USDT", Side: "buy", OrderType: "market", Quantity: "9007199254740993", ReserveAssetId: "USDT", ReserveAmountAtomic: "123"}}}
}
func processFact(t *testing.T, p *Processor, e *wire.LifecycleEvent) (Outcome, error) {
	t.Helper()
	raw, err := lifecycle.Encode(e)
	if err != nil {
		t.Fatal(err)
	}
	return p.Process(context.Background(), "order.events.v1", []byte(e.GetOrderWorkflow().OrderId), raw)
}
func processor(t *testing.T, s Store) *Processor {
	t.Helper()
	p, err := New(Target{Generation: "orders-v1", Mode: Shadow}, s)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestWorkflowSequenceDuplicateAndConflicts(t *testing.T) {
	s := &memoryStore{}
	p := processor(t, s)
	if _, err := processFact(t, p, fact(2)); !errors.Is(err, ErrGap) {
		t.Fatal(err)
	}
	if outcome, err := processFact(t, p, fact(1)); err != nil || outcome != Applied {
		t.Fatalf("%s %v", outcome, err)
	}
	if outcome, err := processFact(t, p, fact(1)); err != nil || outcome != Duplicate {
		t.Fatalf("%s %v", outcome, err)
	}
	changed := fact(1)
	changed.CorrelationId = "different"
	if _, err := processFact(t, p, changed); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	crossOrder := fact(1)
	crossOrder.GetOrderWorkflow().OrderId = "order-2"
	if _, err := processFact(t, p, crossOrder); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	stale := fact(1)
	stale.EventId = "unknown-old-event"
	if _, err := processFact(t, p, stale); !errors.Is(err, ErrStale) {
		t.Fatal(err)
	}
	owner := fact(2)
	owner.GetOrderWorkflow().UserId = "mallory"
	if _, err := processFact(t, p, owner); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	if _, err := processFact(t, p, fact(3)); !errors.Is(err, ErrGap) {
		t.Fatal(err)
	}
	for _, version := range []uint64{2, 3} {
		if outcome, err := processFact(t, p, fact(version)); err != nil || outcome != Applied {
			t.Fatalf("%s %v", outcome, err)
		}
	}
	if outcome, err := processFact(t, p, fact(1)); err != nil || outcome != Duplicate {
		t.Fatalf("late duplicate: %s %v", outcome, err)
	}
}

func TestWorkflowRollbackAndAmbiguousCommit(t *testing.T) {
	for _, fail := range []string{"read", "inbox-read", "write", "inbox-write", "commit"} {
		t.Run(fail, func(t *testing.T) {
			s := &memoryStore{fail: fail}
			p := processor(t, s)
			if outcome, err := processFact(t, p, fact(1)); outcome != "" || !errors.Is(err, storageFailure) {
				t.Fatalf("%s %v", outcome, err)
			}
			s.fail = ""
			if outcome, err := processFact(t, p, fact(1)); outcome != Applied || err != nil {
				t.Fatalf("rollback: %s %v", outcome, err)
			}
		})
	}
	s := &memoryStore{loseAcknowledgement: true}
	p := processor(t, s)
	if outcome, err := processFact(t, p, fact(1)); outcome != "" || !errors.Is(err, storageFailure) {
		t.Fatalf("%s %v", outcome, err)
	}
	if outcome, err := processFact(t, p, fact(1)); outcome != Duplicate || err != nil {
		t.Fatalf("ambiguous retry: %s %v", outcome, err)
	}
}

func TestWorkflowConcurrentDuplicateAndGenerationIsolation(t *testing.T) {
	s := &memoryStore{}
	p := processor(t, s)
	raw, err := lifecycle.Encode(fact(1))
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan Outcome, 16)
	failures := make(chan error, 16)
	var group sync.WaitGroup
	for range 16 {
		group.Add(1)
		go func() {
			defer group.Done()
			outcome, err := p.Process(context.Background(), "order.events.v1", []byte("order-1"), raw)
			results <- outcome
			failures <- err
		}()
	}
	group.Wait()
	close(results)
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	var applied, duplicates int
	for outcome := range results {
		if outcome == Applied {
			applied++
		} else if outcome == Duplicate {
			duplicates++
		}
	}
	if applied != 1 || duplicates != 15 {
		t.Fatalf("applied=%d duplicate=%d", applied, duplicates)
	}
	for _, target := range []Target{{Generation: "orders-v2", Mode: Shadow}, {Generation: "orders-v1", Mode: Rebuild}} {
		other, err := New(target, s)
		if err != nil {
			t.Fatal(err)
		}
		if outcome, err := processFact(t, other, fact(1)); outcome != Applied || err != nil {
			t.Fatalf("isolated replay: %s %v", outcome, err)
		}
	}
}

func TestWorkflowValidationBeforeStorage(t *testing.T) {
	for _, target := range []Target{{}, {Generation: "../live", Mode: Shadow}, {Generation: "orders", Mode: "live"}} {
		if _, err := New(target, &memoryStore{}); err == nil {
			t.Fatal("accepted unsafe target")
		}
	}
	if _, err := New(Target{Generation: "orders", Mode: Shadow}, nil); err == nil {
		t.Fatal("accepted nil store")
	}
	s := &memoryStore{}
	p := processor(t, s)
	raw, err := lifecycle.Encode(fact(1))
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range []struct {
		topic, key string
		raw        []byte
	}{
		{"ledger.journals.v1", "order-1", raw},
		{"order.events.v1", "wrong", raw},
		{"order.events.v1", "order-1", []byte("bad")},
		{"order.events.v1", "order-1", make([]byte, lifecycle.MaxEventBytes+1)},
	} {
		if _, err := p.Process(context.Background(), record.topic, []byte(record.key), record.raw); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.Process(ctx, "order.events.v1", []byte("order-1"), raw); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if s.targets != nil {
		t.Fatal("invalid input reached storage")
	}
}
