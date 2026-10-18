package journal

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

// Transaction model only; these tests do not establish MongoDB behavior.
type model struct {
	mu      sync.Mutex
	targets map[Target]*transaction
	fail    string
	loseAck bool
	locks   []string
}
type transaction struct {
	accounts map[string]*wire.AccountBalanceSnapshot
	journals map[string]*wire.LifecycleEvent
	inbox    map[string]Receipt
	model    *model
	writes   int
}

var storageFailure = errors.New("storage failure")

func (m *model) Within(ctx context.Context, target Target, fn func(Transaction) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	next := &transaction{accounts: map[string]*wire.AccountBalanceSnapshot{}, journals: map[string]*wire.LifecycleEvent{}, inbox: map[string]Receipt{}, model: m}
	if prior := m.targets[target]; prior != nil {
		for k, v := range prior.accounts {
			next.accounts[k] = proto.Clone(v).(*wire.AccountBalanceSnapshot)
		}
		for k, v := range prior.journals {
			next.journals[k] = proto.Clone(v).(*wire.LifecycleEvent)
		}
		for k, v := range prior.inbox {
			next.inbox[k] = v
		}
	}
	if err := fn(next); err != nil {
		return err
	}
	if m.fail == "commit" {
		return storageFailure
	}
	if m.targets == nil {
		m.targets = map[Target]*transaction{}
	}
	m.targets[target] = next
	if m.loseAck {
		m.loseAck = false
		return storageFailure
	}
	return nil
}
func (t *transaction) Inbox(_ context.Context, id string) (Receipt, bool, error) {
	if t.model.fail == "inbox-read" {
		return Receipt{}, false, storageFailure
	}
	v, ok := t.inbox[id]
	return v, ok, nil
}
func (t *transaction) Journal(_ context.Context, id string) (bool, error) {
	if t.model.fail == "journal-read" {
		return false, storageFailure
	}
	_, ok := t.journals[id]
	return ok, nil
}
func (t *transaction) LockAccount(_ context.Context, id string) (State, error) {
	t.model.locks = append(t.model.locks, id)
	if t.model.fail == "lock" {
		return State{}, storageFailure
	}
	if b := t.accounts[id]; b != nil {
		return State{Version: b.Version, UserID: b.UserId, AssetID: b.AssetId}, nil
	}
	return State{}, nil
}
func (t *transaction) PutAccount(_ context.Context, b *wire.AccountBalanceSnapshot) error {
	t.writes++
	if t.model.fail == fmt.Sprintf("account-%d", t.writes) {
		return storageFailure
	}
	t.accounts[b.UserAssetAccountId] = proto.Clone(b).(*wire.AccountBalanceSnapshot)
	return nil
}
func (t *transaction) PutJournal(_ context.Context, e *wire.LifecycleEvent) error {
	if t.model.fail == "journal-write" {
		return storageFailure
	}
	t.journals[e.GetLedgerJournal().JournalId] = proto.Clone(e).(*wire.LifecycleEvent)
	return nil
}
func (t *transaction) PutInbox(_ context.Context, id string, r Receipt) error {
	if t.model.fail == "inbox-write" {
		return storageFailure
	}
	t.inbox[id] = r
	return nil
}

func event(id string, previous, version uint64) *wire.LifecycleEvent {
	return &wire.LifecycleEvent{EventId: "event-" + id, EventType: lifecycle.LedgerJournalType, SchemaVersion: 1, Producer: "ledger-service", CorrelationId: "request", CausationId: "command", OccurredAt: timestamppb.New(time.Unix(1700000000, 0)), Payload: &wire.LifecycleEvent_LedgerJournal{LedgerJournal: &wire.LedgerJournalPosted{
		JournalId: id, BusinessType: "transfer", ReferenceId: "reference", CommandId: "command", SourceSystem: "ledger-service",
		Entries: []*wire.JournalEntry{
			{EntryId: id + "-a", AccountId: "customer", UserAssetAccountId: "b", AssetId: "USD", Bucket: "available", Side: wire.PostingSide_POSTING_SIDE_DEBIT, AmountAtomic: "9007199254740993"},
			{EntryId: id + "-b", AccountId: "customer", UserAssetAccountId: "a", AssetId: "USD", Bucket: "available", Side: wire.PostingSide_POSTING_SIDE_CREDIT, AmountAtomic: "9007199254740993"},
		}, Balances: []*wire.AccountBalanceSnapshot{
			{UserAssetAccountId: "b", UserId: "bob", AssetId: "USD", PreviousVersion: previous, Version: version, AvailableAtomic: "9007199254740993", ReservedAtomic: "0", HoldAtomic: "0", WithdrawalPendingAtomic: "0", DepositPendingAtomic: "0"},
			{UserAssetAccountId: "a", UserId: "alice", AssetId: "USD", PreviousVersion: previous, Version: version, AvailableAtomic: "9007199254740993", ReservedAtomic: "0", HoldAtomic: "0", WithdrawalPendingAtomic: "0", DepositPendingAtomic: "0"},
		},
	}}}
}
func process(t *testing.T, p *Processor, e *wire.LifecycleEvent) (Outcome, error) {
	t.Helper()
	raw, err := lifecycle.Encode(e)
	if err != nil {
		t.Fatal(err)
	}
	return p.Process(context.Background(), "ledger.journals.v1", []byte(e.GetLedgerJournal().JournalId), raw)
}

var target = Target{Generation: "test", Mode: Shadow}

func processor(t *testing.T, m *model) *Processor {
	t.Helper()
	p, err := New(target, m)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestAtomicAccountsDedupAndIncreasingChains(t *testing.T) {
	m := &model{}
	p := processor(t, m)
	e := event("first", 0, 2)
	if out, err := process(t, p, e); err != nil || out != Applied {
		t.Fatal(out, err)
	}
	if len(m.locks) != 2 || m.locks[0] != "a" || m.locks[1] != "b" {
		t.Fatal("nondeterministic account lock order", m.locks)
	}
	if out, err := process(t, p, e); err != nil || out != Duplicate {
		t.Fatal(out, err)
	}
	if out, err := process(t, p, event("next", 2, 5)); err != nil || out != Applied {
		t.Fatal(out, err)
	}
	state := m.targets[target]
	if len(state.journals) != 2 || len(state.inbox) != 2 || state.accounts["a"].Version != 5 || state.accounts["b"].AvailableAtomic != "9007199254740993" {
		t.Fatal("incomplete projection")
	}
}

func TestGapCanRecoverAfterPredecessorArrives(t *testing.T) {
	m := &model{}
	p := processor(t, m)
	later := event("later", 2, 5)
	if _, err := process(t, p, later); !errors.Is(err, ErrGap) {
		t.Fatal(err)
	}
	if m.targets[target] != nil {
		t.Fatal("gap wrote projection state")
	}
	if _, err := process(t, p, event("first", 0, 2)); err != nil {
		t.Fatal(err)
	}
	if out, err := process(t, p, later); out != Applied || err != nil {
		t.Fatal(out, err)
	}
}
func TestFailuresRollbackEveryDocument(t *testing.T) {
	for _, fail := range []string{"inbox-read", "journal-read", "lock", "account-1", "account-2", "journal-write", "inbox-write", "commit"} {
		t.Run(fail, func(t *testing.T) {
			m := &model{fail: fail}
			p := processor(t, m)
			if out, err := process(t, p, event("first", 0, 1)); out != "" || !errors.Is(err, storageFailure) {
				t.Fatal(out, err)
			}
			if m.targets[target] != nil {
				t.Fatal("partial transaction committed")
			}
			m.fail = ""
			if out, err := process(t, p, event("first", 0, 1)); out != Applied || err != nil {
				t.Fatal(out, err)
			}
		})
	}
}
func TestAmbiguousCommitAndConcurrentReplay(t *testing.T) {
	m := &model{loseAck: true}
	p := processor(t, m)
	e := event("first", 0, 1)
	if _, err := process(t, p, e); !errors.Is(err, storageFailure) {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for range 8 {
		group.Add(1)
		go func() {
			defer group.Done()
			if out, err := process(t, p, e); out != Duplicate || err != nil {
				t.Error(out, err)
			}
		}()
	}
	group.Wait()
	if len(m.targets[target].journals) != 1 {
		t.Fatal("duplicate journal")
	}
}
func TestGapsStaleConflictsAndNoPartialApplication(t *testing.T) {
	for _, test := range []struct {
		name   string
		want   error
		change func(*wire.LifecycleEvent)
	}{
		{"gap", ErrGap, func(e *wire.LifecycleEvent) {
			e.GetLedgerJournal().Balances[0].PreviousVersion = 3
			e.GetLedgerJournal().Balances[0].Version = 4
		}},
		{"stale", ErrStale, func(e *wire.LifecycleEvent) {
			e.GetLedgerJournal().Balances[0].PreviousVersion = 0
			e.GetLedgerJournal().Balances[0].Version = 2
		}},
		{"overlap", ErrConflict, func(e *wire.LifecycleEvent) { e.GetLedgerJournal().Balances[0].PreviousVersion = 1 }},
		{"owner", ErrConflict, func(e *wire.LifecycleEvent) { e.GetLedgerJournal().Balances[0].UserId = "mallory" }},
		{"journal-id", ErrConflict, func(e *wire.LifecycleEvent) { e.GetLedgerJournal().JournalId = "first" }},
		{"event-id", ErrConflict, func(e *wire.LifecycleEvent) { e.EventId = "event-first" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			m := &model{}
			p := processor(t, m)
			if _, err := process(t, p, event("first", 0, 2)); err != nil {
				t.Fatal(err)
			}
			e := event("second", 2, 3)
			test.change(e)
			if out, err := process(t, p, e); out != "" || !errors.Is(err, test.want) {
				t.Fatal(out, err)
			}
			if len(m.targets[target].journals) != 1 || m.targets[target].accounts["a"].Version != 2 {
				t.Fatal("partial journal applied")
			}
		})
	}
}
func TestTargetIsolationValidationAndCancellation(t *testing.T) {
	m := &model{}
	p := processor(t, m)
	e := event("first", 0, 1)
	for _, target := range []Target{{Generation: "test", Mode: Shadow}, {Generation: "test", Mode: Rebuild}, {Generation: "other", Mode: Shadow}} {
		other, err := New(target, m)
		if err != nil {
			t.Fatal(err)
		}
		if out, err := process(t, other, e); out != Applied || err != nil {
			t.Fatal(out, err)
		}
	}
	for _, target := range []Target{{}, {Generation: "bad/name", Mode: Shadow}, {Generation: "test", Mode: "live"}} {
		if _, err := New(target, m); err == nil {
			t.Fatal("unsafe target accepted")
		}
	}
	if _, err := New(target, nil); err == nil {
		t.Fatal("nil store accepted")
	}
	raw, err := lifecycle.Encode(e)
	if err != nil {
		t.Fatal(err)
	}
	for _, input := range []struct {
		topic, key string
		raw        []byte
	}{{"other", "first", raw}, {"ledger.journals.v1", "wrong", raw}, {"ledger.journals.v1", "first", []byte("bad")}} {
		if _, err := p.Process(context.Background(), input.topic, []byte(input.key), input.raw); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.Process(ctx, "ledger.journals.v1", []byte("first"), raw); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
