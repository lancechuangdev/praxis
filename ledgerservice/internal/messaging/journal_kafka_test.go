package messaging

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/types/known/timestamppb"
	lifecycle "praxis/api/events/lifecycle/v1"
	wire "praxis/api/gen/events/lifecycle/v1"
	"praxis/ledgerservice/internal/store"
)

type journalWriterStub struct {
	write  func(context.Context, ...kafka.Message) error
	closed bool
}

func (s *journalWriterStub) WriteMessages(ctx context.Context, messages ...kafka.Message) error {
	return s.write(ctx, messages...)
}
func (s *journalWriterStub) Close() error { s.closed = true; return nil }

func journalPublication(t *testing.T) store.JournalPublication {
	t.Helper()
	e := &wire.LifecycleEvent{EventId: "event-1", EventType: lifecycle.LedgerJournalType, SchemaVersion: 1, Producer: "ledger-service", CorrelationId: "request-1", CausationId: "request-1", OccurredAt: timestamppb.New(time.Unix(1700000000, 123)), Payload: &wire.LifecycleEvent_LedgerJournal{LedgerJournal: &wire.LedgerJournalPosted{
		JournalId: "journal-1", BusinessType: "deposit", ReferenceId: "deposit-1", SourceSystem: "custody", CommandId: "command-1",
		Entries: []*wire.JournalEntry{
			{EntryId: "entry-1", AccountId: "custody", AssetId: "USD", Side: wire.PostingSide_POSTING_SIDE_DEBIT, AmountAtomic: "9007199254740993", CustodyPositionId: "custody-1"},
			{EntryId: "entry-2", AccountId: "customer", AssetId: "USD", Side: wire.PostingSide_POSTING_SIDE_CREDIT, AmountAtomic: "9007199254740993", UserAssetAccountId: "account-1", Bucket: "available"},
		},
		Balances: []*wire.AccountBalanceSnapshot{{UserAssetAccountId: "account-1", UserId: "alice", AssetId: "USD", Version: 1, AvailableAtomic: "9007199254740993", ReservedAtomic: "0", HoldAtomic: "0", DepositPendingAtomic: "0", WithdrawalPendingAtomic: "0"}},
	}}}
	payload, err := lifecycle.Encode(e)
	if err != nil {
		t.Fatal(err)
	}
	return store.JournalPublication{EventID: e.EventId, JournalID: "journal-1", Payload: payload}
}

func TestJournalKafkaConfiguration(t *testing.T) {
	valid := func() JournalKafkaConfig {
		return JournalKafkaConfig{Brokers: []string{"broker:9094"}, Transport: &kafka.Transport{TLS: &tls.Config{MinVersion: tls.VersionTLS12}}, SendTimeout: time.Second}
	}
	for _, mutate := range []func(*JournalKafkaConfig){
		func(c *JournalKafkaConfig) { c.Brokers = nil },
		func(c *JournalKafkaConfig) { c.Brokers = []string{" "} },
		func(c *JournalKafkaConfig) { c.SendTimeout = 0 },
		func(c *JournalKafkaConfig) { c.SendTimeout = 31 * time.Second },
		func(c *JournalKafkaConfig) { c.Transport = nil },
		func(c *JournalKafkaConfig) { c.Transport.TLS = nil },
		func(c *JournalKafkaConfig) { c.Transport.TLS.InsecureSkipVerify = true },
		func(c *JournalKafkaConfig) { c.Transport.TLS.MinVersion = tls.VersionTLS11 },
	} {
		cfg := valid()
		mutate(&cfg)
		if _, err := NewJournalKafkaPublisher(cfg); err == nil {
			t.Fatal("accepted unsafe configuration")
		}
	}
	p, err := NewJournalKafkaPublisher(valid())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	w := p.writer.(*kafka.Writer)
	if w.Topic != JournalKafkaTopic || w.RequiredAcks != kafka.RequireAll || w.Async || w.MaxAttempts != 1 || w.BatchSize != 1 || w.AllowAutoTopicCreation {
		t.Fatal("unsafe Kafka writer policy")
	}
	if _, ok := w.Balancer.(*kafka.Hash); !ok {
		t.Fatal("missing keyed hash balancing")
	}
}

func TestJournalKafkaPreservesFactAndWaitsForAcknowledgement(t *testing.T) {
	event := journalPublication(t)
	original := bytes.Clone(event.Payload)
	entered, release := make(chan struct{}), make(chan struct{})
	var captured kafka.Message
	stub := &journalWriterStub{write: func(ctx context.Context, messages ...kafka.Message) error {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > time.Second {
			t.Error("missing send deadline")
		}
		if len(messages) != 1 {
			t.Error("unexpected batch")
		}
		captured = messages[0]
		close(entered)
		<-release
		return nil
	}}
	p := &JournalKafkaPublisher{writer: stub, timeout: time.Second}
	done := make(chan error, 1)
	go func() { done <- p.PublishJournal(context.Background(), event) }()
	<-entered
	select {
	case <-done:
		t.Fatal("returned before acknowledgement")
	default:
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if string(captured.Key) != event.JournalID || !bytes.Equal(captured.Value, original) || !captured.Time.Equal(time.Unix(1700000000, 123)) {
		t.Fatal("changed immutable fact")
	}
	event.Payload[0] ^= 0xff
	if !bytes.Equal(captured.Value, original) {
		t.Fatal("writer retained caller-owned buffer")
	}
	if err := p.Close(); err != nil || !stub.closed {
		t.Fatal("writer not closed")
	}
}

func TestJournalKafkaRejectsInvalidCoordinatesBeforeSend(t *testing.T) {
	p := &JournalKafkaPublisher{timeout: time.Second, writer: &journalWriterStub{write: func(context.Context, ...kafka.Message) error { t.Fatal("invalid fact sent"); return nil }}}
	for _, mutate := range []func(*store.JournalPublication){
		func(e *store.JournalPublication) { e.EventID = "other" },
		func(e *store.JournalPublication) { e.JournalID = "other" },
		func(e *store.JournalPublication) { e.Payload = []byte("invalid") },
		func(e *store.JournalPublication) { e.Payload = make([]byte, lifecycle.MaxEventBytes+1) },
	} {
		e := journalPublication(t)
		mutate(&e)
		if err := p.PublishJournal(context.Background(), e); err == nil {
			t.Fatal("accepted invalid fact")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.PublishJournal(ctx, journalPublication(t)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestJournalKafkaPropagatesBrokerFailureAndDeadline(t *testing.T) {
	brokerErr := errors.New("broker unavailable")
	p := &JournalKafkaPublisher{timeout: time.Second, writer: &journalWriterStub{write: func(context.Context, ...kafka.Message) error { return brokerErr }}}
	if err := p.PublishJournal(context.Background(), journalPublication(t)); !errors.Is(err, brokerErr) {
		t.Fatal(err)
	}
	p.timeout = time.Millisecond
	p.writer = &journalWriterStub{write: func(ctx context.Context, _ ...kafka.Message) error { <-ctx.Done(); return ctx.Err() }}
	if err := p.PublishJournal(context.Background(), journalPublication(t)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}
