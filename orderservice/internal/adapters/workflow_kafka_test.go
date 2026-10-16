package adapters

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
	"praxis/orderservice/internal/order"
)

type workflowWriterStub struct {
	write  func(context.Context, ...kafka.Message) error
	closed bool
}

func (s *workflowWriterStub) WriteMessages(ctx context.Context, messages ...kafka.Message) error {
	return s.write(ctx, messages...)
}
func (s *workflowWriterStub) Close() error { s.closed = true; return nil }

func workflowPublication(t *testing.T) order.WorkflowPublication {
	t.Helper()
	e := &wire.LifecycleEvent{EventId: "event-1", EventType: lifecycle.OrderWorkflowType, SchemaVersion: 1, Producer: "order-service", CorrelationId: "request-1", CausationId: "request-1", OccurredAt: timestamppb.New(time.Unix(1700000000, 123)), Payload: &wire.LifecycleEvent_OrderWorkflow{OrderWorkflow: &wire.OrderWorkflowSnapshot{RequestId: "request-1", OrderId: "order-1", UserId: "alice", WorkflowVersion: 1, State: wire.OrderWorkflowState_ORDER_WORKFLOW_STATE_RESERVATION_PENDING, Symbol: "BTC-USDT", Side: "buy", OrderType: "market", Quantity: "1", ReserveAssetId: "USDT", ReserveAmountAtomic: "123"}}}
	payload, err := lifecycle.Encode(e)
	if err != nil {
		t.Fatal(err)
	}
	return order.WorkflowPublication{EventID: e.EventId, OrderID: "order-1", Version: 1, Payload: payload}
}

func TestWorkflowKafkaConfiguration(t *testing.T) {
	valid := func() WorkflowKafkaConfig {
		return WorkflowKafkaConfig{Brokers: []string{"broker:9094"}, Transport: &kafka.Transport{TLS: &tls.Config{MinVersion: tls.VersionTLS12}}, SendTimeout: time.Second}
	}
	for _, mutate := range []func(*WorkflowKafkaConfig){
		func(c *WorkflowKafkaConfig) { c.Brokers = nil },
		func(c *WorkflowKafkaConfig) { c.Brokers = []string{" "} },
		func(c *WorkflowKafkaConfig) { c.SendTimeout = 0 },
		func(c *WorkflowKafkaConfig) { c.SendTimeout = 31 * time.Second },
		func(c *WorkflowKafkaConfig) { c.Transport = nil },
		func(c *WorkflowKafkaConfig) { c.Transport.TLS = nil },
		func(c *WorkflowKafkaConfig) { c.Transport.TLS.InsecureSkipVerify = true },
		func(c *WorkflowKafkaConfig) { c.Transport.TLS.MinVersion = tls.VersionTLS11 },
	} {
		cfg := valid()
		mutate(&cfg)
		if _, err := NewWorkflowKafkaPublisher(cfg); err == nil {
			t.Fatal("accepted unsafe configuration")
		}
	}
	p, err := NewWorkflowKafkaPublisher(valid())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	w := p.writer.(*kafka.Writer)
	if w.Topic != WorkflowKafkaTopic || w.RequiredAcks != kafka.RequireAll || w.Async || w.MaxAttempts != 1 || w.BatchSize != 1 || w.AllowAutoTopicCreation {
		t.Fatal("unsafe Kafka writer policy")
	}
	if _, ok := w.Balancer.(*kafka.Hash); !ok {
		t.Fatal("missing keyed hash balancing")
	}
}

func TestWorkflowKafkaPreservesFactAndWaitsForAcknowledgement(t *testing.T) {
	event := workflowPublication(t)
	original := bytes.Clone(event.Payload)
	entered, release := make(chan struct{}), make(chan struct{})
	var captured kafka.Message
	stub := &workflowWriterStub{write: func(ctx context.Context, messages ...kafka.Message) error {
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
	p := &WorkflowKafkaPublisher{writer: stub, timeout: time.Second}
	done := make(chan error, 1)
	go func() { done <- p.PublishWorkflow(context.Background(), event) }()
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
	if string(captured.Key) != event.OrderID || !bytes.Equal(captured.Value, original) || !captured.Time.Equal(time.Unix(1700000000, 123)) {
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

func TestWorkflowKafkaRejectsInvalidCoordinatesBeforeSend(t *testing.T) {
	p := &WorkflowKafkaPublisher{timeout: time.Second, writer: &workflowWriterStub{write: func(context.Context, ...kafka.Message) error { t.Fatal("invalid fact sent"); return nil }}}
	for _, mutate := range []func(*order.WorkflowPublication){
		func(e *order.WorkflowPublication) { e.EventID = "other" },
		func(e *order.WorkflowPublication) { e.OrderID = "other" },
		func(e *order.WorkflowPublication) { e.Version = 2 },
		func(e *order.WorkflowPublication) { e.Version = -1 },
		func(e *order.WorkflowPublication) { e.Payload = []byte("invalid") },
		func(e *order.WorkflowPublication) { e.Payload = make([]byte, lifecycle.MaxEventBytes+1) },
	} {
		e := workflowPublication(t)
		mutate(&e)
		if err := p.PublishWorkflow(context.Background(), e); err == nil {
			t.Fatal("accepted invalid fact")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.PublishWorkflow(ctx, workflowPublication(t)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestWorkflowKafkaPropagatesBrokerFailureAndDeadline(t *testing.T) {
	brokerErr := errors.New("broker unavailable")
	p := &WorkflowKafkaPublisher{timeout: time.Second, writer: &workflowWriterStub{write: func(context.Context, ...kafka.Message) error { return brokerErr }}}
	if err := p.PublishWorkflow(context.Background(), workflowPublication(t)); !errors.Is(err, brokerErr) {
		t.Fatal(err)
	}
	p.timeout = time.Millisecond
	p.writer = &workflowWriterStub{write: func(ctx context.Context, _ ...kafka.Message) error { <-ctx.Done(); return ctx.Err() }}
	if err := p.PublishWorkflow(context.Background(), workflowPublication(t)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}
