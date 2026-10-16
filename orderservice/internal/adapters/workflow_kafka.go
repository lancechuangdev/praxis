package adapters

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	lifecycle "praxis/api/events/lifecycle/v1"
	"praxis/orderservice/internal/order"
)

const WorkflowKafkaTopic = "order.events.v1"

type WorkflowKafkaConfig struct {
	Brokers []string
	// Transport is required and remains caller-owned. Supply the deployment's
	// SASL mechanism here; this adapter never loads credentials from environment.
	Transport   *kafka.Transport
	SendTimeout time.Duration
}

type workflowKafkaWriter interface {
	WriteMessages(context.Context, ...kafka.Message) error
	Close() error
}

// WorkflowKafkaPublisher acknowledges synchronous Kafka writes. It provides
// neither transactional producer fencing nor exactly-once delivery. Timeout
// can leave a send in flight; callers must retain immutable retry coordinates.
type WorkflowKafkaPublisher struct {
	writer  workflowKafkaWriter
	timeout time.Duration
}

var _ order.WorkflowPublisher = (*WorkflowKafkaPublisher)(nil)

func NewWorkflowKafkaPublisher(cfg WorkflowKafkaConfig) (*WorkflowKafkaPublisher, error) {
	if len(cfg.Brokers) == 0 || cfg.SendTimeout <= 0 || cfg.SendTimeout > 30*time.Second {
		return nil, errors.New("workflow Kafka requires brokers and send timeout in (0, 30s]")
	}
	for _, broker := range cfg.Brokers {
		if strings.TrimSpace(broker) == "" {
			return nil, errors.New("workflow Kafka broker must not be empty")
		}
	}
	if cfg.Transport == nil || cfg.Transport.TLS == nil || cfg.Transport.TLS.InsecureSkipVerify || cfg.Transport.TLS.MinVersion < tls.VersionTLS12 {
		return nil, errors.New("workflow Kafka requires verified TLS with minimum TLS 1.2")
	}
	return &WorkflowKafkaPublisher{timeout: cfg.SendTimeout, writer: &kafka.Writer{
		Addr: kafka.TCP(cfg.Brokers...), Topic: WorkflowKafkaTopic,
		Transport: cfg.Transport, Balancer: &kafka.Hash{},
		RequiredAcks: kafka.RequireAll, Async: false,
		// Dispatch already holds a database connection. Do not wait for batches
		// or multiply attempts below the source-side retry policy.
		BatchSize: 1, BatchBytes: 2 * lifecycle.MaxEventBytes, MaxAttempts: 1,
		ReadTimeout: cfg.SendTimeout, WriteTimeout: cfg.SendTimeout,
		AllowAutoTopicCreation: false,
	}}, nil
}

func (p *WorkflowKafkaPublisher) PublishWorkflow(ctx context.Context, publication order.WorkflowPublication) error {
	if p == nil || p.writer == nil || p.timeout <= 0 {
		return errors.New("workflow Kafka publisher is not configured")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	event, err := lifecycle.Decode(publication.Payload)
	if err != nil {
		return fmt.Errorf("validate workflow Kafka payload: %w", err)
	}
	workflow := event.GetOrderWorkflow()
	if workflow == nil || publication.Version <= 0 || publication.EventID != event.EventId || publication.OrderID != workflow.OrderId || uint64(publication.Version) != workflow.WorkflowVersion {
		return errors.New("workflow Kafka coordinates do not match payload")
	}
	sendCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	// kafka-go may retain messages after a cancelled WriteMessages call. Copy
	// bytes so source retry buffers cannot alter an older in-flight send.
	err = p.writer.WriteMessages(sendCtx, kafka.Message{
		Key: []byte(publication.OrderID), Value: bytes.Clone(publication.Payload),
		Time: event.OccurredAt.AsTime(),
	})
	if err != nil {
		return fmt.Errorf("publish workflow Kafka record: %w", err)
	}
	return nil
}

// Close must follow worker cancellation and join. The caller closes its shared
// transport's idle connections after all publishers using it have stopped.
func (p *WorkflowKafkaPublisher) Close() error { return p.writer.Close() }
