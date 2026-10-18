package messaging

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
	"praxis/ledgerservice/internal/store"
)

const JournalKafkaTopic = "ledger.journals.v1"

type JournalKafkaConfig struct {
	Brokers []string
	// Transport is required and remains caller-owned. Supply the deployment's
	// SASL mechanism here; this adapter never loads credentials from environment.
	Transport   *kafka.Transport
	SendTimeout time.Duration
}

type journalKafkaWriter interface {
	WriteMessages(context.Context, ...kafka.Message) error
	Close() error
}

// JournalKafkaPublisher acknowledges synchronous Kafka writes. It provides
// neither transactional producer fencing nor exactly-once delivery. Timeout
// can leave a send in flight; callers must retain immutable retry coordinates.
type JournalKafkaPublisher struct {
	writer  journalKafkaWriter
	timeout time.Duration
}

var _ store.JournalPublisher = (*JournalKafkaPublisher)(nil)

func NewJournalKafkaPublisher(cfg JournalKafkaConfig) (*JournalKafkaPublisher, error) {
	if len(cfg.Brokers) == 0 || cfg.SendTimeout <= 0 || cfg.SendTimeout > 30*time.Second {
		return nil, errors.New("journal Kafka requires brokers and send timeout in (0, 30s]")
	}
	for _, broker := range cfg.Brokers {
		if strings.TrimSpace(broker) == "" {
			return nil, errors.New("journal Kafka broker must not be empty")
		}
	}
	if cfg.Transport == nil || cfg.Transport.TLS == nil || cfg.Transport.TLS.InsecureSkipVerify || cfg.Transport.TLS.MinVersion < tls.VersionTLS12 {
		return nil, errors.New("journal Kafka requires verified TLS with minimum TLS 1.2")
	}
	return &JournalKafkaPublisher{timeout: cfg.SendTimeout, writer: &kafka.Writer{
		Addr: kafka.TCP(cfg.Brokers...), Topic: JournalKafkaTopic,
		Transport: cfg.Transport, Balancer: &kafka.Hash{},
		RequiredAcks: kafka.RequireAll, Async: false,
		// Dispatch already holds a database connection. Do not wait for batches
		// or multiply attempts below the source-side retry policy.
		BatchSize: 1, BatchBytes: 2 * lifecycle.MaxEventBytes, MaxAttempts: 1,
		ReadTimeout: cfg.SendTimeout, WriteTimeout: cfg.SendTimeout,
		AllowAutoTopicCreation: false,
	}}, nil
}

func (p *JournalKafkaPublisher) PublishJournal(ctx context.Context, publication store.JournalPublication) error {
	if p == nil || p.writer == nil || p.timeout <= 0 {
		return errors.New("journal Kafka publisher is not configured")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	event, err := lifecycle.Decode(publication.Payload)
	if err != nil {
		return fmt.Errorf("validate journal Kafka payload: %w", err)
	}
	journal := event.GetLedgerJournal()
	if journal == nil || publication.EventID != event.EventId || publication.JournalID != journal.JournalId {
		return errors.New("journal Kafka coordinates do not match payload")
	}
	sendCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	// kafka-go may retain messages after a cancelled WriteMessages call. Copy
	// bytes so source retry buffers cannot alter an older in-flight send.
	err = p.writer.WriteMessages(sendCtx, kafka.Message{
		Key: []byte(publication.JournalID), Value: bytes.Clone(publication.Payload),
		Time: event.OccurredAt.AsTime(),
	})
	if err != nil {
		return fmt.Errorf("publish journal Kafka record: %w", err)
	}
	return nil
}

// Close must follow worker cancellation and join. The caller closes its shared
// transport's idle connections after all publishers using it have stopped.
func (p *JournalKafkaPublisher) Close() error { return p.writer.Close() }
