package messaging

import (
	"time"

	"github.com/segmentio/kafka-go"
	"praxis/outboxrelay/internal/kafkaauth"
)

type WriterConfig struct {
	Brokers      []string
	Auth         kafkaauth.Config
	BatchSize    int
	BatchBytes   int64
	BatchTimeout time.Duration
}

func NewWriter(cfg WriterConfig) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Transport:    cfg.Auth.Transport(),
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
		BatchSize:    cfg.BatchSize,
		BatchBytes:   cfg.BatchBytes,
		BatchTimeout: cfg.BatchTimeout,
		Compression:  kafka.Lz4,
		MaxAttempts:  10,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
}
