package kafkaconsumer

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"praxis/projectionservice/internal/journal"
	"praxis/projectionservice/internal/workflow"
)

type journalStoreFunc func(context.Context, journal.Target, func(journal.Transaction) error) error

func (f journalStoreFunc) Within(ctx context.Context, target journal.Target, fn func(journal.Transaction) error) error {
	return f(ctx, target, fn)
}
func TestJournalConsumerIsolation(t *testing.T) {
	cfg := Config{Stream: "journal", Brokers: []string{"broker:9094"}, Target: workflow.Target{Generation: "test", Mode: workflow.Shadow}, GroupNamespace: "dev", Dialer: &kafka.Dialer{TLS: &tls.Config{MinVersion: tls.VersionTLS12}}, CommitTimeout: time.Second}
	rc, err := readerConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if rc.Topic != "ledger.journals.v1" || rc.GroupID != "projection-journal-dev.shadow.test" || rc.CommitInterval != 0 || rc.QueueCapacity != 1 {
		t.Fatal("journal consumer isolation or commit policy missing", rc)
	}
	s := journalStoreFunc(func(context.Context, journal.Target, func(journal.Transaction) error) error {
		t.Fatal("unexpected store operation")
		return nil
	})
	c, err := NewJournal(cfg, s)
	if err != nil {
		t.Fatal(err)
	}
	if c.topicName() != "ledger.journals.v1" || !strings.Contains(c.Prometheus(), "projection_journal_consumer_active 0") {
		t.Fatal("journal diagnostics incorrect")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Stream = "workflow"
	if _, err := NewJournal(cfg, s); err == nil {
		t.Fatal("cross-stream journal configuration accepted")
	}
	cfg.Stream = "unknown"
	if _, err := readerConfig(cfg); err == nil {
		t.Fatal("unknown stream accepted")
	}
}
func TestJournalLoopCommitBarrierAndSanitizedStatus(t *testing.T) {
	for _, failure := range []error{workflow.ErrGap, workflow.ErrConflict, nil} {
		r := &fakeReader{messages: []kafka.Message{{Topic: "ledger.journals.v1", Partition: 1, Offset: 10}, {Topic: "ledger.journals.v1", Partition: 1, Offset: 11}}}
		calls := 0
		c := consumer(r, func(context.Context, string, []byte, []byte) (workflow.Outcome, error) {
			calls++
			if failure != nil {
				return "", failure
			}
			return workflow.Applied, nil
		})
		c.topic = "ledger.journals.v1"
		err := c.Run(context.Background())
		if failure != nil {
			if !errors.Is(err, failure) || len(r.commits) != 0 || calls != 1 {
				t.Fatal("failed journal advanced offsets", err, r.commits, calls)
			}
			if c.Status().Current.Topic != "ledger.journals.v1" || c.Status().Failure == nil {
				t.Fatal("missing journal failure coordinates")
			}
		} else if !errors.Is(err, io.EOF) || len(r.commits) != 2 || c.Status().LastCommitted.Offset != 11 {
			t.Fatal(err, r.commits, c.Status())
		}
	}
}
