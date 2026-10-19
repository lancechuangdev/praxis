package kafkaconsumer

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"praxis/projectionservice/internal/workflow"
)

type fakeQueue struct {
	target  workflow.Target
	stream  string
	err     error
	records []kafka.Message
}

func (q *fakeQueue) Scope() (workflow.Target, string) { return q.target, q.stream }
func (q *fakeQueue) ProcessRecord(_ context.Context, m kafka.Message) (workflow.Outcome, error) {
	q.records = append(q.records, m)
	if q.err != nil {
		return "", q.err
	}
	return workflow.Applied, nil
}
func TestQueuedConsumerCommitsOnlyAfterDurableEnqueue(t *testing.T) {
	failure := errors.New("Mongo transaction failed")
	for _, err := range []error{nil, failure} {
		target := workflow.Target{Generation: "test", Mode: workflow.Shadow}
		q := &fakeQueue{target: target, stream: "journal", err: err}
		cfg := Config{Stream: "journal", Target: target, GroupNamespace: "test", Brokers: []string{"broker:9094"}, Dialer: &kafka.Dialer{TLS: &tls.Config{MinVersion: tls.VersionTLS12}}, CommitTimeout: time.Second}
		c, createErr := NewQueued(cfg, q)
		if createErr != nil {
			t.Fatal(createErr)
		}
		_ = c.reader.Close()
		r := &fakeReader{messages: []kafka.Message{{Topic: "ledger.journals.v1", Partition: 2, Offset: 10, Key: []byte("key"), Value: []byte("bytes")}}}
		c.reader = r
		result := c.Run(context.Background())
		if len(q.records) != 1 || q.records[0].Offset != 10 || q.records[0].Partition != 2 {
			t.Fatal("source coordinates lost")
		}
		if err != nil {
			if !errors.Is(result, err) || len(r.commits) != 0 {
				t.Fatal("failed enqueue advanced offset", result, r.commits)
			}
		} else if !errors.Is(result, io.EOF) || len(r.commits) != 1 {
			t.Fatal(result, r.commits)
		}
		cfg.Target.Generation = "other"
		if _, err := NewQueued(cfg, q); err == nil {
			t.Fatal("queue scope mismatch accepted")
		}
	}
}
