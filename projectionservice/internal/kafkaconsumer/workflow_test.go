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
	"praxis/projectionservice/internal/workflow"
)

type fakeReader struct {
	messages        []kafka.Message
	fetched, closed int
	commits         []kafka.Message
	commit          func(context.Context, kafka.Message) error
}

func (r *fakeReader) FetchMessage(context.Context) (kafka.Message, error) {
	r.fetched++
	if len(r.messages) == 0 {
		return kafka.Message{}, io.EOF
	}
	m := r.messages[0]
	r.messages = r.messages[1:]
	return m, nil
}
func (r *fakeReader) CommitMessages(ctx context.Context, messages ...kafka.Message) error {
	if len(messages) != 1 {
		panic("expected one-record commit")
	}
	r.commits = append(r.commits, messages[0])
	if r.commit != nil {
		return r.commit(ctx, messages[0])
	}
	return nil
}
func (r *fakeReader) Close() error { r.closed++; return nil }

type processFunc func(context.Context, string, []byte, []byte) (workflow.Outcome, error)

func (f processFunc) Process(ctx context.Context, topic string, key, raw []byte) (workflow.Outcome, error) {
	return f(ctx, topic, key, raw)
}
func message(offset int64) kafka.Message {
	return kafka.Message{Topic: "order.events.v1", Partition: 0, Offset: offset, Key: []byte("order-1"), Value: []byte("fact")}
}
func consumer(r *fakeReader, p processFunc) *Consumer {
	return &Consumer{reader: r, processor: p, commitTimeout: time.Second}
}

func TestSequentialProcessingBeforeSynchronousCommit(t *testing.T) {
	r := &fakeReader{messages: []kafka.Message{message(10), message(11)}}
	processed := 0
	r.commit = func(ctx context.Context, m kafka.Message) error {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > time.Second {
			t.Error("unbounded commit")
		}
		if processed != len(r.commits) || m.Offset != int64(9+processed) {
			t.Error("commit preceded processing or skipped a record")
		}
		return nil
	}
	c := consumer(r, func(_ context.Context, topic string, key, raw []byte) (workflow.Outcome, error) {
		if len(r.commits) != processed {
			t.Error("fetched ahead of commit")
		}
		if topic != "order.events.v1" || string(key) != "order-1" || string(raw) != "fact" {
			t.Error("changed source record")
		}
		processed++
		if processed == 1 {
			return workflow.Applied, nil
		}
		return workflow.Duplicate, nil
	})
	if err := c.Run(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if processed != 2 || len(r.commits) != 2 || r.closed != 1 {
		t.Fatalf("processed=%d commits=%d closed=%d", processed, len(r.commits), r.closed)
	}
	for _, metric := range []string{"projection_workflow_applied_total 1\n", "projection_workflow_duplicates_total 1\n", "projection_workflow_committed_total 2\n", "projection_workflow_consumer_active 0\n"} {
		if !strings.Contains(c.Prometheus(), metric) {
			t.Fatal(c.Prometheus())
		}
	}
}

func TestFailedHeadNeverFetchesOrCommitsSuccessor(t *testing.T) {
	for _, failure := range []error{workflow.ErrGap, workflow.ErrStale, workflow.ErrConflict, workflow.ErrInvalid, errors.New("database unavailable")} {
		t.Run(failure.Error(), func(t *testing.T) {
			r := &fakeReader{messages: []kafka.Message{message(10), message(11)}}
			c := consumer(r, func(context.Context, string, []byte, []byte) (workflow.Outcome, error) { return "", failure })
			if err := c.Run(context.Background()); !errors.Is(err, failure) {
				t.Fatal(err)
			}
			if r.fetched != 1 || len(r.commits) != 0 || r.closed != 1 {
				t.Fatal("failed head was skipped")
			}
			if err := c.Run(context.Background()); err == nil {
				t.Fatal("reused reader after failed head")
			}
			if r.fetched != 1 || r.closed != 1 {
				t.Fatal("second Run touched failed reader")
			}
		})
	}
}

func TestAmbiguousCommitStopsAndFreshReaderCanDeduplicate(t *testing.T) {
	commitFailure := errors.New("commit acknowledgement lost")
	r := &fakeReader{messages: []kafka.Message{message(10), message(11)}, commit: func(context.Context, kafka.Message) error { return commitFailure }}
	c := consumer(r, func(context.Context, string, []byte, []byte) (workflow.Outcome, error) { return workflow.Applied, nil })
	if err := c.Run(context.Background()); !errors.Is(err, commitFailure) {
		t.Fatal(err)
	}
	if r.fetched != 1 || len(r.commits) != 1 || r.closed != 1 {
		t.Fatal("advanced after ambiguous commit")
	}
	restartReader := &fakeReader{messages: []kafka.Message{message(10)}}
	restart := consumer(restartReader, func(context.Context, string, []byte, []byte) (workflow.Outcome, error) {
		return workflow.Duplicate, nil
	})
	if err := restart.Run(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if len(restartReader.commits) != 1 || restartReader.commits[0].Offset != 10 {
		t.Fatal("redelivery was not committed")
	}
}

func TestCancellationDuplicateRunAndUnusedClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := &fakeReader{messages: []kafka.Message{message(10)}}
	entered := make(chan struct{})
	c := consumer(r, func(ctx context.Context, _ string, _ []byte, _ []byte) (workflow.Outcome, error) {
		close(entered)
		<-ctx.Done()
		return "", ctx.Err()
	})
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("processor did not start")
	}
	if err := c.Run(ctx); err == nil {
		t.Fatal("allowed simultaneous Run")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown stalled")
	}
	if len(r.commits) != 0 || r.closed != 1 {
		t.Fatal("cancelled event committed or reader leaked")
	}
	unusedReader := &fakeReader{}
	unused := consumer(unusedReader, func(context.Context, string, []byte, []byte) (workflow.Outcome, error) {
		t.Fatal("unused consumer processed")
		return "", nil
	})
	if err := unused.Close(); err != nil {
		t.Fatal(err)
	}
	if err := unused.Run(context.Background()); err == nil {
		t.Fatal("ran closed consumer")
	}
	if unusedReader.closed != 1 || unusedReader.fetched != 0 {
		t.Fatal("unused reader leaked or was fetched")
	}
}

func TestUnknownOutcomeAndCommitDeadlineFailClosed(t *testing.T) {
	r := &fakeReader{messages: []kafka.Message{message(0)}}
	c := consumer(r, func(context.Context, string, []byte, []byte) (workflow.Outcome, error) { return "", nil })
	if err := c.Run(context.Background()); err == nil || len(r.commits) != 0 {
		t.Fatal("committed an unknown outcome")
	}
	r = &fakeReader{messages: []kafka.Message{message(0), message(1)}, commit: func(ctx context.Context, _ kafka.Message) error { <-ctx.Done(); return ctx.Err() }}
	c = consumer(r, func(context.Context, string, []byte, []byte) (workflow.Outcome, error) { return workflow.Applied, nil })
	c.commitTimeout = time.Millisecond
	if err := c.Run(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if r.fetched != 1 || r.closed != 1 {
		t.Fatal("commit deadline advanced or leaked reader")
	}
}

func TestReaderConfiguration(t *testing.T) {
	valid := func() Config {
		return Config{Brokers: []string{"broker:9094"}, Target: workflow.Target{Generation: "orders-v1", Mode: workflow.Shadow}, GroupNamespace: "dev-db1", Dialer: &kafka.Dialer{TLS: &tls.Config{MinVersion: tls.VersionTLS12}}, CommitTimeout: time.Second}
	}
	for _, mutate := range []func(*Config){
		func(c *Config) { c.Brokers = nil }, func(c *Config) { c.Brokers = []string{" "} },
		func(c *Config) { c.Target.Generation = "../bad" }, func(c *Config) { c.Target.Mode = "live" },
		func(c *Config) { c.GroupNamespace = "" }, func(c *Config) { c.GroupNamespace = "ambiguous.namespace" },
		func(c *Config) { c.CommitTimeout = 0 }, func(c *Config) { c.CommitTimeout = 31 * time.Second },
		func(c *Config) { c.Dialer = nil }, func(c *Config) { c.Dialer.TLS = nil },
		func(c *Config) { c.Dialer.TLS.InsecureSkipVerify = true }, func(c *Config) { c.Dialer.TLS.MinVersion = tls.VersionTLS11 },
	} {
		cfg := valid()
		mutate(&cfg)
		if _, err := readerConfig(cfg); err == nil {
			t.Fatal("accepted unsafe reader configuration")
		}
	}
	cfg := valid()
	rc, err := readerConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if rc.CommitInterval != 0 || rc.StartOffset != kafka.FirstOffset || rc.QueueCapacity != 1 || rc.Topic != "order.events.v1" || rc.Dialer.Timeout > 5*time.Second {
		t.Fatal("unsafe source settings")
	}
	if err := rc.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*Config){func(c *Config) { c.Target.Generation = "orders-v2" }, func(c *Config) { c.Target.Mode = workflow.Rebuild }, func(c *Config) { c.GroupNamespace = "other-db" }} {
		other := valid()
		change(&other)
		next, err := readerConfig(other)
		if err != nil || next.GroupID == rc.GroupID {
			t.Fatal("consumer groups overlap")
		}
	}
	if _, err := New(cfg, nil); err == nil {
		t.Fatal("accepted missing store")
	}
}
