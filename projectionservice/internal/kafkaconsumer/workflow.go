// Package kafkaconsumer supplies a fail-stop Order workflow consumer adapter.
// It does not provision topics, repair gaps, skip poison records or route reads.
package kafkaconsumer

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
	lifecycle "praxis/api/events/lifecycle/v1"
	"praxis/projectionservice/internal/workflow"
)

type Config struct {
	Brokers []string
	Target  workflow.Target
	// GroupNamespace must identify the environment and destination database.
	// Independent stores must never share a Kafka group namespace.
	GroupNamespace string
	// Dialer supplies deployment-approved TLS and SASL. No environment
	// credentials are implicitly loaded. Keep it immutable after construction.
	Dialer        *kafka.Dialer
	CommitTimeout time.Duration
	GapRetry      GapRetryPolicy
}

var groupNamespace = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$`)

func (cfg Config) Validate() error {
	rc, err := readerConfig(cfg)
	if err != nil {
		return err
	}
	return rc.Validate()
}

func readerConfig(cfg Config) (kafka.ReaderConfig, error) {
	if err := cfg.GapRetry.Validate(); err != nil {
		return kafka.ReaderConfig{}, err
	}
	if cfg.Target.Validate() != nil || !groupNamespace.MatchString(cfg.GroupNamespace) || len(cfg.Brokers) == 0 || cfg.CommitTimeout <= 0 || cfg.CommitTimeout > 30*time.Second {
		return kafka.ReaderConfig{}, errors.New("workflow consumer requires valid target, brokers and commit timeout in (0, 30s]")
	}
	for _, broker := range cfg.Brokers {
		if strings.TrimSpace(broker) == "" {
			return kafka.ReaderConfig{}, errors.New("workflow broker must not be empty")
		}
	}
	if cfg.Dialer == nil || cfg.Dialer.TLS == nil || cfg.Dialer.TLS.InsecureSkipVerify || cfg.Dialer.TLS.MinVersion < tls.VersionTLS12 {
		return kafka.ReaderConfig{}, errors.New("workflow consumer requires verified TLS with minimum TLS 1.2")
	}
	dialer := *cfg.Dialer
	dialer.TLS = cfg.Dialer.TLS.Clone()
	if dialer.Timeout <= 0 || dialer.Timeout > 5*time.Second {
		dialer.Timeout = 5 * time.Second
	}
	return kafka.ReaderConfig{
		Brokers: append([]string(nil), cfg.Brokers...), Topic: "order.events.v1",
		GroupID: "projection-workflow-" + cfg.GroupNamespace + "." + string(cfg.Target.Mode) + "." + cfg.Target.Generation,
		Dialer:  &dialer, CommitInterval: 0, StartOffset: kafka.FirstOffset,
		QueueCapacity: 1, MinBytes: 1, MaxBytes: 2 * lifecycle.MaxEventBytes, MaxWait: 500 * time.Millisecond,
		MaxAttempts: 3, ReadBackoffMin: time.Second, ReadBackoffMax: 5 * time.Second,
		WatchPartitionChanges: false,
	}, nil
}

type recordReader interface {
	FetchMessage(context.Context) (kafka.Message, error)
	CommitMessages(context.Context, ...kafka.Message) error
	Close() error
}
type recordProcessor interface {
	Process(context.Context, string, []byte, []byte) (workflow.Outcome, error)
}

type Consumer struct {
	reader                                                                                  recordReader
	processor                                                                               recordProcessor
	commitTimeout                                                                           time.Duration
	started                                                                                 atomic.Bool
	active                                                                                  atomic.Int64
	status                                                                                  atomic.Pointer[Status]
	gapRetry                                                                                GapRetryPolicy
	gapRetries, gapExhausted                                                                atomic.Uint64
	fetched, applied, duplicates, committed, fetchFailures, processFailures, commitFailures atomic.Uint64
}

// New owns the created reader and binds its group and processor to the same
// target. The store remains caller-owned; Run must finish before storage closes.
// Create a new Consumer after any stop, including cancellation.
func New(cfg Config, store workflow.Store) (*Consumer, error) {
	rc, err := readerConfig(cfg)
	if err != nil {
		return nil, err
	}
	p, err := workflow.New(cfg.Target, store)
	if err != nil {
		return nil, err
	}
	if err := rc.Validate(); err != nil {
		return nil, err
	}
	return &Consumer{reader: kafka.NewReader(rc), processor: p, commitTimeout: cfg.CommitTimeout, gapRetry: cfg.GapRetry}, nil
}

// Run is single-use. It processes and synchronously commits one fetched record
// before fetching another. All errors close the reader. Reusing a reader that
// already fetched a failed record could skip it on a later Run, so restarts
// must construct a fresh consumer with the same group and retained offsets.
func (c *Consumer) Run(ctx context.Context) (result error) {
	if c.reader == nil || c.processor == nil || c.commitTimeout <= 0 {
		return errors.New("workflow consumer is not configured")
	}
	if !c.started.CompareAndSwap(false, true) {
		return errors.New("workflow consumer is single-use")
	}
	c.active.Store(1)
	state := Status{Stage: "fetch"}
	defer func() {
		closeErr := c.reader.Close()
		if closeErr != nil && state.Failure == nil && ctx.Err() == nil {
			state.Failure = &Failure{Stage: "close", Reason: failureReason(closeErr)}
			c.publishStatus(state)
		}
		result = errors.Join(result, closeErr)
	}()
	defer func() {
		c.active.Store(0)
		if result != nil && ctx.Err() == nil {
			state.Failure = &Failure{Stage: state.Stage, Reason: failureReason(result)}
		}
		state.Stage = "stopped"
		c.publishStatus(state)
	}()
	for {
		state.Stage = "fetch"
		state.Current = nil
		c.publishStatus(state)
		if err := ctx.Err(); err != nil {
			return err
		}
		message, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			c.fetchFailures.Add(1)
			return fmt.Errorf("fetch workflow: %w", err)
		}
		c.fetched.Add(1)
		state.Stage = "process"
		if message.Topic == "order.events.v1" && message.Partition >= 0 && message.Offset >= 0 {
			state.Current = &Position{Topic: "order.events.v1", Partition: message.Partition, Offset: message.Offset}
		}
		c.publishStatus(state)
		if message.Partition < 0 || message.Offset < 0 {
			c.processFailures.Add(1)
			return fmt.Errorf("%w: invalid workflow source coordinates", workflow.ErrInvalid)
		}
		outcome, err := c.process(ctx, message, func(stage string) { state.Stage = stage; c.publishStatus(state) })
		if err != nil {
			c.processFailures.Add(1)
			return fmt.Errorf("process workflow: %w", err)
		}
		switch outcome {
		case workflow.Applied:
			c.applied.Add(1)
		case workflow.Duplicate:
			c.duplicates.Add(1)
		default:
			c.processFailures.Add(1)
			return errors.New("workflow processor returned no committed outcome")
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		commitCtx, cancel := context.WithTimeout(ctx, c.commitTimeout)
		state.Stage = "commit"
		c.publishStatus(state)
		err = c.reader.CommitMessages(commitCtx, message)
		cancel()
		if err != nil {
			c.commitFailures.Add(1)
			return fmt.Errorf("commit workflow offset: %w", err)
		}
		c.committed.Add(1)
		state.LastCommitted = state.Current
	}
}

// Close releases a consumer that has not been run. Once Run starts, cancel its
// context and wait for it to return; Run owns reader shutdown in that case.
func (c *Consumer) Close() error {
	if !c.started.CompareAndSwap(false, true) {
		return errors.New("workflow consumer has already started or closed")
	}
	if c.reader == nil {
		return nil
	}
	c.publishStatus(Status{Stage: "closed"})
	return c.reader.Close()
}

func (c *Consumer) Prometheus() string {
	status := c.Status()
	failure := ""
	failure += fmt.Sprintf("projection_workflow_gap_retries_total %d\nprojection_workflow_gap_exhausted_total %d\n", c.gapRetries.Load(), c.gapExhausted.Load())
	if status.Failure != nil {
		failure += fmt.Sprintf("projection_workflow_failure{stage=%q,reason=%q} 1\n", status.Failure.Stage, status.Failure.Reason)
	}
	return failure + fmt.Sprintf("projection_workflow_consumer_active %d\nprojection_workflow_fetched_total %d\nprojection_workflow_applied_total %d\nprojection_workflow_duplicates_total %d\nprojection_workflow_committed_total %d\nprojection_workflow_fetch_failures_total %d\nprojection_workflow_process_failures_total %d\nprojection_workflow_commit_failures_total %d\n", c.active.Load(), c.fetched.Load(), c.applied.Load(), c.duplicates.Load(), c.committed.Load(), c.fetchFailures.Load(), c.processFailures.Load(), c.commitFailures.Load())
}

func (c *Consumer) Running() bool { return c.active.Load() == 1 }
