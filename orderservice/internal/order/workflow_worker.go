package order

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

// WorkflowDispatchSource must respect context cancellation and bound each call.
// A successful call with found=true includes the source acknowledgement commit.
type WorkflowDispatchSource interface {
	DispatchOnce(context.Context) (found bool, err error)
}

// WorkflowWorker is an opt-in runtime building block. It does not select a
// broker, activate publication, or provide producer fencing. Use one instance
// per process and reserve its database connections separately from admission.
type WorkflowWorker struct {
	source                                  WorkflowDispatchSource
	concurrency                             int
	pollInterval, errorBackoff              time.Duration
	running                                 atomic.Bool
	active                                  atomic.Int64
	attempts, acknowledged, failures, empty atomic.Uint64
}

func NewWorkflowWorker(source WorkflowDispatchSource, concurrency int, pollInterval, errorBackoff time.Duration) (*WorkflowWorker, error) {
	if source == nil || concurrency < 1 || concurrency > 64 || pollInterval <= 0 || errorBackoff <= 0 || pollInterval > time.Minute || errorBackoff > time.Minute {
		return nil, errors.New("workflow worker requires a source, 1..64 workers and poll/backoff durations in (0, 1m]")
	}
	return &WorkflowWorker{source: source, concurrency: concurrency, pollInterval: pollInterval, errorBackoff: errorBackoff}, nil
}

// Run blocks until cancellation and joins every in-flight dispatch before
// returning ctx.Err(). Callers must stop Run before closing the pool/publisher.
// Source failures are counted and retried with pacing; they never spawn extra
// goroutines. No financial identifiers or payloads become metric labels.
func (w *WorkflowWorker) Run(ctx context.Context) error {
	if !w.running.CompareAndSwap(false, true) {
		return errors.New("workflow worker is already running")
	}
	defer w.running.Store(false)
	var group sync.WaitGroup
	for range w.concurrency {
		group.Add(1)
		go func() {
			defer group.Done()
			// Spread process starts and empty/error polls across replicas. The
			// lower bound prevents repeated immediate retries during an outage.
			if !workflowWait(ctx, w.pollInterval) {
				return
			}
			for ctx.Err() == nil {
				w.active.Add(1)
				w.attempts.Add(1)
				found, err := w.source.DispatchOnce(ctx)
				w.active.Add(-1)
				if found && err == nil {
					w.acknowledged.Add(1)
				}
				if ctx.Err() != nil {
					return
				}
				switch {
				case err != nil:
					w.failures.Add(1)
					if !workflowWait(ctx, w.errorBackoff) {
						return
					}
				case !found:
					w.empty.Add(1)
					if !workflowWait(ctx, w.pollInterval) {
						return
					}
				}
			}
		}()
	}
	group.Wait()
	return ctx.Err()
}

func workflowWait(ctx context.Context, base time.Duration) bool {
	// Jitter in [base, 2*base); constructor bounds prevent overflow.
	timer := time.NewTimer(base + time.Duration(rand.Int64N(int64(base))))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return ctx.Err() == nil
	}
}

// Prometheus returns a concurrent-safe sample for a future runtime metrics
// handler. Acknowledged counts source commits, not unique consumer effects.
func (w *WorkflowWorker) Prometheus() string {
	return fmt.Sprintf("order_workflow_dispatch_active %d\norder_workflow_dispatch_attempts_total %d\norder_workflow_dispatch_acknowledged_total %d\norder_workflow_dispatch_failures_total %d\norder_workflow_dispatch_empty_total %d\n", w.active.Load(), w.attempts.Load(), w.acknowledged.Load(), w.failures.Load(), w.empty.Load())
}
