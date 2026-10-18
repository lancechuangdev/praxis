package store

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

// JournalDispatchSource must respect context cancellation and bound each call.
// A successful call with found=true includes the source acknowledgement commit.
type JournalDispatchSource interface {
	DispatchOnce(context.Context) (found bool, err error)
}

// JournalWorker is an opt-in runtime building block. It does not select a
// broker, activate publication, or provide producer fencing. Use one instance
// per process and reserve its database connections separately from admission.
type JournalWorker struct {
	source                                  JournalDispatchSource
	concurrency                             int
	pollInterval, errorBackoff              time.Duration
	running                                 atomic.Bool
	active                                  atomic.Int64
	attempts, acknowledged, failures, empty atomic.Uint64
}

func NewJournalWorker(source JournalDispatchSource, concurrency int, pollInterval, errorBackoff time.Duration) (*JournalWorker, error) {
	if source == nil || concurrency < 1 || concurrency > 64 || pollInterval <= 0 || errorBackoff <= 0 || pollInterval > time.Minute || errorBackoff > time.Minute {
		return nil, errors.New("journal worker requires a source, 1..64 workers and poll/backoff durations in (0, 1m]")
	}
	return &JournalWorker{source: source, concurrency: concurrency, pollInterval: pollInterval, errorBackoff: errorBackoff}, nil
}

// Run blocks until cancellation and joins every in-flight dispatch before
// returning ctx.Err(). Callers must stop Run before closing the pool/publisher.
// Source failures are counted and retried with pacing; they never spawn extra
// goroutines. No financial identifiers or payloads become metric labels.
func (w *JournalWorker) Run(ctx context.Context) error {
	if !w.running.CompareAndSwap(false, true) {
		return errors.New("journal worker is already running")
	}
	defer w.running.Store(false)
	var group sync.WaitGroup
	for range w.concurrency {
		group.Add(1)
		go func() {
			defer group.Done()
			// Spread process starts and empty/error polls across replicas. The
			// lower bound prevents repeated immediate retries during an outage.
			if !journalWait(ctx, w.pollInterval) {
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
					if !journalWait(ctx, w.errorBackoff) {
						return
					}
				case !found:
					w.empty.Add(1)
					if !journalWait(ctx, w.pollInterval) {
						return
					}
				}
			}
		}()
	}
	group.Wait()
	return ctx.Err()
}

func journalWait(ctx context.Context, base time.Duration) bool {
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
func (w *JournalWorker) Prometheus() string {
	return fmt.Sprintf("ledger_journal_dispatch_active %d\nledger_journal_dispatch_attempts_total %d\nledger_journal_dispatch_acknowledged_total %d\nledger_journal_dispatch_failures_total %d\nledger_journal_dispatch_empty_total %d\n", w.active.Load(), w.attempts.Load(), w.acknowledged.Load(), w.failures.Load(), w.empty.Load())
}
