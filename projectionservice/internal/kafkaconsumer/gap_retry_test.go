package kafkaconsumer

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"praxis/projectionservice/internal/workflow"
)

func gapPolicy() GapRetryPolicy {
	return GapRetryPolicy{Retries: 2, Delay: time.Millisecond, Budget: time.Second}
}

func TestGapRecheckRecoversSameHeadBeforeAnySuccessorFetch(t *testing.T) {
	for _, recovered := range []workflow.Outcome{workflow.Applied, workflow.Duplicate} {
		t.Run(string(recovered), func(t *testing.T) {
			r := &fakeReader{messages: []kafka.Message{message(10)}}
			calls := 0
			var first time.Time
			c := consumer(r, func(ctx context.Context, topic string, key, raw []byte) (workflow.Outcome, error) {
				calls++
				if r.fetched != 1 || len(r.commits) != 0 || topic != "order.events.v1" || string(key) != "order-1" || string(raw) != "fact" {
					t.Error("gap retry advanced source or changed record")
				}
				if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > time.Second {
					t.Error("unbounded processing/retry window")
				}
				if calls == 1 {
					first = time.Now()
					return "", workflow.ErrGap
				}
				if time.Since(first) < time.Millisecond {
					t.Error("gap retry was not paced")
				}
				return recovered, nil
			})
			c.gapRetry = gapPolicy()
			if err := c.Run(context.Background()); !errors.Is(err, io.EOF) {
				t.Fatal(err)
			}
			if calls != 2 || len(r.commits) != 1 || r.commits[0].Offset != 10 || c.gapRetries.Load() != 1 || c.gapExhausted.Load() != 0 {
				t.Fatal("gap recovery counters or commit barrier failed")
			}
		})
	}
}

func TestGapExhaustionPreservesFailedHead(t *testing.T) {
	r := &fakeReader{messages: []kafka.Message{message(10), message(11)}}
	calls := 0
	c := consumer(r, func(context.Context, string, []byte, []byte) (workflow.Outcome, error) {
		calls++
		return "", workflow.ErrGap
	})
	c.gapRetry = gapPolicy()
	if err := c.Run(context.Background()); !errors.Is(err, workflow.ErrGap) {
		t.Fatal(err)
	}
	if calls != 3 || c.gapRetries.Load() != 2 || c.gapExhausted.Load() != 1 || r.fetched != 1 || len(r.commits) != 0 || r.closed != 1 {
		t.Fatal("gap exhaustion skipped a head or exceeded retry bound")
	}
	if c.Status().Current.Offset != 10 || c.Status().Failure.Reason != "gap" {
		t.Fatal(c.Status())
	}
	if !strings.Contains(c.Prometheus(), "projection_workflow_gap_exhausted_total 1\n") {
		t.Fatal(c.Prometheus())
	}
}

func TestOnlyGapsAreRetried(t *testing.T) {
	for _, failure := range []error{workflow.ErrInvalid, workflow.ErrStale, workflow.ErrConflict, errors.New("database unavailable")} {
		r := &fakeReader{messages: []kafka.Message{message(10)}}
		calls := 0
		c := consumer(r, func(context.Context, string, []byte, []byte) (workflow.Outcome, error) { calls++; return "", failure })
		c.gapRetry = gapPolicy()
		if err := c.Run(context.Background()); !errors.Is(err, failure) {
			t.Fatal(err)
		}
		if calls != 1 || c.gapRetries.Load() != 0 || c.gapExhausted.Load() != 0 || len(r.commits) != 0 {
			t.Fatal("non-gap failure retried or committed")
		}
	}
}

func TestGapWaitCancellationAndSharedDeadline(t *testing.T) {
	for _, cancelParent := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		r := &fakeReader{messages: []kafka.Message{message(10)}}
		c := consumer(r, func(context.Context, string, []byte, []byte) (workflow.Outcome, error) {
			if cancelParent {
				cancel()
			}
			return "", workflow.ErrGap
		})
		c.gapRetry = GapRetryPolicy{Retries: 3, Delay: 20 * time.Millisecond, Budget: 21 * time.Millisecond}
		err := c.Run(ctx)
		cancel()
		if len(r.commits) != 0 || r.fetched != 1 {
			t.Fatal("cancelled gap was committed or skipped")
		}
		if cancelParent {
			if !errors.Is(err, context.Canceled) || c.Status().Failure != nil {
				t.Fatal("shutdown produced failure diagnostics")
			}
		} else if !errors.Is(err, workflow.ErrGap) || c.gapExhausted.Load() != 1 {
			t.Fatal("budget failed to preserve gap diagnosis")
		}
	}
}

func TestGapPolicyValidation(t *testing.T) {
	if err := (GapRetryPolicy{}).Validate(); err != nil {
		t.Fatal(err)
	}
	for _, p := range []GapRetryPolicy{
		{Retries: -1}, {Retries: 4}, {Retries: 1},
		{Retries: 1, Delay: time.Second + 1, Budget: 5 * time.Second},
		{Retries: 1, Delay: time.Second, Budget: time.Second},
		{Retries: 1, Delay: time.Millisecond, Budget: 31 * time.Second},
	} {
		if err := p.Validate(); err == nil {
			t.Fatal("unbounded gap policy accepted")
		}
	}
}

func TestCancellationInterruptsGapWaitWithoutExhaustion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := &fakeReader{messages: []kafka.Message{message(10)}}
	c := consumer(r, func(context.Context, string, []byte, []byte) (workflow.Outcome, error) { return "", workflow.ErrGap })
	c.gapRetry = GapRetryPolicy{Retries: 2, Delay: time.Second, Budget: 3 * time.Second}
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	deadline := time.Now().Add(time.Second)
	for c.Status().Stage != "gap_wait" && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if c.Status().Stage != "gap_wait" {
		t.Fatal("consumer did not enter bounded gap wait")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("gap wait ignored cancellation")
	}
	if c.gapRetries.Load() != 0 || c.gapExhausted.Load() != 0 || c.Status().Failure != nil || len(r.commits) != 0 {
		t.Fatal("shutdown retried, committed or exhausted gap")
	}
}
