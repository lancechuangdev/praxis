package order_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"praxis/orderservice/internal/order"
)

type workflowDispatchFunc func(context.Context) (bool, error)

func (f workflowDispatchFunc) DispatchOnce(ctx context.Context) (bool, error) { return f(ctx) }

func TestWorkflowWorkerConfiguration(t *testing.T) {
	source := workflowDispatchFunc(func(context.Context) (bool, error) { t.Fatal("unexpected dispatch"); return false, nil })
	for _, concurrency := range []int{-1, 0, 65} {
		if _, err := order.NewWorkflowWorker(source, concurrency, time.Second, time.Second); err == nil {
			t.Fatalf("accepted concurrency %d", concurrency)
		}
	}
	for _, duration := range []time.Duration{-1, 0, time.Minute + 1} {
		if _, err := order.NewWorkflowWorker(source, 1, duration, time.Second); err == nil {
			t.Fatalf("accepted poll %s", duration)
		}
		if _, err := order.NewWorkflowWorker(source, 1, time.Second, duration); err == nil {
			t.Fatalf("accepted backoff %s", duration)
		}
	}
	if _, err := order.NewWorkflowWorker(nil, 1, time.Second, time.Second); err == nil {
		t.Fatal("accepted nil source")
	}
}

func TestWorkflowWorkerBoundsConcurrencyAndJoinsOnShutdown(t *testing.T) {
	entered := make(chan struct{}, 8)
	release := make(chan struct{})
	var active atomic.Int64
	source := workflowDispatchFunc(func(ctx context.Context) (bool, error) {
		active.Add(1)
		defer active.Add(-1)
		entered <- struct{}{}
		<-ctx.Done()
		<-release // Model cleanup after cancellation; Run must join it.
		return true, ctx.Err()
	})
	worker, err := order.NewWorkflowWorker(source, 3, time.Millisecond, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	for range 3 {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("workers did not start")
		}
	}
	if active.Load() != 3 {
		t.Fatalf("active=%d", active.Load())
	}
	if err := worker.Run(ctx); err == nil {
		t.Fatal("allowed duplicate Run")
	}
	if !strings.Contains(worker.Prometheus(), "order_workflow_dispatch_active 3\n") {
		t.Fatal(worker.Prometheus())
	}
	cancel()
	select {
	case <-done:
		t.Fatal("returned before dispatch cleanup")
	default:
	}
	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown blocked")
	}
	if active.Load() != 0 || !strings.Contains(worker.Prometheus(), "order_workflow_dispatch_active 0\n") {
		t.Fatal("dispatch leaked")
	}
}

func TestWorkflowWorkerPacesErrorsAndEmptyPolls(t *testing.T) {
	for _, failure := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty", true: "failure"}[failure], func(t *testing.T) {
			calls := make(chan time.Time, 8)
			source := workflowDispatchFunc(func(context.Context) (bool, error) {
				calls <- time.Now()
				if failure {
					return true, errors.New("database unavailable")
				}
				return false, nil
			})
			const delay = 10 * time.Millisecond
			worker, err := order.NewWorkflowWorker(source, 1, delay, delay)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- worker.Run(ctx) }()
			var previous time.Time
			for range 3 {
				select {
				case now := <-calls:
					if !previous.IsZero() && now.Sub(previous) < delay {
						t.Fatal("unpaced retry")
					}
					previous = now
				case <-time.After(time.Second):
					t.Fatal("dispatch stalled")
				}
			}
			cancel()
			<-done
			metric := "order_workflow_dispatch_empty_total"
			if failure {
				metric = "order_workflow_dispatch_failures_total"
			}
			// Cancellation can race the third result; the first two are complete.
			if !strings.Contains(worker.Prometheus(), metric+" 2\n") && !strings.Contains(worker.Prometheus(), metric+" 3\n") {
				t.Fatal(worker.Prometheus())
			}
		})
	}
}

func TestWorkflowWorkerCancelledBeforeStartDoesNotDispatch(t *testing.T) {
	worker, err := order.NewWorkflowWorker(workflowDispatchFunc(func(context.Context) (bool, error) { t.Fatal("dispatch after shutdown"); return false, nil }), 1, time.Minute, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := worker.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestWorkflowWorkerCountsCommitDuringShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker, err := order.NewWorkflowWorker(workflowDispatchFunc(func(context.Context) (bool, error) {
		// Source committed just as shutdown arrived. It is still acknowledged.
		cancel()
		return true, nil
	}), 1, time.Millisecond, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	for _, metric := range []string{
		"order_workflow_dispatch_attempts_total 1\n",
		"order_workflow_dispatch_acknowledged_total 1\n",
		"order_workflow_dispatch_failures_total 0\n",
	} {
		if !strings.Contains(worker.Prometheus(), metric) {
			t.Fatal(worker.Prometheus())
		}
	}
}
