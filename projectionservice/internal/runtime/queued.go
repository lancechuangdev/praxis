package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"praxis/projectionservice/internal/kafkaconsumer"
	"praxis/projectionservice/internal/mongostore"
)

type ApplyRecord func(context.Context, mongostore.QueueRecord) error

// Queued owns no database resources. The source commits after durable enqueue;
// projection effects and quarantines remain recoverable independently of Kafka.
type Queued struct {
	Source          Consumer
	Queue           *mongostore.Queue
	Apply           ApplyRecord
	active          atomic.Bool
	applied, failed atomic.Uint64
}

func (q *Queued) Run(ctx context.Context) error {
	if q.Source == nil || q.Queue == nil || q.Apply == nil {
		return errors.New("queued runtime is not configured")
	}
	if !q.active.CompareAndSwap(false, true) {
		return errors.New("queued runtime is already running")
	}
	defer q.active.Store(false)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- q.Source.Run(ctx) }()
	applyDone := make(chan error, 1)
	go func() { applyDone <- q.Drain(ctx) }()
	var err error
	select {
	case err = <-done:
		cancel()
		<-applyDone
	case err = <-applyDone:
		cancel()
		<-done
	case <-ctx.Done():
		err = ctx.Err()
		cancel()
		<-done
		<-applyDone
	}
	return err
}
func (q *Queued) Drain(ctx context.Context) error {
	for ctx.Err() == nil {
		records, err := q.Queue.Batch(ctx)
		if err != nil {
			return err
		}
		for _, record := range records {
			err := q.Apply(ctx, record)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err == nil {
				q.applied.Add(1)
			} else {
				q.failed.Add(1)
			}
			if finishErr := q.Queue.Finish(ctx, record, err); finishErr != nil {
				return finishErr
			}
		}
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return ctx.Err()
}
func (q *Queued) Running() bool                { return q.active.Load() && q.Source.Running() }
func (q *Queued) Status() kafkaconsumer.Status { return q.Source.Status() }
func (q *Queued) Prometheus() string {
	metrics := q.Source.Prometheus()
	metrics = strings.ReplaceAll(metrics, "_applied_total ", "_enqueued_total ")
	return metrics + fmt.Sprintf("projection_queue_apply_success_total %d\nprojection_queue_apply_failures_total %d\n", q.applied.Load(), q.failed.Load())
}
