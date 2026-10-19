package kafkaconsumer

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/segmentio/kafka-go"

	"praxis/projectionservice/internal/workflow"
)

// GapRetryPolicy only rechecks the current source head. It never buffers later
// records, skips a version, bootstraps missing history or restarts the consumer.
type GapRetryPolicy struct {
	Retries int
	Delay   time.Duration
	Budget  time.Duration
}

func (p GapRetryPolicy) Validate() error {
	if p.Retries < 0 || p.Retries > 3 {
		return errors.New("workflow gap retries must be between 0 and 3")
	}
	if p.Retries > 0 && (p.Delay <= 0 || p.Delay > time.Second || p.Budget <= p.Delay || p.Budget > 30*time.Second) {
		return errors.New("workflow gap retries require delay in (0, 1s] and total processing budget in (delay, 30s]")
	}
	return nil
}

func (c *Consumer) process(ctx context.Context, message kafka.Message, stage func(string)) (workflow.Outcome, error) {
	policy := c.gapRetry
	if policy.Retries == 0 {
		return c.processRecord(ctx, message)
	}
	processing, stop := context.WithTimeout(ctx, policy.Budget)
	defer stop()
	observedGap := false
	exhaust := func(err error) (workflow.Outcome, error) {
		c.gapExhausted.Add(1)
		return "", errors.Join(workflow.ErrGap, err)
	}
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		if observedGap && processing.Err() != nil {
			return exhaust(processing.Err())
		}
		if attempt > 0 {
			c.gapRetries.Add(1)
		}
		stage("process")
		outcome, err := c.processRecord(processing, message)
		if err == nil {
			return outcome, nil
		}
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		if !errors.Is(err, workflow.ErrGap) {
			if observedGap && processing.Err() != nil {
				return exhaust(err)
			}
			return "", err
		}
		observedGap = true
		if attempt == policy.Retries || processing.Err() != nil {
			return exhaust(err)
		}
		stage("gap_wait")
		timer := time.NewTimer(policy.Delay + time.Duration(rand.Int64N(int64(policy.Delay))))
		select {
		case <-processing.Done():
			timer.Stop()
			if ctx.Err() != nil {
				return "", ctx.Err()
			}
			return exhaust(processing.Err())
		case <-timer.C:
		}
	}
}
