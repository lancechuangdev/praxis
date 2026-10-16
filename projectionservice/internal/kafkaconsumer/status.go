package kafkaconsumer

import (
	"context"
	"errors"
	"io"

	"github.com/segmentio/kafka-go"
	"praxis/projectionservice/internal/workflow"
)

type Position struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
}
type Failure struct {
	Stage  string `json:"stage"`
	Reason string `json:"reason"`
}

// Status is bounded process-local diagnostics, never a durable checkpoint or a
// complete partition inventory. LastCommitted means this process observed the
// acknowledgement, not that Kafka failed to commit any later ambiguous request.
type Status struct {
	Stage         string    `json:"stage"`
	Current       *Position `json:"current,omitempty"`
	LastCommitted *Position `json:"last_committed,omitempty"`
	Failure       *Failure  `json:"failure,omitempty"`
}

func (c *Consumer) Status() Status {
	if value := c.status.Load(); value != nil {
		return cloneStatus(*value)
	}
	return Status{Stage: "not_started"}
}

func cloneStatus(value Status) Status {
	if value.Current != nil {
		copy := *value.Current
		value.Current = &copy
	}
	if value.LastCommitted != nil {
		copy := *value.LastCommitted
		value.LastCommitted = &copy
	}
	if value.Failure != nil {
		copy := *value.Failure
		value.Failure = &copy
	}
	return value
}
func (c *Consumer) publishStatus(value Status) { copy := cloneStatus(value); c.status.Store(&copy) }

func failureReason(err error) string {
	switch {
	case errors.Is(err, workflow.ErrGap):
		return "gap"
	case errors.Is(err, workflow.ErrStale):
		return "stale"
	case errors.Is(err, workflow.ErrConflict):
		return "conflict"
	case errors.Is(err, workflow.ErrInvalid):
		return "invalid"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case errors.Is(err, kafka.SASLAuthenticationFailed):
		return "authentication"
	case errors.Is(err, kafka.TopicAuthorizationFailed), errors.Is(err, kafka.GroupAuthorizationFailed):
		return "authorization"
	case errors.Is(err, io.EOF):
		return "closed"
	default:
		return "error"
	}
}
