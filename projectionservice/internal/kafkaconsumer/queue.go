package kafkaconsumer

import (
	"context"
	"errors"

	"github.com/segmentio/kafka-go"

	"praxis/projectionservice/internal/workflow"
)

type RecordQueue interface {
	ProcessRecord(context.Context, kafka.Message) (workflow.Outcome, error)
	Scope() (workflow.Target, string)
}
type queueProcessor struct{ RecordQueue }

func (queueProcessor) Process(context.Context, string, []byte, []byte) (workflow.Outcome, error) {
	return "", errors.New("queued processing requires source coordinates")
}
func NewQueued(cfg Config, queue RecordQueue) (*Consumer, error) {
	if queue == nil {
		return nil, errors.New("recovery queue is required")
	}
	target, stream := queue.Scope()
	cfgStream := cfg.Stream
	if cfgStream == "" {
		cfgStream = "workflow"
	}
	if target != cfg.Target || stream != cfgStream {
		return nil, errors.New("recovery queue target or stream mismatch")
	}
	rc, err := readerConfig(cfg)
	if err != nil {
		return nil, err
	}
	if err := rc.Validate(); err != nil {
		return nil, err
	}
	return &Consumer{reader: kafka.NewReader(rc), processor: queueProcessor{queue}, commitTimeout: cfg.CommitTimeout, topic: rc.Topic}, nil
}
func (c *Consumer) processRecord(ctx context.Context, m kafka.Message) (workflow.Outcome, error) {
	if p, ok := c.processor.(interface {
		ProcessRecord(context.Context, kafka.Message) (workflow.Outcome, error)
	}); ok {
		return p.ProcessRecord(ctx, m)
	}
	return c.processor.Process(ctx, m.Topic, m.Key, m.Value)
}
