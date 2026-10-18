package kafkaconsumer

import (
	"context"
	"errors"
	"strings"

	"github.com/segmentio/kafka-go"
	"praxis/projectionservice/internal/journal"
	"praxis/projectionservice/internal/workflow"
)

// NewJournal shares the fail-stop processing/commit barrier while binding a
// journal processor to its own topic, isolated group and transactional store.
func NewJournal(cfg Config, store journal.Store) (*Consumer, error) {
	if cfg.Stream != "" && cfg.Stream != "journal" {
		return nil, errors.New("journal store requires journal stream")
	}
	cfg.Stream = "journal"
	rc, err := readerConfig(cfg)
	if err != nil {
		return nil, err
	}
	if err := rc.Validate(); err != nil {
		return nil, err
	}
	p, err := journal.New(journal.Target{Generation: cfg.Target.Generation, Mode: journal.Mode(cfg.Target.Mode)}, store)
	if err != nil {
		return nil, err
	}
	return &Consumer{reader: kafka.NewReader(rc), processor: journalProcessor{p}, commitTimeout: cfg.CommitTimeout, gapRetry: cfg.GapRetry, topic: rc.Topic}, nil
}

type journalProcessor struct{ processor *journal.Processor }

func (p journalProcessor) Process(ctx context.Context, topic string, key, raw []byte) (workflow.Outcome, error) {
	out, err := p.processor.Process(ctx, topic, key, raw)
	// Preserve journal errors while exposing the shared loop's failure classes.
	for _, pair := range [][2]error{{journal.ErrGap, workflow.ErrGap}, {journal.ErrStale, workflow.ErrStale}, {journal.ErrConflict, workflow.ErrConflict}, {journal.ErrInvalid, workflow.ErrInvalid}} {
		if errors.Is(err, pair[0]) {
			err = errors.Join(err, pair[1])
			break
		}
	}
	return workflow.Outcome(out), err
}
func (c *Consumer) topicName() string {
	if c.topic == "" {
		return "order.events.v1"
	}
	return c.topic
}
func (c *Consumer) Prometheus() string {
	metrics := c.prometheus()
	if c.topicName() == "ledger.journals.v1" {
		return strings.ReplaceAll(metrics, "projection_workflow_", "projection_journal_")
	}
	return metrics
}
