package relay

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

type Event struct {
	SequenceNumber int64
	ID             string
	Topic          string
	EventType      string
	MessageKey     string
	Payload        []byte
}

type Store interface {
	Claim(context.Context, string, int, time.Duration) ([]Event, error)
	MarkPublished(context.Context, string, string) error
	MarkFailed(context.Context, string, string, string) error
}
type Publisher interface {
	WriteMessages(context.Context, ...kafka.Message) error
	Close() error
}

type Metrics struct{ Claimed, Published, Failed atomic.Uint64 }
type Relay struct {
	Store                       Store
	Publisher                   Publisher
	InstanceID                  string
	ClaimSize                   int
	LeaseDuration, PollInterval time.Duration
	Log                         *slog.Logger
	Metrics                     *Metrics
}

func (r *Relay) Run(ctx context.Context) error {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
		n, err := r.RunOnce(ctx)
		if err != nil {
			r.Log.ErrorContext(ctx, "relay batch failed", "error", err)
		}
		delay := r.PollInterval
		if n == r.ClaimSize {
			delay = 0
		}
		timer.Reset(delay)
	}
}

func (r *Relay) RunOnce(ctx context.Context) (int, error) {
	events, err := r.Store.Claim(ctx, r.InstanceID, r.ClaimSize, r.LeaseDuration)
	if err != nil {
		return 0, err
	}
	if len(events) == 0 {
		return 0, nil
	}
	r.Metrics.Claimed.Add(uint64(len(events)))
	sort.Slice(events, func(i, j int) bool { return events[i].SequenceNumber < events[j].SequenceNumber })
	messages := make([]kafka.Message, len(events))
	for i, event := range events {
		messages[i] = kafka.Message{Topic: event.Topic, Key: []byte(event.MessageKey), Value: event.Payload, Headers: []kafka.Header{{Key: "event-id", Value: []byte(event.ID)}, {Key: "event-type", Value: []byte(event.EventType)}}}
	}
	err = r.Publisher.WriteMessages(ctx, messages...)
	if err == nil {
		for _, event := range events {
			if markErr := r.Store.MarkPublished(ctx, event.ID, r.InstanceID); markErr != nil {
				return len(events), markErr
			}
			r.Metrics.Published.Add(1)
		}
		return len(events), nil
	}
	var writes kafka.WriteErrors
	if errors.As(err, &writes) && len(writes) == len(events) {
		var joined error
		for i, event := range events {
			if writes[i] == nil {
				if markErr := r.Store.MarkPublished(ctx, event.ID, r.InstanceID); markErr != nil {
					joined = errors.Join(joined, markErr)
				} else {
					r.Metrics.Published.Add(1)
				}
			} else {
				if markErr := r.Store.MarkFailed(ctx, event.ID, r.InstanceID, writes[i].Error()); markErr != nil {
					joined = errors.Join(joined, markErr)
				}
				r.Metrics.Failed.Add(1)
			}
		}
		return len(events), errors.Join(err, joined)
	}
	for _, event := range events {
		if markErr := r.Store.MarkFailed(ctx, event.ID, r.InstanceID, err.Error()); markErr != nil {
			err = errors.Join(err, markErr)
		}
		r.Metrics.Failed.Add(1)
	}
	return len(events), fmt.Errorf("publish batch: %w", err)
}
