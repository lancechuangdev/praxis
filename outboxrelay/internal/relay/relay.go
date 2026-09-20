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
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

var tracer = otel.Tracer("praxis/outbox-relay")

type Event struct {
	SequenceNumber int64
	ID             string
	Topic          string
	EventType      string
	MessageKey     string
	CorrelationID  string
	CausationID    string
	TraceParent    string
	TraceState     string
	Payload        []byte
}

type Failure struct {
	ID      string
	Message string
}

type Store interface {
	Claim(context.Context, string, int, time.Duration) ([]Event, error)
	MarkPublished(context.Context, []string, string) error
	MarkFailed(context.Context, []Failure, string) error
}
type Publisher interface {
	WriteMessages(context.Context, ...kafka.Message) error
	Close() error
}

type Metrics struct{ Claimed, Published, Failed atomic.Uint64 }
type Relay struct {
	Store                       Store
	Publisher                   Publisher
	TopicMap                    map[string]string
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
	spans := make([]trace.Span, len(events))
	for i, event := range events {
		topic := event.Topic
		if mapped, ok := r.TopicMap[topic]; ok {
			topic = mapped
		}
		carrier := propagation.MapCarrier{"traceparent": event.TraceParent, "tracestate": event.TraceState}
		eventCtx := otel.GetTextMapPropagator().Extract(ctx, carrier)
		eventCtx, spans[i] = tracer.Start(eventCtx, "kafka.produce", trace.WithSpanKind(trace.SpanKindProducer), trace.WithAttributes(attribute.String("messaging.system", "kafka"), attribute.String("messaging.destination.name", topic), attribute.String("messaging.message.id", event.ID)))
		headers := []kafka.Header{{Key: "event-id", Value: []byte(event.ID)}, {Key: "event-type", Value: []byte(event.EventType)}}
		if event.CorrelationID != "" {
			headers = append(headers, kafka.Header{Key: "correlation-id", Value: []byte(event.CorrelationID)})
		}
		if event.CausationID != "" {
			headers = append(headers, kafka.Header{Key: "causation-id", Value: []byte(event.CausationID)})
		}
		if event.TraceParent != "" {
			headers = append(headers, kafka.Header{Key: "traceparent", Value: []byte(event.TraceParent)})
		}
		if event.TraceState != "" {
			headers = append(headers, kafka.Header{Key: "tracestate", Value: []byte(event.TraceState)})
		}
		traceCarrier := propagation.MapCarrier{}
		otel.GetTextMapPropagator().Inject(eventCtx, traceCarrier)
		headers = replaceHeader(headers, "traceparent", traceCarrier.Get("traceparent"))
		headers = replaceHeader(headers, "tracestate", traceCarrier.Get("tracestate"))
		messages[i] = kafka.Message{Topic: topic, Key: []byte(event.MessageKey), Value: event.Payload, Headers: headers}
	}
	err = r.Publisher.WriteMessages(ctx, messages...)
	for _, span := range spans {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
	if err == nil {
		ids := make([]string, len(events))
		for i := range events {
			ids[i] = events[i].ID
		}
		if markErr := r.Store.MarkPublished(ctx, ids, r.InstanceID); markErr != nil {
			return len(events), markErr
		}
		r.Metrics.Published.Add(uint64(len(events)))
		return len(events), nil
	}
	var writes kafka.WriteErrors
	if errors.As(err, &writes) && len(writes) == len(events) {
		published := make([]string, 0, len(events))
		failed := make([]Failure, 0, len(events))
		for i, event := range events {
			if writes[i] == nil {
				published = append(published, event.ID)
			} else {
				failed = append(failed, Failure{ID: event.ID, Message: writes[i].Error()})
			}
		}
		var joined error
		if markErr := r.Store.MarkPublished(ctx, published, r.InstanceID); markErr != nil {
			joined = errors.Join(joined, markErr)
		} else {
			r.Metrics.Published.Add(uint64(len(published)))
		}
		if markErr := r.Store.MarkFailed(ctx, failed, r.InstanceID); markErr != nil {
			joined = errors.Join(joined, markErr)
		} else {
			r.Metrics.Failed.Add(uint64(len(failed)))
		}
		return len(events), errors.Join(err, joined)
	}
	failed := make([]Failure, 0, len(events))
	for _, event := range events {
		failed = append(failed, Failure{ID: event.ID, Message: err.Error()})
	}
	if markErr := r.Store.MarkFailed(ctx, failed, r.InstanceID); markErr != nil {
		err = errors.Join(err, markErr)
	} else {
		r.Metrics.Failed.Add(uint64(len(failed)))
	}
	return len(events), fmt.Errorf("publish batch: %w", err)
}

func replaceHeader(headers []kafka.Header, key, value string) []kafka.Header {
	if value == "" {
		return headers
	}
	for i := range headers {
		if headers[i].Key == key {
			headers[i].Value = []byte(value)
			return headers
		}
	}
	return append(headers, kafka.Header{Key: key, Value: []byte(value)})
}
