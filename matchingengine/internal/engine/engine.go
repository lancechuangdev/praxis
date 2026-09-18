package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	matchingv1 "praxis/matchingengine/gen/matching/v1"
)

var tracer = otel.Tracer("praxis/matching-engine")

type Publisher interface {
	Publish(context.Context, Event) error
	Close() error
}
type Event struct {
	ID, Type, AggregateID, MessageKey string
	CorrelationID, CausationID        string
	TraceParent, TraceState           string
	OccurredAt                        time.Time
	Data                              any
}
type Metrics struct {
	Submitted, Accepted, Failed atomic.Uint64
	EngineNS, KafkaNS           atomic.Uint64
}
type partitionState struct {
	sync.Mutex
	sequence int64
	results  map[string]*matchingv1.SubmitOrderResponse
}

type Service struct {
	matchingv1.UnimplementedMatchingEngineServer
	Publisher  Publisher
	Latency    time.Duration
	Metrics    *Metrics
	partitions sync.Map
}

func (s *Service) SubmitOrder(ctx context.Context, req *matchingv1.SubmitOrderRequest) (*matchingv1.SubmitOrderResponse, error) {
	s.Metrics.Submitted.Add(1)
	if err := validate(req); err != nil {
		s.Metrics.Failed.Add(1)
		return nil, err
	}
	key := fmt.Sprintf("%s:%d", req.Symbol, req.EnginePartition)
	value, _ := s.partitions.LoadOrStore(key, &partitionState{results: map[string]*matchingv1.SubmitOrderResponse{}})
	state := value.(*partitionState)
	state.Lock()
	defer state.Unlock()
	if prior := state.results[req.RequestId]; prior != nil {
		return cloneResponse(prior), nil
	}

	engineStart := time.Now()
	if err := wait(ctx, s.Latency); err != nil {
		s.Metrics.Failed.Add(1)
		return nil, err
	}
	state.sequence++
	response := &matchingv1.SubmitOrderResponse{OrderId: req.OrderId, Status: "accepted", EngineSequence: state.sequence}
	s.Metrics.EngineNS.Add(uint64(time.Since(engineStart)))

	kafkaStart := time.Now()
	err := s.Publisher.Publish(ctx, Event{ID: req.RequestId, Type: "OrderAccepted", AggregateID: req.OrderId, MessageKey: key, CorrelationID: req.CorrelationId, CausationID: req.CausationId, TraceParent: req.TraceParent, TraceState: req.TraceState, OccurredAt: time.Now().UTC(), Data: map[string]any{"order": req, "engine_sequence": state.sequence}})
	s.Metrics.KafkaNS.Add(uint64(time.Since(kafkaStart)))
	if err != nil {
		state.sequence--
		s.Metrics.Failed.Add(1)
		return nil, err
	}
	state.results[req.RequestId] = response
	s.Metrics.Accepted.Add(1)
	return cloneResponse(response), nil
}

func cloneResponse(response *matchingv1.SubmitOrderResponse) *matchingv1.SubmitOrderResponse {
	return &matchingv1.SubmitOrderResponse{
		OrderId:        response.OrderId,
		Status:         response.Status,
		EngineSequence: response.EngineSequence,
	}
}

func validate(req *matchingv1.SubmitOrderRequest) error {
	if req == nil || strings.TrimSpace(req.RequestId) == "" || strings.TrimSpace(req.OrderId) == "" || strings.TrimSpace(req.UserId) == "" || strings.TrimSpace(req.Symbol) == "" || strings.TrimSpace(req.ReservationId) == "" {
		return errors.New("request, order, user, symbol, and reservation IDs are required")
	}
	if req.EnginePartition < 0 {
		return errors.New("engine partition cannot be negative")
	}
	return nil
}
func wait(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

type KafkaPublisher struct {
	writer  *kafka.Writer
	topic   string
	timeout time.Duration
}

func NewKafkaPublisher(brokers []string, topic string, size int, bytes int64, batchTimeout, timeout time.Duration) *KafkaPublisher {
	return &KafkaPublisher{topic: topic, timeout: timeout, writer: &kafka.Writer{Addr: kafka.TCP(brokers...), Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll, Async: false, BatchSize: size, BatchBytes: bytes, BatchTimeout: batchTimeout, Compression: kafka.Lz4, MaxAttempts: 5, WriteTimeout: timeout, ReadTimeout: timeout}}
}
func (p *KafkaPublisher) Publish(ctx context.Context, event Event) error {
	ctx, span := tracer.Start(ctx, "kafka.produce", trace.WithSpanKind(trace.SpanKindProducer), trace.WithAttributes(attribute.String("messaging.system", "kafka"), attribute.String("messaging.destination.name", p.topic), attribute.String("messaging.message.id", event.ID)))
	defer span.End()
	payload, err := json.Marshal(map[string]any{"id": event.ID, "type": event.Type, "aggregate_id": event.AggregateID, "correlation_id": event.CorrelationID, "causation_id": event.CausationID, "trace_parent": event.TraceParent, "trace_state": event.TraceState, "occurred_at": event.OccurredAt, "data": event.Data})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	callCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	headers := []kafka.Header{{Key: "event-id", Value: []byte(event.ID)}, {Key: "event-type", Value: []byte(event.Type)}}
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
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	headers = replaceHeader(headers, "traceparent", carrier.Get("traceparent"))
	headers = replaceHeader(headers, "tracestate", carrier.Get("tracestate"))
	err = p.writer.WriteMessages(callCtx, kafka.Message{Topic: p.topic, Key: []byte(event.MessageKey), Value: payload, Headers: headers})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return err
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
func (p *KafkaPublisher) Close() error { return p.writer.Close() }

type NoopPublisher struct{}

func (NoopPublisher) Publish(context.Context, Event) error { return nil }
func (NoopPublisher) Close() error                         { return nil }
