package eventsv1

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const Version = 1

type Envelope struct {
	Version       int             `json:"version"`
	ID            string          `json:"id"`
	Type          string          `json:"type"`
	AggregateID   string          `json:"aggregate_id"`
	CorrelationID string          `json:"correlation_id,omitempty"`
	CausationID   string          `json:"causation_id,omitempty"`
	TraceParent   string          `json:"trace_parent,omitempty"`
	TraceState    string          `json:"trace_state,omitempty"`
	OccurredAt    time.Time       `json:"occurred_at"`
	Data          json.RawMessage `json:"data"`
}

func New(id, eventType, aggregateID string, occurredAt time.Time, data any) (Envelope, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return Envelope{}, err
	}
	e := Envelope{Version: Version, ID: id, Type: eventType, AggregateID: aggregateID, OccurredAt: occurredAt.UTC(), Data: raw}
	return e, e.Validate()
}

func Decode(raw []byte) (Envelope, error) {
	var e Envelope
	if err := json.Unmarshal(raw, &e); err != nil {
		return Envelope{}, fmt.Errorf("decode event envelope: %w", err)
	}
	// Version zero is the repository's legacy, unversioned JSON envelope.
	if e.Version == 0 {
		e.Version = Version
	}
	if err := e.Validate(); err != nil {
		return Envelope{}, err
	}
	return e, nil
}

func (e Envelope) Validate() error {
	if e.Version != Version {
		return fmt.Errorf("unsupported event envelope version %d", e.Version)
	}
	if e.ID == "" || e.Type == "" || e.AggregateID == "" || e.OccurredAt.IsZero() || len(e.Data) == 0 {
		return errors.New("event envelope id, type, aggregate_id, occurred_at, and data are required")
	}
	if !json.Valid(e.Data) {
		return errors.New("event envelope data must be valid JSON")
	}
	return nil
}

type DLQEnvelope struct {
	Version         int         `json:"version"`
	ID              string      `json:"id"`
	SourceTopic     string      `json:"source_topic"`
	SourcePartition int         `json:"source_partition"`
	SourceOffset    int64       `json:"source_offset"`
	SourceTimestamp time.Time   `json:"source_timestamp,omitempty"`
	SourceKey       []byte      `json:"source_key,omitempty"`
	SourceHeaders   []DLQHeader `json:"source_headers,omitempty"`
	ConsumerGroup   string      `json:"consumer_group"`
	FailureClass    string      `json:"failure_class"`
	FailureCode     string      `json:"failure_code"`
	FailureReason   string      `json:"failure_reason"`
	FailedAt        time.Time   `json:"failed_at"`
	AttemptCount    int         `json:"attempt_count"`
	ReplayCount     int         `json:"replay_count"`
	Original        []byte      `json:"original"`
}

type DLQHeader struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

func (d DLQEnvelope) Validate() error {
	if d.Version != Version || d.ID == "" || d.SourceTopic == "" || d.SourcePartition < 0 || d.SourceOffset < 0 || d.ConsumerGroup == "" || d.FailureClass == "" || d.FailureCode == "" || d.FailureReason == "" || d.FailedAt.IsZero() || d.AttemptCount < 1 || len(d.Original) == 0 {
		return errors.New("invalid DLQ envelope")
	}
	if d.ReplayCount < 0 {
		return errors.New("invalid DLQ replay count")
	}
	return nil
}
