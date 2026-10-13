package dlq

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	eventsv1 "praxis/api/events/v1"
)

const (
	HeaderDLQTopic     = "praxis-dlq-topic"
	HeaderDLQPartition = "praxis-dlq-partition"
	HeaderDLQOffset    = "praxis-dlq-offset"
	HeaderReplayJob    = "praxis-replay-job-id"
	HeaderReplayBy     = "praxis-replay-operator"
	HeaderReplayAt     = "praxis-replay-at"
)

type Selection struct {
	FailureCode string
	EventID     string
	From, Until time.Time
}

func (s Selection) Match(d eventsv1.DLQEnvelope) bool {
	if s.FailureCode != "" && d.FailureCode != s.FailureCode {
		return false
	}
	if !s.From.IsZero() && d.FailedAt.Before(s.From) {
		return false
	}
	if !s.Until.IsZero() && d.FailedAt.After(s.Until) {
		return false
	}
	if s.EventID != "" {
		e, err := eventsv1.Decode(d.Original)
		if err != nil || e.ID != s.EventID {
			return false
		}
	}
	return true
}

func Authorize(operator string, allowed []string) error {
	operator = strings.TrimSpace(operator)
	if operator == "" {
		return errors.New("operator identity is required")
	}
	for _, candidate := range allowed {
		if operator == strings.TrimSpace(candidate) && operator != "" {
			return nil
		}
	}
	return fmt.Errorf("operator %q is not authorized", operator)
}

func ReplayMessage(d eventsv1.DLQEnvelope, dlqMessage kafka.Message, jobID, operator string, now time.Time) (kafka.Message, error) {
	if err := d.Validate(); err != nil {
		return kafka.Message{}, err
	}
	if strings.TrimSpace(jobID) == "" || strings.TrimSpace(operator) == "" {
		return kafka.Message{}, errors.New("replay job ID and operator are required")
	}
	headers := make([]kafka.Header, 0, len(d.SourceHeaders)+6)
	for _, h := range d.SourceHeaders {
		headers = append(headers, kafka.Header{Key: h.Key, Value: append([]byte(nil), h.Value...)})
	}
	headers = append(headers,
		kafka.Header{Key: HeaderDLQTopic, Value: []byte(dlqMessage.Topic)},
		kafka.Header{Key: HeaderDLQPartition, Value: []byte(fmt.Sprint(dlqMessage.Partition))},
		kafka.Header{Key: HeaderDLQOffset, Value: []byte(fmt.Sprint(dlqMessage.Offset))},
		kafka.Header{Key: HeaderReplayJob, Value: []byte(jobID)},
		kafka.Header{Key: HeaderReplayBy, Value: []byte(operator)},
		kafka.Header{Key: HeaderReplayAt, Value: []byte(now.UTC().Format(time.RFC3339Nano))},
	)
	return kafka.Message{Topic: d.SourceTopic, Key: append([]byte(nil), d.SourceKey...), Value: append([]byte(nil), d.Original...), Headers: headers, Time: now.UTC()}, nil
}
