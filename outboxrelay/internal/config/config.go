package config

import (
	"errors"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"praxis/outboxrelay/internal/kafkaauth"
)

type Config struct {
	DatabaseURL   string
	Brokers       []string
	KafkaAuth     kafkaauth.Config
	TopicMap      map[string]string
	InstanceID    string
	HTTPAddress   string
	ClaimSize     int
	LeaseDuration time.Duration
	PollInterval  time.Duration
	BatchSize     int
	BatchBytes    int64
	BatchTimeout  time.Duration
}

func Load() (Config, error) {
	host, _ := os.Hostname()
	kafkaAuth, err := kafkaauth.Load()
	if err != nil {
		return Config{}, err
	}
	topicMap, err := parseTopicMap(os.Getenv("OUTBOX_KAFKA_TOPIC_MAP"))
	if err != nil {
		return Config{}, err
	}
	c := Config{DatabaseURL: os.Getenv("OUTBOX_DATABASE_URL"), Brokers: strings.Split(value("OUTBOX_KAFKA_BROKERS", "localhost:9092"), ","), KafkaAuth: kafkaAuth, TopicMap: topicMap, InstanceID: value("OUTBOX_INSTANCE_ID", host), HTTPAddress: value("OUTBOX_HTTP_ADDRESS", ":8082"), ClaimSize: integer("OUTBOX_CLAIM_SIZE", 500), LeaseDuration: duration("OUTBOX_LEASE_DURATION", 30*time.Second), PollInterval: duration("OUTBOX_POLL_INTERVAL", 100*time.Millisecond), BatchSize: integer("OUTBOX_KAFKA_BATCH_SIZE", 500), BatchBytes: int64(integer("OUTBOX_KAFKA_BATCH_BYTES", 512*1024)), BatchTimeout: duration("OUTBOX_KAFKA_BATCH_TIMEOUT", 5*time.Millisecond)}
	if c.DatabaseURL == "" {
		return c, errors.New("OUTBOX_DATABASE_URL is required")
	}
	if c.InstanceID == "" {
		return c, errors.New("OUTBOX_INSTANCE_ID is required")
	}
	if c.ClaimSize <= 0 || c.BatchSize <= 0 || c.BatchBytes <= 0 {
		return c, errors.New("batch and claim sizes must be positive")
	}
	return c, nil
}

var topicName = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

func parseTopicMap(raw string) (map[string]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	result := make(map[string]string)
	for _, entry := range strings.Split(raw, ",") {
		parts := strings.Split(strings.TrimSpace(entry), "=")
		if len(parts) != 2 {
			return nil, errors.New("OUTBOX_KAFKA_TOPIC_MAP must contain comma-separated source=destination pairs")
		}
		source, destination := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
		if !topicName.MatchString(source) || !topicName.MatchString(destination) {
			return nil, errors.New("OUTBOX_KAFKA_TOPIC_MAP contains an invalid topic name")
		}
		if _, exists := result[source]; exists {
			return nil, errors.New("OUTBOX_KAFKA_TOPIC_MAP contains a duplicate source topic")
		}
		result[source] = destination
	}
	return result, nil
}
func value(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func integer(k string, d int) int {
	if v := os.Getenv(k); v != "" {
		n, e := strconv.Atoi(v)
		if e == nil {
			return n
		}
	}
	return d
}
func duration(k string, d time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		x, e := time.ParseDuration(v)
		if e == nil {
			return x
		}
	}
	return d
}
