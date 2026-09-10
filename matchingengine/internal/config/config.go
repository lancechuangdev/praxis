package config

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	GRPCAddress, HTTPAddress string
	EngineLatency            time.Duration
	KafkaEnabled             bool
	KafkaBrokers             []string
	EventsTopic              string
	KafkaTimeout             time.Duration
	KafkaBatchSize           int
	KafkaBatchBytes          int64
	KafkaBatchTimeout        time.Duration
}

func Load() (Config, error) {
	c := Config{
		GRPCAddress: value("MATCHING_GRPC_ADDRESS", ":9092"), HTTPAddress: value("MATCHING_HTTP_ADDRESS", ":8084"),
		EngineLatency: duration("MATCHING_ENGINE_LATENCY", time.Millisecond), KafkaEnabled: boolean("MATCHING_KAFKA_ENABLED", true),
		KafkaBrokers: split(value("MATCHING_KAFKA_BROKERS", "localhost:9092")), EventsTopic: value("MATCHING_KAFKA_TOPIC", "matching.events.v1"),
		KafkaTimeout: duration("MATCHING_KAFKA_TIMEOUT", 2*time.Second), KafkaBatchSize: integer("MATCHING_KAFKA_BATCH_SIZE", 500),
		KafkaBatchBytes: int64(integer("MATCHING_KAFKA_BATCH_BYTES", 512*1024)), KafkaBatchTimeout: duration("MATCHING_KAFKA_BATCH_TIMEOUT", 2*time.Millisecond),
	}
	if c.KafkaBatchSize <= 0 || c.KafkaBatchBytes <= 0 {
		return c, errors.New("Kafka batch limits must be positive")
	}
	return c, nil
}

func value(k, d string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return d
}
func split(v string) []string {
	p := strings.Split(v, ",")
	for i := range p {
		p[i] = strings.TrimSpace(p[i])
	}
	return p
}
func duration(k string, d time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return d
	}
	x, e := time.ParseDuration(v)
	if e != nil {
		return d
	}
	return x
}
func integer(k string, d int) int {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return d
	}
	x, e := strconv.Atoi(v)
	if e != nil {
		return d
	}
	return x
}
func boolean(k string, d bool) bool {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return d
	}
	x, e := strconv.ParseBool(v)
	if e != nil {
		return d
	}
	return x
}
