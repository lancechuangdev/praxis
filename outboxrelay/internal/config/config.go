package config

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	DatabaseURL   string
	Brokers       []string
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
	c := Config{DatabaseURL: os.Getenv("OUTBOX_DATABASE_URL"), Brokers: strings.Split(value("OUTBOX_KAFKA_BROKERS", "localhost:9092"), ","), InstanceID: value("OUTBOX_INSTANCE_ID", host), HTTPAddress: value("OUTBOX_HTTP_ADDRESS", ":8082"), ClaimSize: integer("OUTBOX_CLAIM_SIZE", 500), LeaseDuration: duration("OUTBOX_LEASE_DURATION", 30*time.Second), PollInterval: duration("OUTBOX_POLL_INTERVAL", 100*time.Millisecond), BatchSize: integer("OUTBOX_KAFKA_BATCH_SIZE", 500), BatchBytes: int64(integer("OUTBOX_KAFKA_BATCH_BYTES", 512*1024)), BatchTimeout: duration("OUTBOX_KAFKA_BATCH_TIMEOUT", 5*time.Millisecond)}
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
