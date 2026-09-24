package config

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"praxis/matchingengine/internal/kafkaauth"
)

type Config struct {
	GRPCAddress, HTTPAddress string
	EngineLatency            time.Duration
	DatabaseURL              string
	DBWriterMaxConns         int32
	EventsTopic              string
	KafkaBrokers             []string
	KafkaAuth                kafkaauth.Config
	MigrateOnStartup         bool
}

func Load() (Config, error) {
	auth, err := kafkaauth.Load()
	if err != nil {
		return Config{}, err
	}
	dbURL, err := databaseURL()
	if err != nil {
		return Config{}, err
	}
	max, err := positiveInt32("MATCHING_DB_WRITER_MAX_CONNS", 32)
	if err != nil {
		return Config{}, err
	}
	migrate, err := strconv.ParseBool(value("MATCHING_MIGRATE_ON_STARTUP", "true"))
	if err != nil {
		return Config{}, fmt.Errorf("MATCHING_MIGRATE_ON_STARTUP must be true or false: %w", err)
	}
	return Config{GRPCAddress: value("MATCHING_GRPC_ADDRESS", ":9092"), HTTPAddress: value("MATCHING_HTTP_ADDRESS", ":8084"), EngineLatency: duration("MATCHING_ENGINE_LATENCY", time.Millisecond), DatabaseURL: dbURL, DBWriterMaxConns: max, EventsTopic: value("MATCHING_KAFKA_TOPIC", "matching.events.v1"), KafkaBrokers: strings.Split(value("MATCHING_KAFKA_BROKERS", "localhost:9092"), ","), KafkaAuth: auth, MigrateOnStartup: migrate}, nil
}
func databaseURL() (string, error) {
	if raw := strings.TrimSpace(os.Getenv("MATCHING_DATABASE_WRITER_URL")); raw != "" {
		return raw, nil
	}
	host, user, name := strings.TrimSpace(os.Getenv("MATCHING_DB_WRITER_HOST")), strings.TrimSpace(os.Getenv("MATCHING_DB_USER")), strings.TrimSpace(os.Getenv("MATCHING_DB_NAME"))
	if host == "" || user == "" || name == "" {
		return "", fmt.Errorf("set MATCHING_DATABASE_WRITER_URL or MATCHING_DB_WRITER_HOST, MATCHING_DB_USER, and MATCHING_DB_NAME")
	}
	if strings.ContainsAny(host, ":/") {
		return "", fmt.Errorf("MATCHING_DB_WRITER_HOST must be a hostname without a port")
	}
	password, err := databasePassword()
	if err != nil {
		return "", err
	}
	u := url.URL{Scheme: "postgres", User: url.UserPassword(user, password), Host: net.JoinHostPort(host, "5432"), Path: "/" + name}
	u.RawQuery = "sslmode=require"
	return u.String(), nil
}
func positiveInt32(name string, fallback int32) (int32, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	v, err := strconv.ParseInt(raw, 10, 32)
	if err != nil || v < 1 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return int32(v), nil
}
func value(k, d string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return d
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
