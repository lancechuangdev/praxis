package config

import (
	"errors"
	"os"
	"strings"
)

type Config struct {
	DatabaseURL, HTTPAddress, GRPCAddress string
	KafkaBrokers                          []string
	CommandsTopic, ConsumerGroup          string
}

func Load() (Config, error) {
	c := Config{DatabaseURL: os.Getenv("LEDGER_DATABASE_URL"), HTTPAddress: value("LEDGER_HTTP_ADDRESS", ":8081"), GRPCAddress: value("LEDGER_GRPC_ADDRESS", ":9091"), KafkaBrokers: strings.Split(value("LEDGER_KAFKA_BROKERS", "localhost:9092"), ","), CommandsTopic: value("LEDGER_COMMANDS_TOPIC", "ledger-commands"), ConsumerGroup: value("LEDGER_CONSUMER_GROUP", "cex-ledger-service")}
	if c.DatabaseURL == "" {
		return c, errors.New("LEDGER_DATABASE_URL is required")
	}
	return c, nil
}
func value(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}
