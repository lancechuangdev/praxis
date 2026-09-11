package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	DatabaseURL                  string
	DBMaxConns                   int32
	HTTPAddress, GRPCAddress     string
	KafkaBrokers                 []string
	CommandsTopic, ConsumerGroup string
}

func Load() (Config, error) {
	maxConns, err := positiveInt32("LEDGER_DB_MAX_CONNS", 32)
	if err != nil {
		return Config{}, err
	}
	c := Config{DatabaseURL: os.Getenv("LEDGER_DATABASE_URL"), DBMaxConns: maxConns, HTTPAddress: value("LEDGER_HTTP_ADDRESS", ":8081"), GRPCAddress: value("LEDGER_GRPC_ADDRESS", ":9091"), KafkaBrokers: strings.Split(value("LEDGER_KAFKA_BROKERS", "localhost:9092"), ","), CommandsTopic: value("LEDGER_COMMANDS_TOPIC", "ledger-commands"), ConsumerGroup: value("LEDGER_CONSUMER_GROUP", "cex-ledger-service")}
	if c.DatabaseURL == "" {
		return c, errors.New("LEDGER_DATABASE_URL is required")
	}
	return c, nil
}

func positiveInt32(name string, fallback int32) (int32, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	v, err := strconv.ParseInt(raw, 10, 32)
	if err != nil || v < 1 {
		return 0, fmt.Errorf("%s must be a positive 32-bit integer", name)
	}
	return int32(v), nil
}
func value(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}
