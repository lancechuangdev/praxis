package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"praxis/ledgerservice/internal/kafkaauth"
)

type Config struct {
	DatabaseURL                  string
	DBMaxConns                   int32
	DBStatementCacheCapacity     int
	DBQueryExecMode              pgx.QueryExecMode
	HTTPAddress, GRPCAddress     string
	KafkaBrokers                 []string
	KafkaAuth                    kafkaauth.Config
	CommandsTopic, ConsumerGroup string
}

func Load() (Config, error) {
	maxConns, err := positiveInt32("LEDGER_DB_MAX_CONNS", 32)
	if err != nil {
		return Config{}, err
	}
	statementCacheCapacity, err := positiveInt("LEDGER_DB_STATEMENT_CACHE_CAPACITY", 128)
	if err != nil {
		return Config{}, err
	}
	queryExecMode, err := queryExecMode(value("LEDGER_DB_QUERY_EXEC_MODE", "cache_statement"))
	if err != nil {
		return Config{}, err
	}
	kafkaAuth, err := kafkaauth.Load()
	if err != nil {
		return Config{}, err
	}
	c := Config{DatabaseURL: os.Getenv("LEDGER_DATABASE_URL"), DBMaxConns: maxConns, DBStatementCacheCapacity: statementCacheCapacity, DBQueryExecMode: queryExecMode, HTTPAddress: value("LEDGER_HTTP_ADDRESS", ":8081"), GRPCAddress: value("LEDGER_GRPC_ADDRESS", ":9091"), KafkaBrokers: strings.Split(value("LEDGER_KAFKA_BROKERS", "localhost:9092"), ","), KafkaAuth: kafkaAuth, CommandsTopic: value("LEDGER_COMMANDS_TOPIC", "ledger-commands"), ConsumerGroup: value("LEDGER_CONSUMER_GROUP", "cex-ledger-service")}
	if c.DatabaseURL == "" {
		return c, errors.New("LEDGER_DATABASE_URL is required")
	}
	return c, nil
}

func positiveInt(name string, fallback int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v < 1 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return v, nil
}

func queryExecMode(raw string) (pgx.QueryExecMode, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "cache_statement":
		return pgx.QueryExecModeCacheStatement, nil
	case "cache_describe":
		return pgx.QueryExecModeCacheDescribe, nil
	case "describe_exec":
		return pgx.QueryExecModeDescribeExec, nil
	case "exec":
		return pgx.QueryExecModeExec, nil
	case "simple_protocol":
		return pgx.QueryExecModeSimpleProtocol, nil
	default:
		return 0, fmt.Errorf("LEDGER_DB_QUERY_EXEC_MODE must be cache_statement, cache_describe, describe_exec, exec, or simple_protocol")
	}
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
