package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTPAddress         string
	LedgerMode          string
	LedgerGRPCAddress   string
	LedgerTimeout       time.Duration
	MockLedgerLatency   time.Duration
	RiskLatency         time.Duration
	RiskRejectBPS       int
	MatchingMode        string
	MatchingGRPCAddress string
	MatchingTimeout     time.Duration
	MockMatchingLatency time.Duration
}

func Load() (Config, error) {
	c := Config{
		HTTPAddress:         value("ORDER_HTTP_ADDRESS", ":8083"),
		LedgerMode:          value("ORDER_LEDGER_MODE", "grpc"),
		LedgerGRPCAddress:   value("ORDER_LEDGER_GRPC_ADDRESS", "localhost:9091"),
		LedgerTimeout:       duration("ORDER_LEDGER_TIMEOUT", 2*time.Second),
		MockLedgerLatency:   duration("ORDER_MOCK_LEDGER_LATENCY", 3*time.Millisecond),
		RiskLatency:         duration("ORDER_RISK_LATENCY", time.Millisecond),
		RiskRejectBPS:       integer("ORDER_RISK_REJECT_BPS", 0),
		MatchingMode:        value("ORDER_MATCHING_MODE", "grpc"),
		MatchingGRPCAddress: value("ORDER_MATCHING_GRPC_ADDRESS", "localhost:9092"),
		MatchingTimeout:     duration("ORDER_MATCHING_TIMEOUT", 2*time.Second),
		MockMatchingLatency: duration("ORDER_MOCK_MATCHING_LATENCY", time.Millisecond),
	}
	if c.LedgerMode != "grpc" && c.LedgerMode != "mock" {
		return c, errors.New("ORDER_LEDGER_MODE must be grpc or mock")
	}
	if c.RiskRejectBPS < 0 || c.RiskRejectBPS > 10000 {
		return c, errors.New("ORDER_RISK_REJECT_BPS must be between 0 and 10000")
	}
	if c.MatchingMode != "grpc" && c.MatchingMode != "mock" {
		return c, errors.New("ORDER_MATCHING_MODE must be grpc or mock")
	}
	return c, nil
}

func value(name, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		return v
	}
	return fallback
}
func integer(name string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		panic(fmt.Sprintf("invalid %s: %v", name, err))
	}
	return v
}
func duration(name string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	v, err := time.ParseDuration(raw)
	if err != nil {
		panic(fmt.Sprintf("invalid %s: %v", name, err))
	}
	return v
}
