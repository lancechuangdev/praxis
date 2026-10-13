package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	GRPCAddress, HTTPAddress string
	PolicyVersion            string
	RejectBPS                int
	DecisionLatency          time.Duration
}

func Load() (Config, error) {
	c := Config{GRPCAddress: value("RISK_GRPC_ADDRESS", ":9095"), HTTPAddress: value("RISK_HTTP_ADDRESS", ":8085"), PolicyVersion: value("RISK_POLICY_VERSION", "embedded-v1")}
	var err error
	if c.RejectBPS, err = integer("RISK_REJECT_BPS", 0); err != nil || c.RejectBPS < 0 || c.RejectBPS > 10000 {
		return Config{}, fmt.Errorf("RISK_REJECT_BPS must be an integer between 0 and 10000")
	}
	if c.DecisionLatency, err = duration("RISK_DECISION_LATENCY", time.Millisecond); err != nil || c.DecisionLatency < 0 {
		return Config{}, fmt.Errorf("RISK_DECISION_LATENCY must be a non-negative duration")
	}
	return c, nil
}

func value(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
func integer(name string, fallback int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	return strconv.Atoi(raw)
}
func duration(name string, fallback time.Duration) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	return time.ParseDuration(raw)
}
