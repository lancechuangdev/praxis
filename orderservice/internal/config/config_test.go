package config

import "testing"

func TestLoadRejectsAdmissionReserveThatConsumesDownstreamCapacity(t *testing.T) {
	t.Setenv("ORDER_DOWNSTREAM_CONCURRENCY", "8")
	t.Setenv("ORDER_DOWNSTREAM_ADMISSION_RESERVE", "8")
	if _, err := Load(); err == nil {
		t.Fatal("expected invalid downstream capacity reservation")
	}
}

func TestLoadAcceptsDedicatedRecoveryAndAdmissionCapacity(t *testing.T) {
	t.Setenv("ORDER_DOWNSTREAM_CONCURRENCY", "8")
	t.Setenv("ORDER_DOWNSTREAM_ADMISSION_RESERVE", "3")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DownstreamConcurrency-cfg.DownstreamAdmissionReserve != 5 {
		t.Fatalf("recovery capacity=%d, want 5", cfg.DownstreamConcurrency-cfg.DownstreamAdmissionReserve)
	}
}

func TestLoadValidatesRiskShadowConfiguration(t *testing.T) {
	t.Setenv("ORDER_RISK_REMOTE_MODE", "invalid")
	if _, err := Load(); err == nil {
		t.Fatal("expected unsupported risk mode")
	}
	t.Setenv("ORDER_RISK_REMOTE_MODE", "shadow")
	t.Setenv("ORDER_RISK_SHADOW_MAX_IN_FLIGHT", "0")
	if _, err := Load(); err == nil {
		t.Fatal("expected invalid shadow concurrency")
	}
}

func TestLoadAcceptsAuthoritativeRiskMode(t *testing.T) {
	t.Setenv("ORDER_RISK_REMOTE_MODE", "authoritative")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RiskRemoteMode != "authoritative" {
		t.Fatalf("mode=%q", cfg.RiskRemoteMode)
	}
}
