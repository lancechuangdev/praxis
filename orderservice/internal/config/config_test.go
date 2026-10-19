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

func TestLifecycleCaptureIsOptInAndValidated(t *testing.T) {
	t.Setenv("ORDER_LIFECYCLE_CAPTURE", "false")
	cfg, err := Load()
	if err != nil || cfg.LifecycleCapture {
		t.Fatalf("default capture=%v error=%v", cfg.LifecycleCapture, err)
	}
	t.Setenv("ORDER_LIFECYCLE_CAPTURE", "true")
	cfg, err = Load()
	if err != nil || !cfg.LifecycleCapture {
		t.Fatalf("enabled capture=%v error=%v", cfg.LifecycleCapture, err)
	}
	t.Setenv("ORDER_LIFECYCLE_CAPTURE", "invalid")
	if _, err := Load(); err == nil {
		t.Fatal("invalid capture flag accepted")
	}
}

func TestWorkflowObservationIsIndependentAndOptIn(t *testing.T) {
	t.Setenv("ORDER_WORKFLOW_OBSERVE", "")
	t.Setenv("ORDER_LIFECYCLE_CAPTURE", "false")
	cfg, err := Load()
	if err != nil || cfg.WorkflowObserve {
		t.Fatalf("default observe=%v error=%v", cfg.WorkflowObserve, err)
	}
	t.Setenv("ORDER_WORKFLOW_OBSERVE", "true")
	cfg, err = Load()
	if err != nil || !cfg.WorkflowObserve || cfg.LifecycleCapture {
		t.Fatalf("observe=%v capture=%v error=%v", cfg.WorkflowObserve, cfg.LifecycleCapture, err)
	}
	t.Setenv("ORDER_WORKFLOW_OBSERVE", "invalid")
	if _, err := Load(); err == nil {
		t.Fatal("invalid observation flag accepted")
	}
}

func TestWorkflowDispatchEnrollment(t *testing.T) {
	t.Setenv("ORDER_WORKFLOW_DISPATCH", "invalid")
	if _, err := Load(); err == nil {
		t.Fatal("invalid flag accepted")
	}
	t.Setenv("ORDER_WORKFLOW_DISPATCH", "true")
	t.Setenv("ORDER_KAFKA_BROKERS", "")
	if _, err := Load(); err == nil {
		t.Fatal("missing broker accepted")
	}
	t.Setenv("ORDER_KAFKA_BROKERS", "broker:9098")
	t.Setenv("ORDER_KAFKA_AUTH_MODE", "msk_iam")
	t.Setenv("AWS_REGION", "")
	if _, err := Load(); err == nil {
		t.Fatal("IAM region missing")
	}
	t.Setenv("AWS_REGION", "us-east-1")
	c, err := Load()
	if err != nil || !c.WorkflowDispatch {
		t.Fatal(c.WorkflowDispatch, err)
	}
}
