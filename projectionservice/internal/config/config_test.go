package config

import (
	"crypto/tls"
	"strings"
	"testing"
	"time"
)

func enabledEnvironment(t *testing.T) {
	t.Helper()
	for key, value := range map[string]string{
		"PROJECTION_WORKFLOW_ENABLED": "true", "PROJECTION_HTTP_ADDRESS": "127.0.0.1:0",
		"PROJECTION_MONGODB_URI":      "mongodb://user:secret@localhost:27017/?replicaSet=rs0",
		"PROJECTION_MONGODB_DATABASE": "test_projection", "PROJECTION_GENERATION": "orders-v1",
		"PROJECTION_MODE": "shadow", "PROJECTION_GROUP_NAMESPACE": "dev-db",
		"PROJECTION_KAFKA_BROKERS": "broker:9096", "PROJECTION_KAFKA_USERNAME": "user", "PROJECTION_KAFKA_PASSWORD": "secret",
		"PROJECTION_KAFKA_AUTH_MODE": "scram_sha512", "AWS_REGION": "",
		"PROJECTION_LAG_OBSERVE": "false",
		"PROJECTION_GAP_RETRIES": "0", "PROJECTION_GAP_RETRY_DELAY": "100ms", "PROJECTION_GAP_RETRY_BUDGET": "5s",
		"PROJECTION_TRANSACTION_TIMEOUT": "10s", "PROJECTION_COMMIT_TIMEOUT": "5s",
	} {
		t.Setenv(key, value)
	}
}

func TestDisabledRuntimeRequiresNoConnections(t *testing.T) {
	t.Setenv("PROJECTION_WORKFLOW_ENABLED", "")
	t.Setenv("PROJECTION_HTTP_ADDRESS", "")
	t.Setenv("PROJECTION_MONGODB_URI", "invalid")
	t.Setenv("PROJECTION_KAFKA_AUTH_MODE", "invalid")
	cfg, err := Load()
	if err != nil || cfg.Enabled || cfg.Mongo != nil {
		t.Fatalf("disabled config err=%v", err)
	}
}

func TestIAMRuntimeDoesNotRequireSCRAMOrRetrieveAWSCredentialsAtLoad(t *testing.T) {
	enabledEnvironment(t)
	t.Setenv("PROJECTION_KAFKA_AUTH_MODE", "msk_iam")
	t.Setenv("PROJECTION_KAFKA_USERNAME", "")
	t.Setenv("PROJECTION_KAFKA_PASSWORD", "")
	if _, err := Load(); err == nil {
		t.Fatal("IAM accepted without explicit region")
	}
	t.Setenv("AWS_REGION", "us-west-2")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Kafka.Dialer.SASLMechanism.Name() != "OAUTHBEARER" || cfg.Kafka.Dialer.TLS.InsecureSkipVerify {
		t.Fatal("IAM mechanism or TLS policy missing")
	}
}

func TestEnabledRuntimeBoundsAndSecureTransport(t *testing.T) {
	enabledEnvironment(t)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Mongo.TLSConfig == nil || cfg.Mongo.TLSConfig.InsecureSkipVerify || cfg.Mongo.TLSConfig.MinVersion < tls.VersionTLS12 {
		t.Fatal("insecure Mongo transport")
	}
	if *cfg.Mongo.MaxPoolSize != 8 || *cfg.Mongo.MinPoolSize != 0 || *cfg.Mongo.ServerSelectionTimeout != 5*time.Second {
		t.Fatal("unbounded Mongo pool or timeout")
	}
	if cfg.Kafka.Dialer.SASLMechanism.Name() != "SCRAM-SHA-512" || cfg.Kafka.Target != cfg.Target {
		t.Fatal("Kafka authentication or target mismatch")
	}
	if err := cfg.Kafka.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestInvalidRuntimeConfigurationAndRedaction(t *testing.T) {
	for _, tc := range []struct{ key, value string }{
		{"PROJECTION_WORKFLOW_ENABLED", "invalid"}, {"PROJECTION_HTTP_ADDRESS", "invalid"},
		{"PROJECTION_MODE", "live"}, {"PROJECTION_GENERATION", "../live"},
		{"PROJECTION_MONGODB_DATABASE", "bad.name"}, {"PROJECTION_MONGODB_URI", "mongodb://secret@/invalid"},
		{"PROJECTION_MONGODB_URI", "mongodb://user:secret@localhost/?tls=true&tlsInsecure=true"},
		{"PROJECTION_KAFKA_USERNAME", ""}, {"PROJECTION_KAFKA_PASSWORD", ""},
		{"PROJECTION_KAFKA_BROKERS", "broker"}, {"PROJECTION_GROUP_NAMESPACE", "bad.namespace"},
		{"PROJECTION_KAFKA_BROKERS", ":9096"}, {"PROJECTION_KAFKA_BROKERS", "broker:0"},
		{"PROJECTION_TRANSACTION_TIMEOUT", "0s"}, {"PROJECTION_COMMIT_TIMEOUT", "31s"},
		{"PROJECTION_COMMIT_TIMEOUT", "invalid"},
		{"PROJECTION_LAG_OBSERVE", "invalid"},
		{"PROJECTION_GAP_RETRIES", "invalid"}, {"PROJECTION_GAP_RETRIES", "4"}, {"PROJECTION_GAP_RETRIES", "-1"},
	} {
		t.Run(tc.key+tc.value, func(t *testing.T) {
			enabledEnvironment(t)
			t.Setenv(tc.key, tc.value)
			_, err := Load()
			if err == nil {
				t.Fatal("invalid runtime config accepted")
			}
			if strings.Contains(err.Error(), "secret") {
				t.Fatal("configuration error leaked credentials")
			}
		})
	}
}

func TestGapRetryRuntimeIsExplicitAndBounded(t *testing.T) {
	enabledEnvironment(t)
	cfg, err := Load()
	if err != nil || cfg.Kafka.GapRetry.Retries != 0 {
		t.Fatal("gap retry not opt-in")
	}
	t.Setenv("PROJECTION_GAP_RETRIES", "2")
	cfg, err = Load()
	if err != nil || cfg.Kafka.GapRetry.Retries != 2 || cfg.Kafka.GapRetry.Budget != 5*time.Second {
		t.Fatal("gap runtime policy not loaded")
	}
	t.Setenv("PROJECTION_GAP_RETRY_DELAY", "2s")
	if _, err := Load(); err == nil {
		t.Fatal("unbounded gap retry delay accepted")
	}
}

func TestLagObservationIsOptIn(t *testing.T) {
	enabledEnvironment(t)
	cfg, err := Load()
	if err != nil || cfg.LagObserve {
		t.Fatal("lag observation enabled by default")
	}
	t.Setenv("PROJECTION_LAG_OBSERVE", "true")
	cfg, err = Load()
	if err != nil || !cfg.LagObserve {
		t.Fatal("lag observation flag ignored")
	}
}
