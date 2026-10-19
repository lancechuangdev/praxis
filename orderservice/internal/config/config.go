package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"praxis/orderservice/internal/kafkaauth"
)

type Config struct {
	WorkflowDispatch           bool
	KafkaBrokers               []string
	KafkaAuth                  kafkaauth.Config
	LifecycleCapture           bool
	WorkflowObserve            bool
	HTTPAddress                string
	DatabaseURL                string
	SagaRetryInterval          time.Duration
	SagaRecoveryRestartDelay   time.Duration
	SagaRecoveryMaxPanics      int
	SagaMaxReleaseAttempts     int
	SagaMaxReservationAttempts int
	SagaClaimLease             time.Duration
	SagaReservationConcurrency int
	SagaReleaseConcurrency     int
	DownstreamConcurrency      int
	DownstreamAdmissionReserve int
	AdmissionMode              string
	AdmissionMaxInFlight       int
	AdmissionMaxWaiting        int
	AdmissionBucketCapacity    int
	AdmissionRefillPerSecond   int
	LedgerMode                 string
	LedgerGRPCAddress          string
	LedgerTimeout              time.Duration
	MockLedgerLatency          time.Duration
	RiskLatency                time.Duration
	RiskRejectBPS              int
	RiskRemoteMode             string
	RiskGRPCAddress            string
	RiskTimeout                time.Duration
	RiskShadowMaxInFlight      int
	MatchingMode               string
	MatchingGRPCAddress        string
	MatchingTimeout            time.Duration
	MockMatchingLatency        time.Duration
}

func Load() (Config, error) {
	dispatch, err := strconv.ParseBool(value("ORDER_WORKFLOW_DISPATCH", "false"))
	if err != nil {
		return Config{}, errors.New("ORDER_WORKFLOW_DISPATCH must be true or false")
	}
	var brokers []string
	auth := kafkaauth.Config{}
	if dispatch {
		brokers = strings.Split(os.Getenv("ORDER_KAFKA_BROKERS"), ",")
		for i, broker := range brokers {
			brokers[i] = strings.TrimSpace(broker)
			host, port, err := net.SplitHostPort(brokers[i])
			n, e := strconv.Atoi(port)
			if err != nil || e != nil || host == "" || n < 1 || n > 65535 {
				return Config{}, errors.New("ORDER_KAFKA_BROKERS requires host:port endpoints")
			}
		}
		auth = kafkaauth.Config{Mode: os.Getenv("ORDER_KAFKA_AUTH_MODE"), Region: os.Getenv("AWS_REGION"), Username: os.Getenv("ORDER_KAFKA_USERNAME"), Password: os.Getenv("ORDER_KAFKA_PASSWORD")}
		if _, err := kafkaauth.Dialer(auth); err != nil {
			return Config{}, err
		}
	}
	observe, err := strconv.ParseBool(value("ORDER_WORKFLOW_OBSERVE", "false"))
	if err != nil {
		return Config{}, errors.New("ORDER_WORKFLOW_OBSERVE must be true or false")
	}
	capture, err := strconv.ParseBool(value("ORDER_LIFECYCLE_CAPTURE", "false"))
	if err != nil {
		return Config{}, errors.New("ORDER_LIFECYCLE_CAPTURE must be true or false")
	}
	c := Config{
		WorkflowDispatch: dispatch, KafkaBrokers: brokers, KafkaAuth: auth,
		LifecycleCapture:           capture,
		WorkflowObserve:            observe,
		HTTPAddress:                value("ORDER_HTTP_ADDRESS", ":8083"),
		DatabaseURL:                value("ORDER_DATABASE_URL", "postgres://ledger:ledger@localhost:5433/cex_ledger?sslmode=disable"),
		SagaRetryInterval:          duration("ORDER_SAGA_RETRY_INTERVAL", time.Second),
		SagaRecoveryRestartDelay:   duration("ORDER_SAGA_RECOVERY_RESTART_DELAY", time.Second),
		SagaRecoveryMaxPanics:      integer("ORDER_SAGA_RECOVERY_MAX_PANICS", 3),
		SagaMaxReleaseAttempts:     integer("ORDER_SAGA_MAX_RELEASE_ATTEMPTS", 10),
		SagaMaxReservationAttempts: integer("ORDER_SAGA_MAX_RESERVATION_ATTEMPTS", 10),
		SagaClaimLease:             duration("ORDER_SAGA_CLAIM_LEASE", 30*time.Second),
		SagaReservationConcurrency: integer("ORDER_SAGA_RESERVATION_CONCURRENCY", 4),
		SagaReleaseConcurrency:     integer("ORDER_SAGA_RELEASE_CONCURRENCY", 4),
		DownstreamConcurrency:      integer("ORDER_DOWNSTREAM_CONCURRENCY", 16),
		DownstreamAdmissionReserve: integer("ORDER_DOWNSTREAM_ADMISSION_RESERVE", 8),
		AdmissionMode:              value("ORDER_ADMISSION_MODE", "observe"),
		AdmissionMaxInFlight:       integer("ORDER_ADMISSION_MAX_IN_FLIGHT", 128),
		AdmissionMaxWaiting:        integer("ORDER_ADMISSION_MAX_WAITING", 256),
		AdmissionBucketCapacity:    integer("ORDER_ADMISSION_BUCKET_CAPACITY", 1000),
		AdmissionRefillPerSecond:   integer("ORDER_ADMISSION_REFILL_PER_SECOND", 500),
		LedgerMode:                 value("ORDER_LEDGER_MODE", "grpc"),
		LedgerGRPCAddress:          value("ORDER_LEDGER_GRPC_ADDRESS", "localhost:9091"),
		LedgerTimeout:              duration("ORDER_LEDGER_TIMEOUT", 2*time.Second),
		MockLedgerLatency:          duration("ORDER_MOCK_LEDGER_LATENCY", 3*time.Millisecond),
		RiskLatency:                duration("ORDER_RISK_LATENCY", time.Millisecond),
		RiskRejectBPS:              integer("ORDER_RISK_REJECT_BPS", 0),
		RiskRemoteMode:             value("ORDER_RISK_REMOTE_MODE", "off"),
		RiskGRPCAddress:            value("ORDER_RISK_GRPC_ADDRESS", "localhost:9095"),
		RiskTimeout:                duration("ORDER_RISK_TIMEOUT", 100*time.Millisecond),
		RiskShadowMaxInFlight:      integer("ORDER_RISK_SHADOW_MAX_IN_FLIGHT", 32),
		MatchingMode:               value("ORDER_MATCHING_MODE", "grpc"),
		MatchingGRPCAddress:        value("ORDER_MATCHING_GRPC_ADDRESS", "localhost:9092"),
		MatchingTimeout:            duration("ORDER_MATCHING_TIMEOUT", 2*time.Second),
		MockMatchingLatency:        duration("ORDER_MOCK_MATCHING_LATENCY", time.Millisecond),
	}
	if c.LedgerMode != "grpc" && c.LedgerMode != "mock" {
		return c, errors.New("ORDER_LEDGER_MODE must be grpc or mock")
	}
	if c.RiskRejectBPS < 0 || c.RiskRejectBPS > 10000 {
		return c, errors.New("ORDER_RISK_REJECT_BPS must be between 0 and 10000")
	}
	if c.RiskRemoteMode != "off" && c.RiskRemoteMode != "shadow" && c.RiskRemoteMode != "authoritative" {
		return c, errors.New("ORDER_RISK_REMOTE_MODE must be off, shadow, or authoritative")
	}
	if c.RiskTimeout <= 0 || c.RiskShadowMaxInFlight < 1 {
		return c, errors.New("risk timeout and shadow max in-flight must be positive")
	}
	if c.MatchingMode != "grpc" && c.MatchingMode != "mock" {
		return c, errors.New("ORDER_MATCHING_MODE must be grpc or mock")
	}
	if c.SagaMaxReleaseAttempts < 1 {
		return c, errors.New("ORDER_SAGA_MAX_RELEASE_ATTEMPTS must be positive")
	}
	if c.SagaRetryInterval <= 0 {
		return c, errors.New("ORDER_SAGA_RETRY_INTERVAL must be positive")
	}
	if c.SagaRecoveryRestartDelay < 0 {
		return c, errors.New("ORDER_SAGA_RECOVERY_RESTART_DELAY must not be negative")
	}
	if c.SagaRecoveryMaxPanics < 1 {
		return c, errors.New("ORDER_SAGA_RECOVERY_MAX_PANICS must be positive")
	}
	if c.SagaMaxReservationAttempts < 1 {
		return c, errors.New("ORDER_SAGA_MAX_RESERVATION_ATTEMPTS must be positive")
	}
	if c.SagaClaimLease <= c.LedgerTimeout+c.MatchingTimeout {
		return c, errors.New("ORDER_SAGA_CLAIM_LEASE must exceed the combined Ledger and Matching timeouts")
	}
	if c.SagaReservationConcurrency < 1 || c.SagaReleaseConcurrency < 1 {
		return c, errors.New("saga retry concurrency limits must be positive")
	}
	if c.DownstreamConcurrency < 2 || c.DownstreamAdmissionReserve < 1 || c.DownstreamAdmissionReserve >= c.DownstreamConcurrency {
		return c, errors.New("downstream concurrency must exceed the positive admission reserve")
	}
	if c.AdmissionMode != "off" && c.AdmissionMode != "observe" {
		return c, errors.New("ORDER_ADMISSION_MODE must be off or observe until a shared authoritative limiter is configured")
	}
	if c.AdmissionMaxInFlight < 1 || c.AdmissionMaxWaiting < 0 || c.AdmissionBucketCapacity < 1 || c.AdmissionRefillPerSecond < 1 {
		return c, errors.New("admission limits must be positive and max waiting must not be negative")
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
