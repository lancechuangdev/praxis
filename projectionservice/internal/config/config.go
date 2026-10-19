package config

import (
	"crypto/tls"
	"errors"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"praxis/projectionservice/internal/kafkaauth"
	"praxis/projectionservice/internal/kafkaconsumer"
	"praxis/projectionservice/internal/readapi"
	"praxis/projectionservice/internal/workflow"
)

type Config struct {
	ReadEnabled        bool
	ReadAuth           readapi.Config
	Stream             string
	Enabled            bool
	LagObserve         bool
	HTTPAddress        string
	Mongo              *options.ClientOptions
	Database           string
	Target             workflow.Target
	TransactionTimeout time.Duration
	Kafka              kafkaconsumer.Config
}

func Load() (Config, error)      { return load(true) }
func LoadAdmin() (Config, error) { return load(false) }
func load(consumer bool) (Config, error) {
	var cfg Config
	cfg.Stream = os.Getenv("PROJECTION_STREAM")
	if cfg.Stream == "" {
		cfg.Stream = "workflow"
	}
	if cfg.Stream != "workflow" && cfg.Stream != "journal" {
		return cfg, errors.New("PROJECTION_STREAM must be workflow or journal")
	}
	enabled := os.Getenv("PROJECTION_ENABLED")
	legacyEnabled := os.Getenv("PROJECTION_WORKFLOW_ENABLED")
	if enabled != "" && legacyEnabled != "" && enabled != legacyEnabled {
		return cfg, errors.New("conflicting projection enable flags")
	}
	if enabled == "" {
		enabled = legacyEnabled
	}
	if enabled == "" {
		enabled = "false"
	}
	var err error
	cfg.Enabled, err = strconv.ParseBool(enabled)
	if err != nil {
		return cfg, errors.New("projection enable flag must be true or false")
	}
	cfg.HTTPAddress = os.Getenv("PROJECTION_HTTP_ADDRESS")
	if cfg.HTTPAddress == "" {
		cfg.HTTPAddress = ":8086"
	}
	if _, _, err := net.SplitHostPort(cfg.HTTPAddress); err != nil {
		return cfg, errors.New("PROJECTION_HTTP_ADDRESS must be host:port")
	}
	if !cfg.Enabled {
		return cfg, nil
	}
	lagFlag := os.Getenv("PROJECTION_LAG_OBSERVE")
	if lagFlag == "" {
		lagFlag = "false"
	}
	cfg.LagObserve, err = strconv.ParseBool(lagFlag)
	if err != nil {
		return cfg, errors.New("PROJECTION_LAG_OBSERVE must be true or false")
	}
	cfg.Target = workflow.Target{Generation: os.Getenv("PROJECTION_GENERATION"), Mode: workflow.Mode(os.Getenv("PROJECTION_MODE"))}
	if err := cfg.Target.Validate(); err != nil {
		return cfg, err
	}
	cfg.Database = os.Getenv("PROJECTION_MONGODB_DATABASE")
	if cfg.Database == "" || strings.ContainsAny(cfg.Database, " /\\.\"$\x00") || len(cfg.Database) > 63 {
		return cfg, errors.New("PROJECTION_MONGODB_DATABASE must be an explicit valid database name")
	}
	uri, err := mongoURI()
	if err != nil {
		return cfg, err
	}
	if uri == "" {
		return cfg, errors.New("PROJECTION_MONGODB_URI is required")
	}
	mongoOptions := options.Client().ApplyURI(uri)
	if err := mongoOptions.Validate(); err != nil {
		return cfg, errors.New("invalid PROJECTION_MONGODB_URI")
	}
	tlsConfig := mongoOptions.TLSConfig
	if tlsConfig == nil {
		tlsConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	} else {
		tlsConfig = tlsConfig.Clone()
	}
	if tlsConfig.InsecureSkipVerify {
		return cfg, errors.New("MongoDB certificate verification must be enabled")
	}
	if tlsConfig.MinVersion < tls.VersionTLS12 {
		tlsConfig.MinVersion = tls.VersionTLS12
	}
	cfg.Mongo = mongoOptions.SetTLSConfig(tlsConfig).SetMaxPoolSize(8).SetMinPoolSize(0).SetConnectTimeout(5 * time.Second).SetServerSelectionTimeout(5 * time.Second).SetTimeout(10 * time.Second)
	cfg.TransactionTimeout, err = boundedDuration("PROJECTION_TRANSACTION_TIMEOUT", 10*time.Second)
	if err != nil {
		return cfg, err
	}
	if !consumer {
		return cfg, nil
	}
	commitTimeout, err := boundedDuration("PROJECTION_COMMIT_TIMEOUT", 5*time.Second)
	if err != nil {
		return cfg, err
	}
	dialer, err := kafkaauth.Dialer(kafkaauth.Config{Mode: os.Getenv("PROJECTION_KAFKA_AUTH_MODE"), Region: os.Getenv("AWS_REGION"), Username: os.Getenv("PROJECTION_KAFKA_USERNAME"), Password: os.Getenv("PROJECTION_KAFKA_PASSWORD")})
	if err != nil {
		return cfg, err
	}
	brokers := strings.Split(os.Getenv("PROJECTION_KAFKA_BROKERS"), ",")
	for i, broker := range brokers {
		brokers[i] = strings.TrimSpace(broker)
		host, port, err := net.SplitHostPort(brokers[i])
		portNumber, portErr := strconv.Atoi(port)
		if err != nil || host == "" || portErr != nil || portNumber < 1 || portNumber > 65535 {
			return cfg, errors.New("PROJECTION_KAFKA_BROKERS must contain host:port endpoints")
		}
	}
	cfg.Kafka = kafkaconsumer.Config{Stream: cfg.Stream, Brokers: brokers, Target: cfg.Target, GroupNamespace: os.Getenv("PROJECTION_GROUP_NAMESPACE"), CommitTimeout: commitTimeout, Dialer: dialer}
	retries := os.Getenv("PROJECTION_GAP_RETRIES")
	if retries == "" {
		retries = "0"
	}
	cfg.Kafka.GapRetry.Retries, err = strconv.Atoi(retries)
	if err != nil {
		return cfg, errors.New("PROJECTION_GAP_RETRIES must be an integer between 0 and 3")
	}
	if cfg.Kafka.GapRetry.Retries != 0 {
		cfg.Kafka.GapRetry.Delay, err = boundedDuration("PROJECTION_GAP_RETRY_DELAY", 100*time.Millisecond)
		if err != nil {
			return cfg, err
		}
		cfg.Kafka.GapRetry.Budget, err = boundedDuration("PROJECTION_GAP_RETRY_BUDGET", 5*time.Second)
		if err != nil {
			return cfg, err
		}
	}
	if cfg.Kafka.GroupNamespace == "" {
		return cfg, errors.New("PROJECTION_GROUP_NAMESPACE is required")
	}
	if err := cfg.Kafka.Validate(); err != nil {
		return cfg, err
	}
	cfg.ReadEnabled, err = strconv.ParseBool(envDefault("PROJECTION_READS_ENABLED", "false"))
	if err != nil {
		return cfg, errors.New("PROJECTION_READS_ENABLED must be true or false")
	}
	if cfg.ReadEnabled {
		cfg.ReadAuth = readapi.Config{Issuer: os.Getenv("PROJECTION_JWT_ISSUER"), Audience: os.Getenv("PROJECTION_JWT_AUDIENCE"), KeyID: os.Getenv("PROJECTION_JWT_KEY_ID")}
		file, err := os.Open(os.Getenv("PROJECTION_JWT_PUBLIC_KEY_FILE"))
		if err != nil {
			return cfg, errors.New("JWT public key file is required")
		}
		raw, err := io.ReadAll(io.LimitReader(file, 65537))
		_ = file.Close()
		if err != nil || len(raw) > 65536 {
			return cfg, errors.New("invalid JWT public key file")
		}
		key, err := jwt.ParseRSAPublicKeyFromPEM(raw)
		if err != nil || (key.N.BitLen() < 2048 || key.N.BitLen() > 8192) || cfg.ReadAuth.Issuer == "" || cfg.ReadAuth.Audience == "" || cfg.ReadAuth.KeyID == "" {
			return cfg, errors.New("projection reads require issuer, audience, key ID and RSA public key >=2048 bits")
		}
		cfg.ReadAuth.PublicKey = key
	}
	return cfg, nil
}
func envDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func boundedDuration(key string, fallback time.Duration) (time.Duration, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 || value > 30*time.Second {
		return 0, errors.New(key + " must be a duration in (0, 30s]")
	}
	return value, nil
}
