package config

import (
	"crypto/tls"
	"errors"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"praxis/projectionservice/internal/kafkaauth"
	"praxis/projectionservice/internal/kafkaconsumer"
	"praxis/projectionservice/internal/workflow"
)

type Config struct {
	Enabled            bool
	HTTPAddress        string
	Mongo              *options.ClientOptions
	Database           string
	Target             workflow.Target
	TransactionTimeout time.Duration
	Kafka              kafkaconsumer.Config
}

func Load() (Config, error) {
	var cfg Config
	enabled := os.Getenv("PROJECTION_WORKFLOW_ENABLED")
	if enabled == "" {
		enabled = "false"
	}
	var err error
	cfg.Enabled, err = strconv.ParseBool(enabled)
	if err != nil {
		return cfg, errors.New("PROJECTION_WORKFLOW_ENABLED must be true or false")
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
	cfg.Target = workflow.Target{Generation: os.Getenv("PROJECTION_GENERATION"), Mode: workflow.Mode(os.Getenv("PROJECTION_MODE"))}
	if err := cfg.Target.Validate(); err != nil {
		return cfg, err
	}
	cfg.Database = os.Getenv("PROJECTION_MONGODB_DATABASE")
	if cfg.Database == "" || strings.ContainsAny(cfg.Database, " /\\.\"$\x00") || len(cfg.Database) > 63 {
		return cfg, errors.New("PROJECTION_MONGODB_DATABASE must be an explicit valid database name")
	}
	uri := os.Getenv("PROJECTION_MONGODB_URI")
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
	cfg.Kafka = kafkaconsumer.Config{Brokers: brokers, Target: cfg.Target, GroupNamespace: os.Getenv("PROJECTION_GROUP_NAMESPACE"), CommitTimeout: commitTimeout, Dialer: dialer}
	if cfg.Kafka.GroupNamespace == "" {
		return cfg, errors.New("PROJECTION_GROUP_NAMESPACE is required")
	}
	if err := cfg.Kafka.Validate(); err != nil {
		return cfg, err
	}
	return cfg, nil
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
