package kafkaauth

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-msk-iam-sasl-signer-go/signer"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
)

type Config struct {
	Mode   string
	Region string
}

func Load() (Config, error) {
	mode := strings.ToLower(strings.TrimSpace(os.Getenv("KAFKA_AUTH_MODE")))
	if mode == "" {
		mode = "plaintext"
	}
	config := Config{Mode: mode, Region: strings.TrimSpace(os.Getenv("AWS_REGION"))}
	switch mode {
	case "plaintext":
		return config, nil
	case "msk_iam":
		if config.Region == "" {
			return Config{}, errors.New("AWS_REGION is required when KAFKA_AUTH_MODE=msk_iam")
		}
		return config, nil
	default:
		return Config{}, fmt.Errorf("unsupported KAFKA_AUTH_MODE %q (want plaintext or msk_iam)", mode)
	}
}

func (c Config) Dialer() *kafka.Dialer {
	if c.Mode != "msk_iam" {
		return nil
	}
	return &kafka.Dialer{Timeout: 10 * time.Second, TLS: tlsConfig(), SASLMechanism: mechanism{region: c.Region, generate: signer.GenerateAuthToken}}
}

func (c Config) Transport() *kafka.Transport {
	if c.Mode != "msk_iam" {
		return nil
	}
	return &kafka.Transport{TLS: tlsConfig(), SASL: mechanism{region: c.Region, generate: signer.GenerateAuthToken}}
}

func tlsConfig() *tls.Config { return &tls.Config{MinVersion: tls.VersionTLS12} }

type tokenGenerator func(context.Context, string) (string, int64, error)

type mechanism struct {
	region   string
	generate tokenGenerator
}

func (mechanism) Name() string { return "OAUTHBEARER" }

func (m mechanism) Start(ctx context.Context) (sasl.StateMachine, []byte, error) {
	token, _, err := m.generate(ctx, m.region)
	if err != nil {
		return nil, nil, fmt.Errorf("generate MSK IAM token: %w", err)
	}
	return oauthSession{}, []byte("n,,\x01auth=Bearer " + token + "\x01\x01"), nil
}

type oauthSession struct{}

func (oauthSession) Next(_ context.Context, challenge []byte) (bool, []byte, error) {
	if len(challenge) != 0 {
		return false, nil, errors.New("MSK IAM SASL authentication rejected")
	}
	return true, nil, nil
}
