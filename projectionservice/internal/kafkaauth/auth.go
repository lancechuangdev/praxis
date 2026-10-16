// Package kafkaauth constructs verified-TLS projection Kafka dialers. IAM
// credential retrieval is deferred until a SASL handshake, never configuration.
package kafkaauth

import (
	"context"
	"crypto/tls"
	"errors"
	"strings"
	"time"

	"github.com/aws/aws-msk-iam-sasl-signer-go/signer"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/scram"
)

type Config struct{ Mode, Region, Username, Password string }

func Dialer(cfg Config) (*kafka.Dialer, error) {
	mode := strings.ToLower(strings.TrimSpace(cfg.Mode))
	if mode == "" {
		mode = "scram_sha512"
	}
	var mechanism sasl.Mechanism
	switch mode {
	case "scram_sha512":
		if cfg.Username == "" || cfg.Password == "" {
			return nil, errors.New("projection Kafka SCRAM credentials are required")
		}
		var err error
		mechanism, err = scram.Mechanism(scram.SHA512, cfg.Username, cfg.Password)
		if err != nil {
			return nil, errors.New("invalid projection Kafka SCRAM credentials")
		}
	case "msk_iam":
		region := strings.TrimSpace(cfg.Region)
		if region == "" {
			return nil, errors.New("AWS_REGION is required for projection MSK IAM")
		}
		mechanism = iamMechanism{region: region, generate: signer.GenerateAuthToken, now: time.Now}
	default:
		return nil, errors.New("PROJECTION_KAFKA_AUTH_MODE must be scram_sha512 or msk_iam")
	}
	return &kafka.Dialer{Timeout: 5 * time.Second, TLS: &tls.Config{MinVersion: tls.VersionTLS12}, SASLMechanism: mechanism}, nil
}

type tokenGenerator func(context.Context, string) (string, int64, error)
type iamMechanism struct {
	region   string
	generate tokenGenerator
	now      func() time.Time
}

func (iamMechanism) Name() string { return "OAUTHBEARER" }

func (m iamMechanism) Start(ctx context.Context) (sasl.StateMachine, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	token, expiry, err := m.generate(ctx, m.region)
	if ctx.Err() != nil {
		return nil, nil, ctx.Err()
	}
	if err != nil {
		return nil, nil, errors.New("projection MSK IAM token generation failed")
	}
	if token == "" || strings.ContainsAny(token, "\x00\x01\r\n") || expiry <= m.now().Add(5*time.Second).UnixMilli() {
		return nil, nil, errors.New("projection MSK IAM signer returned an invalid or expired token")
	}
	return iamSession{}, []byte("n,,\x01auth=Bearer " + token + "\x01\x01"), nil
}

type iamSession struct{}

func (iamSession) Next(ctx context.Context, challenge []byte) (bool, []byte, error) {
	if err := ctx.Err(); err != nil {
		return false, nil, err
	}
	if len(challenge) > 0 {
		return false, nil, errors.New("projection MSK IAM authentication rejected")
	}
	return true, nil, nil
}
