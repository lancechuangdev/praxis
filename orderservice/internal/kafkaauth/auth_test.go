package kafkaauth

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-msk-iam-sasl-signer-go/signer"
	"github.com/aws/aws-sdk-go-v2/aws"
)

func TestDialerModesAndTLS(t *testing.T) {
	for _, cfg := range []Config{{Username: "user", Password: "secret"}, {Mode: "scram_sha512", Username: "user", Password: "secret"}, {Mode: "msk_iam", Region: "us-west-2"}} {
		dialer, err := Dialer(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if dialer.TLS == nil || dialer.TLS.InsecureSkipVerify || dialer.TLS.MinVersion < tls.VersionTLS12 || dialer.Timeout > 5*time.Second || dialer.TLS.ServerName != "" {
			t.Fatal("unsafe Kafka dialer")
		}
		want := "SCRAM-SHA-512"
		if cfg.Mode == "msk_iam" {
			want = "OAUTHBEARER"
		}
		if dialer.SASLMechanism.Name() != want {
			t.Fatal("wrong mechanism")
		}
	}
	for _, cfg := range []Config{{}, {Mode: "msk_iam"}, {Mode: "plaintext"}, {Mode: "secret"}} {
		if _, err := Dialer(cfg); err == nil || strings.Contains(err.Error(), "secret") {
			t.Fatal("unsafe configuration or error disclosure")
		}
	}
}

func TestIAMSignerWireCompatibilityWithoutNetwork(t *testing.T) {
	m := iamMechanism{region: "us-west-2", now: time.Now, generate: func(ctx context.Context, region string) (string, int64, error) {
		return signer.GenerateAuthTokenFromCredentialsProvider(ctx, region, aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "AKIDEXAMPLE", SecretAccessKey: "test-only-secret", SessionToken: "test-only-session"}, nil
		}))
	}}
	_, initial, err := m.Start(context.Background())
	if err != nil {
		t.Fatal("local signer handshake failed")
	}
	token := strings.TrimSuffix(strings.TrimPrefix(string(initial), "n,,\x01auth=Bearer "), "\x01\x01")
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		t.Fatal("signer did not return URL-safe token")
	}
	uri, err := url.Parse(string(decoded))
	if err != nil {
		t.Fatal("invalid locally signed URL")
	}
	if uri.Host != "kafka.us-west-2.amazonaws.com" || uri.Scheme != "https" || uri.Query().Get("X-Amz-Signature") == "" {
		t.Fatal("signer region or signature missing")
	}
}

func TestIAMFreshTokensAndBoundedHandshake(t *testing.T) {
	now := time.Unix(1700000000, 0)
	calls := 0
	m := iamMechanism{region: "us-west-2", now: func() time.Time { return now }, generate: func(ctx context.Context, region string) (string, int64, error) {
		calls++
		if region != "us-west-2" {
			t.Error("wrong region")
		}
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > 5*time.Second {
			t.Error("unbounded signing")
		}
		return fmt.Sprintf("signed-token-%d", calls), now.Add(time.Minute).UnixMilli(), nil
	}}
	for call := 1; call <= 2; call++ {
		session, initial, err := m.Start(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if string(initial) != fmt.Sprintf("n,,\x01auth=Bearer signed-token-%d\x01\x01", call) {
			t.Fatal("unexpected OAuth frame or reused token")
		}
		if done, response, err := session.Next(context.Background(), nil); !done || len(response) != 0 || err != nil {
			t.Fatal("successful handshake rejected")
		}
		if done, _, err := session.Next(context.Background(), []byte("sensitive broker response")); done || err == nil || strings.Contains(err.Error(), "sensitive") {
			t.Fatal("broker rejection accepted or leaked")
		}
	}
}

func TestIAMFailureAndCancellation(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		token  string
		expiry int64
		err    error
	}{
		{"", now.Add(time.Minute).UnixMilli(), nil},
		{"signed", now.Add(-time.Second).UnixMilli(), nil},
		{"signed", now.Add(time.Second).UnixMilli(), nil},
		{"signed\x01injection", now.Add(time.Minute).UnixMilli(), nil},
		{"sensitive-token", 0, errors.New("sensitive credentials")},
	} {
		m := iamMechanism{region: "us-west-2", now: func() time.Time { return now }, generate: func(context.Context, string) (string, int64, error) { return tc.token, tc.expiry, tc.err }}
		if session, initial, err := m.Start(context.Background()); err == nil || session != nil || len(initial) != 0 || strings.Contains(err.Error(), "sensitive") {
			t.Fatal("invalid signer result accepted or disclosed")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	m := iamMechanism{generate: func(context.Context, string) (string, int64, error) {
		t.Fatal("cancelled signer called")
		return "", 0, nil
	}}
	if _, _, err := m.Start(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, _, err := (iamSession{}).Next(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	m = iamMechanism{generate: func(ctx context.Context, _ string) (string, int64, error) {
		cancel()
		return "token", now.Add(time.Minute).UnixMilli(), nil
	}}
	if _, _, err := m.Start(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
