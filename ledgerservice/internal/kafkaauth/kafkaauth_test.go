package kafkaauth

import (
	"context"
	"errors"
	"testing"
)

func TestLoad(t *testing.T) {
	t.Setenv("KAFKA_AUTH_MODE", "")
	t.Setenv("AWS_REGION", "")
	config, err := Load()
	if err != nil || config.Mode != "plaintext" || config.Dialer() != nil || config.Transport() != nil {
		t.Fatalf("plaintext defaults: config=%+v err=%v", config, err)
	}
	t.Setenv("KAFKA_AUTH_MODE", "msk_iam")
	if _, err := Load(); err == nil {
		t.Fatal("expected missing region error")
	}
	t.Setenv("AWS_REGION", "us-west-2")
	config, err = Load()
	if err != nil || config.Dialer() == nil || config.Transport() == nil {
		t.Fatalf("IAM config: config=%+v err=%v", config, err)
	}
	if config.Transport().TLS.ServerName != "" || config.Transport().TLS.InsecureSkipVerify {
		t.Fatal("TLS hostname verification must remain enabled")
	}
	t.Setenv("KAFKA_AUTH_MODE", "unknown")
	if _, err := Load(); err == nil {
		t.Fatal("expected unknown mode error")
	}
}

func TestMechanism(t *testing.T) {
	m := mechanism{region: "us-west-2", generate: func(_ context.Context, region string) (string, int64, error) {
		if region != "us-west-2" {
			t.Fatalf("unexpected region %q", region)
		}
		return "signed-token", 0, nil
	}}
	if m.Name() != "OAUTHBEARER" {
		t.Fatalf("unexpected mechanism %q", m.Name())
	}
	session, initial, err := m.Start(context.Background())
	if err != nil || string(initial) != "n,,\x01auth=Bearer signed-token\x01\x01" {
		t.Fatalf("initial response=%q err=%v", initial, err)
	}
	if done, _, err := session.Next(context.Background(), nil); !done || err != nil {
		t.Fatalf("success: done=%v err=%v", done, err)
	}
	if done, _, err := session.Next(context.Background(), []byte("rejected")); done || err == nil {
		t.Fatalf("rejection: done=%v err=%v", done, err)
	}
	m.generate = func(context.Context, string) (string, int64, error) { return "", 0, errors.New("no credentials") }
	if _, _, err := m.Start(context.Background()); err == nil {
		t.Fatal("expected token generation error")
	}
}
