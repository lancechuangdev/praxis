package config

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

type fakeSecretReader struct {
	value string
	err   error
	arn   string
}

func (f *fakeSecretReader) GetSecretValue(_ context.Context, input *secretsmanager.GetSecretValueInput, _ ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error) {
	f.arn = *input.SecretId
	if f.err != nil {
		return nil, f.err
	}
	return &secretsmanager.GetSecretValueOutput{SecretString: &f.value}, nil
}

func TestReadSecretPassword(t *testing.T) {
	reader := &fakeSecretReader{value: `{"password":"a@b:c/d"}`}
	password, err := readSecretPassword(context.Background(), reader, "arn:aws:secretsmanager:us-west-2:123456789012:secret:ledger")
	if err != nil || password != "a@b:c/d" || !strings.Contains(reader.arn, ":ledger") {
		t.Fatalf("password matched=%t, arn=%q, err=%v", password == "a@b:c/d", reader.arn, err)
	}
}

func TestReadSecretPasswordRejectsMissingOrInvalidValue(t *testing.T) {
	for _, value := range []string{`{}`, `not-json`, `{"password":""}`} {
		reader := &fakeSecretReader{value: value}
		if _, err := readSecretPassword(context.Background(), reader, "secret"); err == nil {
			t.Fatalf("expected error for %q", value)
		}
	}
	reader := &fakeSecretReader{err: errors.New("unavailable")}
	if _, err := readSecretPassword(context.Background(), reader, "secret"); err == nil {
		t.Fatal("expected AWS error")
	}
}

func TestDatabasePasswordLocalFallbackAndConflict(t *testing.T) {
	t.Setenv("LEDGER_DB_SECRET_ARN", "")
	t.Setenv("LEDGER_DB_PASSWORD", "local-password")
	if password, err := databasePassword(); err != nil || password != "local-password" {
		t.Fatalf("local fallback: %q, %v", password, err)
	}
	t.Setenv("LEDGER_DB_SECRET_ARN", "secret")
	if _, err := databasePassword(); err == nil {
		t.Fatal("expected conflicting password sources to fail")
	}
}
