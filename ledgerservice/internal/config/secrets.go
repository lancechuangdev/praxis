package config

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

type secretReader interface {
	GetSecretValue(context.Context, *secretsmanager.GetSecretValueInput, ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error)
}

func databasePassword() (string, error) {
	secretARN := os.Getenv("LEDGER_DB_SECRET_ARN")
	plainPassword := os.Getenv("LEDGER_DB_PASSWORD")
	if secretARN == "" {
		if plainPassword == "" {
			return "", errors.New("set LEDGER_DB_SECRET_ARN or LEDGER_DB_PASSWORD")
		}
		return plainPassword, nil
	}
	if plainPassword != "" {
		return "", errors.New("set only one of LEDGER_DB_SECRET_ARN and LEDGER_DB_PASSWORD")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	awsConfig, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return "", fmt.Errorf("load AWS credentials for Ledger database secret: %w", err)
	}
	return readSecretPassword(ctx, secretsmanager.NewFromConfig(awsConfig), secretARN)
}

func readSecretPassword(ctx context.Context, reader secretReader, secretARN string) (string, error) {
	result, err := reader.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: &secretARN})
	if err != nil {
		return "", fmt.Errorf("read Ledger database secret: %w", err)
	}
	if result == nil || result.SecretString == nil {
		return "", errors.New("Ledger database secret has no SecretString")
	}
	var payload struct {
		Password string `json:"password"`
	}
	if err := json.Unmarshal([]byte(*result.SecretString), &payload); err != nil || payload.Password == "" {
		return "", errors.New("Ledger database secret must contain a non-empty JSON password")
	}
	return payload.Password, nil
}
