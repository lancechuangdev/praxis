package config

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"strings"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

func mongoURI() (string, error) {
	uri, arn := os.Getenv("PROJECTION_MONGODB_URI"), os.Getenv("PROJECTION_MONGODB_URI_SECRET_ARN")
	if uri != "" && arn != "" {
		return "", errors.New("configure one MongoDB URI source")
	}
	if arn == "" {
		return uri, nil
	}
	endpoint, err := secretsEndpoint(os.Getenv("PROJECTION_SECRETS_MANAGER_ENDPOINT"))
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return "", errors.New("MongoDB credential identity configuration failed")
	}
	response, err := secretsmanager.NewFromConfig(cfg, func(o *secretsmanager.Options) {
		if endpoint != nil {
			o.BaseEndpoint = endpoint
		}
	}).GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: &arn})
	if err != nil || response.SecretString == nil || len(*response.SecretString) > 65536 {
		return "", errors.New("MongoDB URI secret retrieval failed")
	}
	var secret struct {
		URI string `json:"uri"`
	}
	if err := json.Unmarshal([]byte(*response.SecretString), &secret); err != nil || secret.URI == "" {
		return "", errors.New("MongoDB URI secret must contain uri")
	}
	return secret.URI, nil
}

func secretsEndpoint(raw string) (*string, error) {
	if raw == "" {
		return nil, nil
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || (u.Port() != "" && u.Port() != "443") || !strings.HasPrefix(u.Hostname(), "vpce-") || !strings.HasSuffix(u.Hostname(), ".vpce.amazonaws.com") {
		return nil, errors.New("Secrets Manager override must be an HTTPS AWS private endpoint")
	}
	return &raw, nil
}
