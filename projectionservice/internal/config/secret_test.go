package config

import "testing"

func TestPrivateSecretsEndpointValidation(t *testing.T) {
	if endpoint, err := secretsEndpoint(""); err != nil || endpoint != nil {
		t.Fatal(endpoint, err)
	}
	for _, raw := range []string{"http://vpce-test.vpce.amazonaws.com", "https://external.example", "https://user:password@vpce-test.vpce.amazonaws.com", "https://vpce-test.vpce.amazonaws.com/path", "https://vpce-test.vpce.amazonaws.com?token=secret"} {
		if _, err := secretsEndpoint(raw); err == nil {
			t.Fatal("unsafe secret endpoint accepted")
		}
	}
	if _, err := secretsEndpoint("https://vpce-test.secretsmanager.us-east-1.vpce.amazonaws.com"); err != nil {
		t.Fatal(err)
	}
}
