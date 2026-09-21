package migrations

import (
	"context"
	"testing"
)

func TestBootstrapRuntimeRolesRejectsInvalidPasswords(t *testing.T) {
	for _, tc := range []struct {
		name, ledger, outbox string
	}{
		{"missing ledger", "", "outbox-secret"},
		{"missing outbox", "ledger-secret", ""},
		{"shared password", "same-secret", "same-secret"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := BootstrapRuntimeRoles(context.Background(), nil, tc.ledger, tc.outbox); err == nil {
				t.Fatal("expected invalid passwords to fail before accessing the database")
			}
		})
	}
}
