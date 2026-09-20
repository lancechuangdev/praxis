package config

import (
	"net/url"
	"testing"
)

func TestDatabaseConnectionFromSeparateEnvironment(t *testing.T) {
	t.Setenv("OUTBOX_DATABASE_URL", "")
	t.Setenv("OUTBOX_DB_HOST", "ledger.example.internal")
	t.Setenv("OUTBOX_DB_USER", "relay_user")
	t.Setenv("OUTBOX_DB_PASSWORD", "a@b:c/d")
	t.Setenv("OUTBOX_DB_NAME", "cex_ledger")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(c.DatabaseURL)
	if err != nil {
		t.Fatal(err)
	}
	password, _ := u.User.Password()
	if u.Host != "ledger.example.internal:5432" || u.User.Username() != "relay_user" || password != "a@b:c/d" || u.Path != "/cex_ledger" || u.Query().Get("sslmode") != "require" {
		t.Fatalf("unexpected database connection settings: host=%q user=%q password_matched=%t path=%q sslmode=%q", u.Host, u.User.Username(), password == "a@b:c/d", u.Path, u.Query().Get("sslmode"))
	}
}

func TestDatabaseURLTakesPrecedence(t *testing.T) {
	t.Setenv("OUTBOX_DATABASE_URL", "postgres://local/ledger?sslmode=disable")
	t.Setenv("OUTBOX_DB_HOST", "ledger.example.internal")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.DatabaseURL != "postgres://local/ledger?sslmode=disable" {
		t.Fatalf("unexpected database URL: %q", c.DatabaseURL)
	}
}

func TestSeparateDatabaseEnvironmentRequiresAllFields(t *testing.T) {
	t.Setenv("OUTBOX_DATABASE_URL", "")
	t.Setenv("OUTBOX_DB_HOST", "ledger.example.internal")
	t.Setenv("OUTBOX_DB_USER", "relay_user")
	t.Setenv("OUTBOX_DB_PASSWORD", "")
	t.Setenv("OUTBOX_DB_NAME", "cex_ledger")
	if _, err := Load(); err == nil {
		t.Fatal("expected missing password error")
	}
}

func TestParseTopicMap(t *testing.T) {
	cases := []struct {
		input   string
		want    map[string]string
		wantErr bool
	}{
		{input: ""},
		{input: "ledger-events=ledger.events.v1", want: map[string]string{"ledger-events": "ledger.events.v1"}},
		{input: "old=a, other=b", want: map[string]string{"old": "a", "other": "b"}},
		{input: "old", wantErr: true},
		{input: "=new", wantErr: true},
		{input: "old=", wantErr: true},
		{input: "old=new,old=other", wantErr: true},
		{input: "old=new,", wantErr: true},
		{input: "old=new=extra", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			got, err := parseTopicMap(tc.input)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error=%v, wantErr=%v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if len(got) != len(tc.want) {
				t.Fatalf("map=%v, want=%v", got, tc.want)
			}
			for source, destination := range tc.want {
				if got[source] != destination {
					t.Fatalf("map=%v, want=%v", got, tc.want)
				}
			}
		})
	}
}
