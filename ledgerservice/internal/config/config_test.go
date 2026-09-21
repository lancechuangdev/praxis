package config

import (
	"net/url"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestDatabaseStatementDefaults(t *testing.T) {
	t.Setenv("LEDGER_DATABASE_URL", "postgres://test")
	t.Setenv("LEDGER_DB_QUERY_EXEC_MODE", "")
	t.Setenv("LEDGER_DB_STATEMENT_CACHE_CAPACITY", "")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.DBQueryExecMode != pgx.QueryExecModeCacheStatement {
		t.Fatalf("query mode = %v, want cache_statement", c.DBQueryExecMode)
	}
	if c.DBStatementCacheCapacity != 128 {
		t.Fatalf("statement cache capacity = %d, want 128", c.DBStatementCacheCapacity)
	}
}

func TestDatabaseConnectionFromSeparateEnvironment(t *testing.T) {
	t.Setenv("LEDGER_DATABASE_URL", "")
	t.Setenv("LEDGER_DB_HOST", "ledger.example.internal")
	t.Setenv("LEDGER_DB_USER", "ledger_user")
	t.Setenv("LEDGER_DB_PASSWORD", "a@b:c/d")
	t.Setenv("LEDGER_DB_NAME", "cex_ledger")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(c.DatabaseURL)
	if err != nil {
		t.Fatal(err)
	}
	password, _ := u.User.Password()
	if u.Host != "ledger.example.internal:5432" || u.User.Username() != "ledger_user" || password != "a@b:c/d" || u.Path != "/cex_ledger" || u.Query().Get("sslmode") != "require" {
		t.Fatalf("unexpected database connection settings: host=%q user=%q password_matched=%t path=%q sslmode=%q", u.Host, u.User.Username(), password == "a@b:c/d", u.Path, u.Query().Get("sslmode"))
	}
}

func TestDatabaseURLTakesPrecedence(t *testing.T) {
	t.Setenv("LEDGER_DATABASE_URL", "postgres://local/ledger?sslmode=disable")
	t.Setenv("LEDGER_DB_HOST", "ledger.example.internal")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.DatabaseURL != "postgres://local/ledger?sslmode=disable" {
		t.Fatalf("unexpected database URL: %q", c.DatabaseURL)
	}
}

func TestSeparateDatabaseEnvironmentRequiresAllFields(t *testing.T) {
	t.Setenv("LEDGER_DATABASE_URL", "")
	t.Setenv("LEDGER_DB_HOST", "ledger.example.internal")
	t.Setenv("LEDGER_DB_USER", "ledger_user")
	t.Setenv("LEDGER_DB_PASSWORD", "")
	t.Setenv("LEDGER_DB_NAME", "cex_ledger")
	if _, err := Load(); err == nil {
		t.Fatal("expected missing password error")
	}
}

func TestMigrateOnStartup(t *testing.T) {
	t.Setenv("LEDGER_DATABASE_URL", "postgres://test")
	t.Setenv("LEDGER_MIGRATE_ON_STARTUP", "")
	c, err := Load()
	if err != nil || !c.MigrateOnStartup {
		t.Fatalf("default migration setting = %t, %v", c.MigrateOnStartup, err)
	}
	t.Setenv("LEDGER_MIGRATE_ON_STARTUP", "false")
	c, err = Load()
	if err != nil || c.MigrateOnStartup {
		t.Fatalf("disabled migration setting = %t, %v", c.MigrateOnStartup, err)
	}
	t.Setenv("LEDGER_MIGRATE_ON_STARTUP", "invalid")
	if _, err := Load(); err == nil {
		t.Fatal("expected invalid migration setting to fail")
	}
}

func TestDatabaseQueryExecModes(t *testing.T) {
	for raw, want := range map[string]pgx.QueryExecMode{
		"cache_statement": pgx.QueryExecModeCacheStatement,
		"cache_describe":  pgx.QueryExecModeCacheDescribe,
		"describe_exec":   pgx.QueryExecModeDescribeExec,
		"exec":            pgx.QueryExecModeExec,
		"simple_protocol": pgx.QueryExecModeSimpleProtocol,
	} {
		t.Run(raw, func(t *testing.T) {
			t.Setenv("LEDGER_DATABASE_URL", "postgres://test")
			t.Setenv("LEDGER_DB_QUERY_EXEC_MODE", raw)
			c, err := Load()
			if err != nil {
				t.Fatal(err)
			}
			if c.DBQueryExecMode != want {
				t.Fatalf("query mode = %v, want %v", c.DBQueryExecMode, want)
			}
		})
	}
}

func TestInvalidDatabaseStatementConfiguration(t *testing.T) {
	for name, value := range map[string]string{
		"LEDGER_DB_QUERY_EXEC_MODE":          "unknown",
		"LEDGER_DB_STATEMENT_CACHE_CAPACITY": "0",
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("LEDGER_DATABASE_URL", "postgres://test")
			t.Setenv(name, value)
			if _, err := Load(); err == nil {
				t.Fatalf("Load() accepted %s=%q", name, value)
			}
		})
	}
}
