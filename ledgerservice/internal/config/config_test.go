package config

import (
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
