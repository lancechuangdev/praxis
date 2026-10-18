package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"praxis/ledgerservice/internal/config"
	"praxis/ledgerservice/internal/kafkaauth"
)

func TestJournalDispatchDisabledHasNoDependencies(t *testing.T) {
	worker, stop, err := startJournalDispatch(context.Background(), config.Config{}, nil)
	if err != nil || worker != nil || stop == nil {
		t.Fatalf("worker=%v stop nil=%v err=%v", worker, stop == nil, err)
	}
	stop()
}

func TestJournalDispatchStopJoinsAndIsIdempotent(t *testing.T) {
	pool, err := pgxpool.ParseConfig("postgres://unused:unused@127.0.0.1:1/unused?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{JournalDispatch: true, KafkaBrokers: []string{"127.0.0.1:1"}, KafkaAuth: kafkaauth.Config{Mode: "msk_iam", Region: "us-east-1"}}
	worker, stop, err := startJournalDispatch(context.Background(), cfg, pool)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	done := make(chan struct{})
	go func() { stop(); stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stop did not join worker")
	}
	if !strings.Contains(worker.Prometheus(), "ledger_journal_dispatch_active 0\n") {
		t.Fatal("worker still active")
	}
	if pool.MaxConns == 1 {
		t.Fatal("changed financial writer pool configuration")
	}
}
