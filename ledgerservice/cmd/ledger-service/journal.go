package main

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"praxis/ledgerservice/internal/config"
	"praxis/ledgerservice/internal/messaging"
	"praxis/ledgerservice/internal/store"
)

// startJournalDispatch owns an additional one-connection writer pool. Stop
// cancels and joins dispatch before closing its publisher, transport and pool.
// Disabled operation creates no resources and does not probe Kafka or storage.
func startJournalDispatch(ctx context.Context, cfg config.Config, writer *pgxpool.Config) (*store.JournalWorker, func(), error) {
	if !cfg.JournalDispatch {
		return nil, func() {}, nil
	}
	poolCfg := writer.Copy()
	poolCfg.MaxConns = 1
	poolCfg.MinConns = 0
	poolCfg.ConnConfig.RuntimeParams["praxis.ledger_lifecycle_capture"] = "off"
	db, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, nil, err
	}
	transport := cfg.KafkaAuth.Transport()
	publisher, err := messaging.NewJournalKafkaPublisher(messaging.JournalKafkaConfig{Brokers: cfg.KafkaBrokers, Transport: transport, SendTimeout: 3 * time.Second})
	if err != nil {
		if transport != nil {
			transport.CloseIdleConnections()
		}
		db.Close()
		return nil, nil, err
	}
	worker, err := store.NewJournalWorker(store.JournalDispatcher{DB: db, Publisher: publisher, Timeout: 5 * time.Second}, 1, 250*time.Millisecond, time.Second)
	if err != nil {
		_ = publisher.Close()
		transport.CloseIdleConnections()
		db.Close()
		return nil, nil, err
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); _ = worker.Run(runCtx) }()
	var once sync.Once
	stop := func() {
		once.Do(func() { cancel(); <-done; _ = publisher.Close(); transport.CloseIdleConnections(); db.Close() })
	}
	return worker, stop, nil
}
