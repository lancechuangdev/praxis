package main

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	"praxis/orderservice/internal/adapters"
	"praxis/orderservice/internal/config"
	"praxis/orderservice/internal/kafkaauth"
	"praxis/orderservice/internal/order"
)

func startWorkflowDispatch(ctx context.Context, cfg config.Config) (*order.WorkflowWorker, func(), error) {
	if !cfg.WorkflowDispatch {
		return nil, func() {}, nil
	}
	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return nil, nil, err
	}
	poolCfg.MaxConns, poolCfg.MinConns, poolCfg.MinIdleConns = 1, 0, 0
	poolCfg.ConnConfig.ConnectTimeout = 5 * time.Second
	poolCfg.ConnConfig.RuntimeParams["statement_timeout"] = "5000ms"
	db, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, nil, err
	}
	dialer, err := kafkaauth.Dialer(cfg.KafkaAuth)
	if err != nil {
		db.Close()
		return nil, nil, err
	}
	transport := &kafka.Transport{TLS: dialer.TLS, SASL: dialer.SASLMechanism, DialTimeout: 3 * time.Second}
	publisher, err := adapters.NewWorkflowKafkaPublisher(adapters.WorkflowKafkaConfig{Brokers: cfg.KafkaBrokers, Transport: transport, SendTimeout: 3 * time.Second})
	if err != nil {
		transport.CloseIdleConnections()
		db.Close()
		return nil, nil, err
	}
	worker, err := order.NewWorkflowWorker(order.WorkflowDispatcher{DB: db, Publisher: publisher, Timeout: 5 * time.Second}, 1, 250*time.Millisecond, time.Second)
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
	return worker, func() {
		once.Do(func() { cancel(); <-done; _ = publisher.Close(); transport.CloseIdleConnections(); db.Close() })
	}, nil
}
