package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"praxis/projectionservice/internal/config"
	"praxis/projectionservice/internal/kafkaconsumer"
	"praxis/projectionservice/internal/mongostore"
	"praxis/projectionservice/internal/runtime"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(ctx, log); err != nil {
		log.Error("projection service stopped", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, log *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	service := &runtime.Service{}
	if cfg.Enabled {
		client, err := mongo.Connect(cfg.Mongo)
		if err != nil {
			return fmt.Errorf("MongoDB client setup failed")
		}
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			_ = client.Disconnect(cleanup)
		}()
		store, err := mongostore.New(client, cfg.Database, cfg.Target, cfg.TransactionTimeout)
		if err != nil {
			return err
		}
		if err := store.Verify(ctx); err != nil {
			return fmt.Errorf("MongoDB storage preflight failed; check endpoint, privileges, collection and index configuration")
		}
		consumer, err := kafkaconsumer.New(cfg.Kafka, store)
		if err != nil {
			return err
		}
		defer func() { _ = consumer.Close() }() // Releases an unused reader if listener setup fails.
		service.Worker = consumer
	}
	listener, err := net.Listen("tcp", cfg.HTTPAddress)
	if err != nil {
		return fmt.Errorf("projection HTTP listener failed")
	}
	log.Info("projection service started", "workflow_enabled", cfg.Enabled, "http", cfg.HTTPAddress)
	return service.Serve(ctx, listener)
}
