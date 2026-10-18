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
	"praxis/projectionservice/internal/journal"
	"praxis/projectionservice/internal/kafkaconsumer"
	"praxis/projectionservice/internal/mongostore"
	"praxis/projectionservice/internal/runtime"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	mode := "serve"
	if len(os.Args) > 1 {
		if len(os.Args) != 2 || os.Args[1] != "prepare" {
			fmt.Fprintln(os.Stderr, "usage: projection-service [prepare]")
			os.Exit(2)
		}
		mode = "prepare"
	}
	if err := runMode(ctx, log, mode); err != nil {
		log.Error("projection service stopped", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, log *slog.Logger) error {
	return runMode(ctx, log, "serve")
}
func runMode(ctx context.Context, log *slog.Logger, mode string) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if mode != "serve" && mode != "prepare" {
		return fmt.Errorf("unsupported projection command")
	}
	if mode == "prepare" && !cfg.Enabled {
		return fmt.Errorf("prepare requires an enabled, explicitly configured target")
	}
	service := &runtime.Service{}
	service.Stream = cfg.Stream
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
		var consumer *kafkaconsumer.Consumer
		if cfg.Stream == "journal" {
			store, setupErr := mongostore.NewJournal(client, cfg.Database, journal.Target{Generation: cfg.Target.Generation, Mode: journal.Mode(cfg.Target.Mode)}, cfg.TransactionTimeout)
			if setupErr != nil {
				return setupErr
			}
			if mode == "prepare" {
				if err := store.Prepare(ctx); err != nil {
					return fmt.Errorf("MongoDB journal target preparation failed")
				}
				log.Info("projection target prepared", "stream", cfg.Stream, "generation", cfg.Target.Generation, "mode", cfg.Target.Mode)
				return nil
			}
			if err := store.Verify(ctx); err != nil {
				return fmt.Errorf("MongoDB journal storage preflight failed; check topology, permissions, collections and indexes")
			}
			consumer, err = kafkaconsumer.NewJournal(cfg.Kafka, store)
		} else {
			store, setupErr := mongostore.New(client, cfg.Database, cfg.Target, cfg.TransactionTimeout)
			if setupErr != nil {
				return setupErr
			}
			if mode == "prepare" {
				if err := store.Prepare(ctx); err != nil {
					return fmt.Errorf("MongoDB workflow target preparation failed")
				}
				log.Info("projection target prepared", "stream", cfg.Stream, "generation", cfg.Target.Generation, "mode", cfg.Target.Mode)
				return nil
			}
			if err := store.Verify(ctx); err != nil {
				return fmt.Errorf("MongoDB storage preflight failed; check endpoint, privileges, collection and index configuration")
			}
			consumer, err = kafkaconsumer.New(cfg.Kafka, store)
		}
		if err != nil {
			return err
		}
		defer func() { _ = consumer.Close() }() // Releases an unused reader if listener setup fails.
		service.Worker = consumer
		if cfg.LagObserve {
			observer, err := kafkaconsumer.NewLagObserver(cfg.Kafka)
			if err != nil {
				return err
			}
			defer observer.Close()
			service.LagMetrics = observer.Prometheus
		}
	}
	listener, err := net.Listen("tcp", cfg.HTTPAddress)
	if err != nil {
		return fmt.Errorf("projection HTTP listener failed")
	}
	log.Info("projection service started", "enabled", cfg.Enabled, "stream", cfg.Stream, "http", cfg.HTTPAddress)
	return service.Serve(ctx, listener)
}
