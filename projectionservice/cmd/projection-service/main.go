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
	"praxis/projectionservice/internal/readapi"
	"praxis/projectionservice/internal/runtime"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cmd, err := parseCommand(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if err := runCommand(ctx, log, cmd); err != nil {
		log.Error("projection service stopped", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, log *slog.Logger) error {
	return runMode(ctx, log, "serve")
}
func runMode(ctx context.Context, log *slog.Logger, mode string) error {
	return runCommand(ctx, log, command{Mode: mode})
}
func runCommand(ctx context.Context, log *slog.Logger, cmd command) error {
	mode := cmd.Mode
	var cfg config.Config
	var err error
	if mode == "serve" {
		cfg, err = config.Load()
	} else {
		cfg, err = config.LoadAdmin()
	}
	if err != nil {
		return err
	}
	if mode != "serve" && mode != "prepare" && mode != "drain" && mode != "retry" && mode != "replay" && mode != "reconcile" && mode != "activate" && mode != "inspect" {
		return fmt.Errorf("unsupported projection command")
	}
	if mode != "serve" && !cfg.Enabled {
		return fmt.Errorf("admin command requires an enabled, explicitly configured target")
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
		worker, closeWorker, err := configure(ctx, cfg, client, mode == "prepare", mode == "serve")
		if err != nil {
			return err
		}
		defer closeWorker()
		if mode == "prepare" {
			log.Info("projection target prepared", "stream", cfg.Stream, "generation", cfg.Target.Generation, "mode", cfg.Target.Mode)
			return nil
		}
		if mode != "serve" {
			return runAdmin(ctx, cmd, cfg, client, worker.(*runtime.Queued))
		}
		service.Worker = worker
		service.QueueMetrics = worker.(*runtime.Queued).Queue.Prometheus
		if cfg.ReadEnabled {
			reads, err := mongostore.NewReads(client, cfg.Database, cfg.Target, cfg.Stream, cfg.TransactionTimeout)
			if err != nil {
				return err
			}
			if err := reads.Verify(ctx); err != nil {
				return fmt.Errorf("projection read index preflight failed")
			}
			service.Reads, err = readapi.New(cfg.ReadAuth, reads)
			if err != nil {
				return err
			}
		}
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
