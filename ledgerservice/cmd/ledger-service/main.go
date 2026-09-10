package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	ledgerv1 "praxis/ledgerservice/gen/ledger/v1"
	"praxis/ledgerservice/internal/config"
	"praxis/ledgerservice/internal/messaging"
	"praxis/ledgerservice/internal/store"
	"praxis/ledgerservice/internal/transport"
	"praxis/ledgerservice/migrations"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load()
	if err != nil {
		log.Error("configuration", "error", err)
		os.Exit(1)
	}
	db, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("database", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	if err = migrations.Apply(ctx, db); err != nil {
		log.Error("migrations", "error", err)
		os.Exit(1)
	}
	repo := store.New(db)
	metrics := &transport.Metrics{}
	listener, err := net.Listen("tcp", cfg.GRPCAddress)
	if err != nil {
		log.Error("grpc listen", "error", err)
		os.Exit(1)
	}
	grpcServer := grpc.NewServer()
	ledgerv1.RegisterLedgerServiceServer(grpcServer, &transport.GRPC{Store: repo, Metrics: metrics})
	httpServer := &http.Server{Addr: cfg.HTTPAddress, Handler: transport.HTTP{Store: repo, DB: db, Metrics: metrics}.Handler(), ReadHeaderTimeout: 5 * time.Second}
	consumer := messaging.NewConsumer(cfg.KafkaBrokers, cfg.CommandsTopic, cfg.ConsumerGroup, db, repo, log)
	errCh := make(chan error, 3)
	go func() { errCh <- grpcServer.Serve(listener) }()
	go func() { errCh <- httpServer.ListenAndServe() }()
	go func() { errCh <- consumer.Run(ctx) }()
	log.Info("ledger service started", "http", cfg.HTTPAddress, "grpc", cfg.GRPCAddress, "topic", cfg.CommandsTopic)
	select {
	case <-ctx.Done():
	case err = <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("service stopped", "error", err)
		}
	}
	stop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	grpcServer.GracefulStop()
	_ = httpServer.Shutdown(shutdownCtx)
	_ = consumer.Close()
}
