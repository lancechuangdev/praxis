package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	ledgerv1 "praxis/ledgerservice/gen/ledger/v1"
	"praxis/ledgerservice/internal/config"
	"praxis/ledgerservice/internal/messaging"
	"praxis/ledgerservice/internal/store"
	"praxis/ledgerservice/internal/telemetry"
	"praxis/ledgerservice/internal/transport"
	"praxis/ledgerservice/migrations"
)

func main() {
	if len(os.Args) == 2 && os.Args[1] == "healthcheck" {
		if err := checkReady(os.Getenv("LEDGER_HTTP_ADDRESS")); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load()
	if err != nil {
		log.Error("configuration", "error", err)
		os.Exit(1)
	}
	telemetryShutdown, err := telemetry.Setup(ctx, "ledger-service")
	if err != nil {
		log.Error("telemetry", "error", err)
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if shutdownErr := telemetryShutdown(shutdownCtx); shutdownErr != nil {
			log.Error("telemetry shutdown", "error", shutdownErr)
		}
	}()
	poolConfig, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		log.Error("database configuration", "error", err)
		os.Exit(1)
	}
	poolConfig.MaxConns = cfg.DBMaxConns
	poolConfig.ConnConfig.StatementCacheCapacity = cfg.DBStatementCacheCapacity
	poolConfig.ConnConfig.DefaultQueryExecMode = cfg.DBQueryExecMode
	db, err := pgxpool.NewWithConfig(ctx, poolConfig)
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
	grpcServer := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))
	ledgerv1.RegisterLedgerServiceServer(grpcServer, &transport.GRPC{Store: repo, Metrics: metrics})
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	httpServer := &http.Server{Addr: cfg.HTTPAddress, Handler: otelhttp.NewHandler(transport.HTTP{Store: repo, DB: db, Metrics: metrics}.Handler(), "ledger.http"), ReadHeaderTimeout: 5 * time.Second}
	consumer := messaging.NewConsumer(cfg.KafkaBrokers, cfg.CommandsTopic, cfg.ConsumerGroup, cfg.KafkaAuth, db, repo, log)
	errCh := make(chan error, 3)
	go func() { errCh <- grpcServer.Serve(listener) }()
	go func() { errCh <- httpServer.ListenAndServe() }()
	go func() { errCh <- consumer.Run(ctx) }()
	log.Info("ledger service started", "http", cfg.HTTPAddress, "grpc", cfg.GRPCAddress, "topic", cfg.CommandsTopic, "db_max_conns", cfg.DBMaxConns, "db_query_exec_mode", cfg.DBQueryExecMode.String(), "db_statement_cache_capacity", cfg.DBStatementCacheCapacity)
	select {
	case <-ctx.Done():
	case err = <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("service stopped", "error", err)
		}
	}
	stop()
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	httpDone := make(chan struct{})
	go func() {
		_ = httpServer.Shutdown(shutdownCtx)
		close(httpDone)
	}()
	grpcDone := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(grpcDone)
	}()
	select {
	case <-grpcDone:
	case <-shutdownCtx.Done():
		grpcServer.Stop()
	}
	<-httpDone
	_ = consumer.Close()
}

func checkReady(address string) error {
	if address == "" {
		address = ":8081"
	}
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("healthcheck address: %w", err)
	}
	client := &http.Client{Timeout: 3 * time.Second}
	response, err := client.Get("http://127.0.0.1:" + port + "/readyz")
	if err != nil {
		return fmt.Errorf("healthcheck request: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("healthcheck status: %s", response.Status)
	}
	return nil
}
