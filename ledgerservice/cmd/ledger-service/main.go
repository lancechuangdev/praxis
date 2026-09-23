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
	"praxis/ledgerservice/internal/observability"
	"praxis/ledgerservice/internal/store"
	"praxis/ledgerservice/internal/transport"
	"praxis/ledgerservice/migrations"
)

func main() {
	mode, err := commandMode(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if mode == "healthcheck" {
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
	tracingShutdown, err := observability.SetupTracing(ctx, "ledger-service")
	if err != nil {
		log.Error("tracing setup", "error", err)
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if shutdownErr := tracingShutdown(shutdownCtx); shutdownErr != nil {
			log.Error("tracing shutdown", "error", shutdownErr)
		}
	}()
	writerPoolConfig, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		log.Error("database configuration", "error", err)
		os.Exit(1)
	}
	writerPoolConfig.MaxConns = cfg.DBWriterMaxConns
	writerPoolConfig.ConnConfig.StatementCacheCapacity = cfg.DBStatementCacheCapacity
	writerPoolConfig.ConnConfig.DefaultQueryExecMode = cfg.DBQueryExecMode
	writerDB, err := pgxpool.NewWithConfig(ctx, writerPoolConfig)
	if err != nil {
		log.Error("database", "error", err)
		os.Exit(1)
	}
	defer writerDB.Close()
	readerPoolConfig, err := pgxpool.ParseConfig(cfg.ReaderDatabaseURL)
	if err != nil {
		log.Error("reader database configuration", "error", err)
		os.Exit(1)
	}
	readerPoolConfig.MaxConns = cfg.DBReaderMaxConns
	readerPoolConfig.ConnConfig.StatementCacheCapacity = cfg.DBStatementCacheCapacity
	readerPoolConfig.ConnConfig.DefaultQueryExecMode = cfg.DBQueryExecMode
	readerPoolConfig.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
	readerDB, err := pgxpool.NewWithConfig(ctx, readerPoolConfig)
	if err != nil {
		log.Error("reader database", "error", err)
		os.Exit(1)
	}
	defer readerDB.Close()
	if mode == "migrate" || cfg.MigrateOnStartup {
		err = migrations.Apply(ctx, writerDB)
	} else {
		err = migrations.Verify(ctx, writerDB)
	}
	if err != nil {
		log.Error("schema migrations", "error", err)
		os.Exit(1)
	}
	if mode == "migrate" {
		log.Info("ledger migrations applied")
		return
	}
	repo := store.New(writerDB, readerDB)
	metrics := &observability.Metrics{}
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
	httpServer := &http.Server{Addr: cfg.HTTPAddress, Handler: otelhttp.NewHandler(transport.HTTP{Store: repo, WriterDB: writerDB, ReaderDB: readerDB, Metrics: metrics}.Handler(), "ledger.http"), ReadHeaderTimeout: 5 * time.Second}
	consumer := messaging.NewConsumer(cfg.KafkaBrokers, cfg.CommandsTopic, cfg.ConsumerGroup, cfg.KafkaAuth, writerDB, repo, log)
	errCh := make(chan error, 3)
	go func() { errCh <- grpcServer.Serve(listener) }()
	go func() { errCh <- httpServer.ListenAndServe() }()
	go func() { errCh <- consumer.Run(ctx) }()
	log.Info("ledger service started", "http", cfg.HTTPAddress, "grpc", cfg.GRPCAddress, "topic", cfg.CommandsTopic, "db_writer_max_conns", cfg.DBWriterMaxConns, "db_reader_max_conns", cfg.DBReaderMaxConns, "db_query_exec_mode", cfg.DBQueryExecMode.String(), "db_statement_cache_capacity", cfg.DBStatementCacheCapacity)
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

func commandMode(args []string) (string, error) {
	if len(args) == 0 {
		return "serve", nil
	}
	if len(args) == 1 && (args[0] == "healthcheck" || args[0] == "migrate") {
		return args[0], nil
	}
	return "", fmt.Errorf("usage: ledger-service [healthcheck|migrate]")
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
