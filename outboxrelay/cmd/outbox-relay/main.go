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
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"praxis/outboxrelay/internal/config"
	"praxis/outboxrelay/internal/messaging"
	"praxis/outboxrelay/internal/relay"
	"praxis/outboxrelay/internal/store"
	"praxis/outboxrelay/internal/telemetry"
	"praxis/outboxrelay/migrations"
)

func main() {
	mode, err := commandMode(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if mode == "healthcheck" {
		if err := checkReady(os.Getenv("OUTBOX_HTTP_ADDRESS")); err != nil {
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
	telemetryShutdown, err := telemetry.Setup(ctx, "outbox-relay")
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
	db, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("database", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	if err = db.Ping(ctx); err != nil {
		log.Error("database ping", "error", err)
		os.Exit(1)
	}
	if mode == "migrate" || cfg.MigrateOnStartup {
		err = migrations.Apply(ctx, db)
	} else {
		err = migrations.Verify(ctx, db)
	}
	if err != nil {
		log.Error("schema migrations", "error", err)
		os.Exit(1)
	}
	if mode == "migrate" {
		if os.Getenv("OUTBOX_BOOTSTRAP_RUNTIME_ROLES") == "true" {
			ledgerPassword := os.Getenv("LEDGER_RUNTIME_PASSWORD")
			outboxPassword := os.Getenv("OUTBOX_RUNTIME_PASSWORD")
			if err := migrations.BootstrapRuntimeRoles(ctx, db, ledgerPassword, outboxPassword); err != nil {
				log.Error("runtime role bootstrap", "error", err)
				os.Exit(1)
			}
		}
		log.Info("outbox migrations applied")
		return
	}

	writer := messaging.NewWriter(messaging.WriterConfig{Brokers: cfg.Brokers, Auth: cfg.KafkaAuth, BatchSize: cfg.BatchSize, BatchBytes: cfg.BatchBytes, BatchTimeout: cfg.BatchTimeout})
	metrics := &relay.Metrics{}
	worker := &relay.Relay{Store: store.Postgres{DB: db}, Publisher: writer, TopicMap: cfg.TopicMap, InstanceID: cfg.InstanceID, ClaimSize: cfg.ClaimSize, LeaseDuration: cfg.LeaseDuration, PollInterval: cfg.PollInterval, Log: log, Metrics: metrics}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := db.Ping(r.Context()); err != nil {
			http.Error(w, "database unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = fmt.Fprintf(w, "outbox_events_claimed_total %d\noutbox_events_published_total %d\noutbox_events_failed_total %d\n", metrics.Claimed.Load(), metrics.Published.Load(), metrics.Failed.Load())
		_, _ = fmt.Fprint(w, metrics.BatchDuration.Prometheus("outbox_batch_duration_seconds"))
	})
	server := &http.Server{Addr: cfg.HTTPAddress, Handler: otelhttp.NewHandler(mux, "outbox-relay.http"), ReadHeaderTimeout: 5 * time.Second}
	errCh := make(chan error, 2)
	go func() { errCh <- worker.Run(ctx) }()
	go func() { errCh <- server.ListenAndServe() }()
	log.Info("outbox relay started", "instance_id", cfg.InstanceID, "claim_size", cfg.ClaimSize, "kafka_batch_size", cfg.BatchSize, "kafka_batch_bytes", cfg.BatchBytes, "kafka_batch_timeout", cfg.BatchTimeout)
	select {
	case <-ctx.Done():
	case err = <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, http.ErrServerClosed) {
			log.Error("service stopped", "error", err)
		}
	}
	stop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
	_ = writer.Close()
}

func commandMode(args []string) (string, error) {
	if len(args) == 0 {
		return "serve", nil
	}
	if len(args) == 1 && (args[0] == "healthcheck" || args[0] == "migrate") {
		return args[0], nil
	}
	return "", fmt.Errorf("usage: outbox-relay [healthcheck|migrate]")
}

func checkReady(address string) error {
	if address == "" {
		address = ":8082"
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
