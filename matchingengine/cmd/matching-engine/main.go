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

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	matchingv1 "praxis/matchingengine/gen/matching/v1"
	"praxis/matchingengine/internal/config"
	"praxis/matchingengine/internal/engine"
	"praxis/matchingengine/internal/telemetry"
)

func main() {
	if len(os.Args) == 2 && os.Args[1] == "healthcheck" {
		if err := checkReady(os.Getenv("MATCHING_HTTP_ADDRESS")); err != nil {
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
	telemetryShutdown, err := telemetry.Setup(ctx, "matching-engine")
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
	listener, err := net.Listen("tcp", cfg.GRPCAddress)
	if err != nil {
		log.Error("listen", "error", err)
		os.Exit(1)
	}
	var publisher engine.Publisher = engine.NoopPublisher{}
	if cfg.KafkaEnabled {
		publisher = engine.NewKafkaPublisher(cfg.KafkaBrokers, cfg.EventsTopic, cfg.KafkaAuth, cfg.KafkaBatchSize, cfg.KafkaBatchBytes, cfg.KafkaBatchTimeout, cfg.KafkaTimeout)
	}
	defer publisher.Close()
	metrics := &engine.Metrics{}
	service := &engine.Service{Publisher: publisher, Latency: cfg.EngineLatency, Metrics: metrics}
	grpcServer := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))
	matchingv1.RegisterMatchingEngineServer(grpcServer, service)
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ready\n")) })
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = fmt.Fprintf(w, "matching_orders_submitted_total %d\nmatching_orders_accepted_total %d\nmatching_orders_failed_total %d\n", metrics.Submitted.Load(), metrics.Accepted.Load(), metrics.Failed.Load())
		_, _ = fmt.Fprint(w, metrics.EngineDuration.Prometheus("matching_engine_duration_seconds"), metrics.KafkaDuration.Prometheus("matching_kafka_duration_seconds"))
	})
	httpServer := &http.Server{Addr: cfg.HTTPAddress, Handler: otelhttp.NewHandler(mux, "matching.http"), ReadHeaderTimeout: 5 * time.Second}
	errCh := make(chan error, 2)
	go func() { errCh <- grpcServer.Serve(listener) }()
	go func() { errCh <- httpServer.ListenAndServe() }()
	log.Info("matching engine started", "grpc", cfg.GRPCAddress, "http", cfg.HTTPAddress, "kafka_enabled", cfg.KafkaEnabled, "events_topic", cfg.EventsTopic)
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
}

func checkReady(address string) error {
	if address == "" {
		address = ":8084"
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
