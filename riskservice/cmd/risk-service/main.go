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

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	riskv1 "praxis/api/gen/risk/v1"
	"praxis/riskservice/internal/config"
	"praxis/riskservice/internal/risk"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", cfg.GRPCAddress)
	if err != nil {
		return err
	}
	metrics := &risk.Metrics{}
	service := &risk.Service{PolicyVersion: cfg.PolicyVersion, RejectBPS: cfg.RejectBPS, Latency: cfg.DecisionLatency, Metrics: metrics}
	grpcServer := grpc.NewServer()
	riskv1.RegisterRiskServiceServer(grpcServer, service)
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ready\n")) })
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = fmt.Fprintf(w, "risk_requests_total %d\nrisk_decisions_total{decision=\"approved\"} %d\nrisk_decisions_total{decision=\"rejected\"} %d\nrisk_failures_total %d\nrisk_decision_duration_seconds_sum %.9f\nrisk_decision_duration_seconds_count %d\n", metrics.Requests.Load(), metrics.Approved.Load(), metrics.Rejected.Load(), metrics.Failed.Load(), float64(metrics.DurationNS.Load())/1e9, metrics.Requests.Load())
	})
	httpServer := &http.Server{Addr: cfg.HTTPAddress, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	errCh := make(chan error, 2)
	go func() { errCh <- grpcServer.Serve(listener) }()
	go func() { errCh <- httpServer.ListenAndServe() }()
	log.Info("risk service started", "grpc", cfg.GRPCAddress, "http", cfg.HTTPAddress, "policy_version", cfg.PolicyVersion)
	select {
	case <-ctx.Done():
	case err = <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { _ = httpServer.Shutdown(shutdownCtx); close(done) }()
	grpcDone := make(chan struct{})
	go func() { grpcServer.GracefulStop(); close(grpcDone) }()
	select {
	case <-grpcDone:
	case <-shutdownCtx.Done():
		grpcServer.Stop()
	}
	<-done
	return nil
}
