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

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"praxis/orderservice/internal/adapters"
	"praxis/orderservice/internal/config"
	"praxis/orderservice/internal/observability"
	"praxis/orderservice/internal/order"
	"praxis/orderservice/internal/transport"
	"praxis/orderservice/migrations"
)

func main() {
	if len(os.Args) == 2 && os.Args[1] == "healthcheck" {
		if err := checkReady(os.Getenv("ORDER_HTTP_ADDRESS")); err != nil {
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
	sagaDB, err := order.OpenSagaDBWithLifecycle(ctx, cfg.DatabaseURL, cfg.LifecycleCapture)
	if err != nil {
		log.Error("saga database", "error", err)
		os.Exit(1)
	}
	defer sagaDB.Close()
	if err = migrations.Apply(ctx, sagaDB); err != nil {
		log.Error("saga migrations", "error", err)
		os.Exit(1)
	}
	var workflowMetrics func(context.Context) string
	if cfg.WorkflowObserve {
		observationDB, observationErr := order.OpenWorkflowObservationDB(ctx, cfg.DatabaseURL)
		if observationErr != nil {
			log.Error("workflow observation configuration", "error", observationErr)
			os.Exit(1)
		}
		defer observationDB.Close()
		workflowMetrics = (order.WorkflowOutboxObserver{DB: observationDB}).Prometheus
	}
	tracingShutdown, err := observability.SetupTracing(ctx, "order-service")
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

	var ledger order.Ledger
	var ledgerReady func(context.Context) error
	if cfg.LedgerMode == "mock" {
		mock := adapters.MockLedger{Latency: cfg.MockLedgerLatency}
		ledger = mock
		ledgerReady = mock.Ready
	} else {
		client, clientErr := adapters.NewGRPCLedger(cfg.LedgerGRPCAddress, cfg.LedgerTimeout)
		err = clientErr
		if err != nil {
			log.Error("ledger client", "error", err)
			os.Exit(1)
		}
		ledger = client
		ledgerReady = client.Ready
	}
	defer ledger.Close()

	var matching order.MatchingEngine
	var matchingReady func(context.Context) error
	if cfg.MatchingMode == "mock" {
		mock := &adapters.MockMatching{Latency: cfg.MockMatchingLatency}
		matching = mock
		matchingReady = mock.Ready
	} else {
		client, clientErr := adapters.NewGRPCMatching(cfg.MatchingGRPCAddress, cfg.MatchingTimeout)
		err = clientErr
		if err != nil {
			log.Error("matching client", "error", err)
			os.Exit(1)
		}
		matching = client
		matchingReady = client.Ready
	}
	defer matching.Close()

	hostname, _ := os.Hostname()
	recoveryOwner := fmt.Sprintf("%s:%d", hostname, os.Getpid())
	var admission order.AdmissionController
	if cfg.AdmissionMode == "observe" {
		admission = order.NewLocalTokenBuckets(float64(cfg.AdmissionBucketCapacity), float64(cfg.AdmissionRefillPerSecond), true)
	}
	metrics := &order.Metrics{}
	var riskChecker order.RiskChecker = order.EmbeddedRiskChecker{Latency: cfg.RiskLatency, RejectBPS: cfg.RiskRejectBPS}
	var riskReady func(context.Context) error
	if cfg.RiskRemoteMode == "shadow" || cfg.RiskRemoteMode == "authoritative" {
		riskClient, clientErr := adapters.NewGRPCRisk(cfg.RiskGRPCAddress, cfg.RiskTimeout)
		if clientErr != nil {
			log.Error("risk client", "error", clientErr)
			os.Exit(1)
		}
		defer riskClient.Close()
		selected, selectErr := order.SelectRiskChecker(cfg.RiskRemoteMode, riskChecker, riskClient, cfg.RiskShadowMaxInFlight, metrics, log)
		if selectErr != nil {
			log.Error("risk mode", "error", selectErr)
			os.Exit(1)
		}
		riskChecker = selected
		if cfg.RiskRemoteMode == "authoritative" {
			riskReady = riskClient.Ready
		}
	}
	service := &order.Service{Ledger: ledger, Matching: matching, Risk: riskChecker, Metrics: metrics, Sagas: &order.PostgresSagaStore{DB: sagaDB, MaxReleaseAttempts: cfg.SagaMaxReleaseAttempts, MaxReservationAttempts: cfg.SagaMaxReservationAttempts}, Admission: admission, Concurrency: order.NewConcurrencyGate(cfg.AdmissionMaxInFlight, cfg.AdmissionMaxWaiting), RecoveryOwner: recoveryOwner, RecoveryLease: cfg.SagaClaimLease, ReservationRetryConcurrency: cfg.SagaReservationConcurrency, ReleaseRetryConcurrency: cfg.SagaReleaseConcurrency, DownstreamConcurrency: cfg.DownstreamConcurrency, DownstreamAdmissionReserve: cfg.DownstreamAdmissionReserve}
	go func() {
		if recoveryErr := service.RunSagaRecoverySupervised(ctx, cfg.SagaRetryInterval, cfg.SagaRecoveryRestartDelay, cfg.SagaRecoveryMaxPanics, log); recoveryErr != nil {
			log.Error("saga recovery worker stopped", "error", recoveryErr)
		}
	}()
	ready := func(ctx context.Context) error {
		if readyErr := service.SagaRecoveryReady(); readyErr != nil {
			return readyErr
		}
		checks := []func() error{
			func() error { return sagaDB.Ping(ctx) },
			func() error {
				if readyErr := ledgerReady(ctx); readyErr != nil {
					return fmt.Errorf("ledger: %w", readyErr)
				}
				return nil
			},
			func() error {
				if readyErr := matchingReady(ctx); readyErr != nil {
					return fmt.Errorf("matching: %w", readyErr)
				}
				return nil
			},
		}
		if riskReady != nil {
			checks = append(checks, func() error {
				if readyErr := riskReady(ctx); readyErr != nil {
					return fmt.Errorf("risk: %w", readyErr)
				}
				return nil
			})
		}
		results := make(chan error, len(checks))
		for _, check := range checks {
			go func(check func() error) { results <- check() }(check)
		}
		var readinessErr error
		for range checks {
			readinessErr = errors.Join(readinessErr, <-results)
		}
		return readinessErr
	}
	server := &http.Server{Addr: cfg.HTTPAddress, Handler: otelhttp.NewHandler(transport.HTTP{Service: service, Ready: ready, WorkflowMetrics: workflowMetrics}.Handler(), "order.http"), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}
	errCh := make(chan error, 1)
	go func() { errCh <- server.ListenAndServe() }()
	log.Info("order service started", "http", cfg.HTTPAddress, "ledger_mode", cfg.LedgerMode, "matching_mode", cfg.MatchingMode, "risk_remote_mode", cfg.RiskRemoteMode, "lifecycle_capture", cfg.LifecycleCapture)
	select {
	case <-ctx.Done():
	case err = <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server stopped", "error", err)
		}
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
}

func checkReady(address string) error {
	if address == "" {
		address = ":8083"
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
