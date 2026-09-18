package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"praxis/orderservice/internal/adapters"
	"praxis/orderservice/internal/config"
	"praxis/orderservice/internal/order"
	"praxis/orderservice/internal/transport"
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

	service := &order.Service{Ledger: ledger, Matching: matching, RiskLatency: cfg.RiskLatency, RiskRejectBPS: cfg.RiskRejectBPS, Metrics: &order.Metrics{}}
	ready := func(ctx context.Context) error {
		results := make(chan error, 2)
		go func() {
			if readyErr := ledgerReady(ctx); readyErr != nil {
				results <- fmt.Errorf("ledger: %w", readyErr)
				return
			}
			results <- nil
		}()
		go func() {
			if readyErr := matchingReady(ctx); readyErr != nil {
				results <- fmt.Errorf("matching: %w", readyErr)
				return
			}
			results <- nil
		}()
		return errors.Join(<-results, <-results)
	}
	server := &http.Server{Addr: cfg.HTTPAddress, Handler: transport.HTTP{Service: service, Ready: ready}.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}
	errCh := make(chan error, 1)
	go func() { errCh <- server.ListenAndServe() }()
	log.Info("order service started", "http", cfg.HTTPAddress, "ledger_mode", cfg.LedgerMode, "matching_mode", cfg.MatchingMode)
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
