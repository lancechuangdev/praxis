package main

import (
	"context"
	"errors"
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
	if cfg.LedgerMode == "mock" {
		ledger = adapters.MockLedger{Latency: cfg.MockLedgerLatency}
	} else {
		ledger, err = adapters.NewGRPCLedger(cfg.LedgerGRPCAddress, cfg.LedgerTimeout)
		if err != nil {
			log.Error("ledger client", "error", err)
			os.Exit(1)
		}
	}
	defer ledger.Close()

	var matching order.MatchingEngine
	if cfg.MatchingMode == "mock" {
		matching = &adapters.MockMatching{Latency: cfg.MockMatchingLatency}
	} else {
		matching, err = adapters.NewGRPCMatching(cfg.MatchingGRPCAddress, cfg.MatchingTimeout)
		if err != nil {
			log.Error("matching client", "error", err)
			os.Exit(1)
		}
	}
	defer matching.Close()

	service := &order.Service{Ledger: ledger, Matching: matching, RiskLatency: cfg.RiskLatency, RiskRejectBPS: cfg.RiskRejectBPS, Metrics: &order.Metrics{}}
	server := &http.Server{Addr: cfg.HTTPAddress, Handler: transport.HTTP{Service: service}.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}
	errCh := make(chan error, 1)
	go func() { errCh <- server.ListenAndServe() }()
	log.Info("mock order service started", "http", cfg.HTTPAddress, "ledger_mode", cfg.LedgerMode, "matching_mode", cfg.MatchingMode)
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
