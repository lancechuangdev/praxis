package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"praxis/projectionservice/internal/kafkaconsumer"
)

var ErrConsumerStopped = errors.New("projection consumer stopped; operator intervention required")

type Consumer interface {
	Run(context.Context) error
	Running() bool
	Prometheus() string
	Status() kafkaconsumer.Status
}

// Service retains health and metrics after a consumer failure. It never retries
// or recreates a stopped consumer. Worker=nil is the disabled configuration.
type Service struct {
	Worker     Consumer
	LagMetrics func(context.Context) string
	failed     atomic.Bool
}

func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		var consumer *kafkaconsumer.Status
		if s.Worker != nil {
			status := s.Worker.Status()
			consumer = &status
		}
		_ = json.NewEncoder(w).Encode(struct {
			Enabled  bool                  `json:"enabled"`
			Failed   bool                  `json:"failed"`
			Consumer *kafkaconsumer.Status `json:"consumer,omitempty"`
		}{Enabled: s.Worker != nil, Failed: s.failed.Load(), Consumer: consumer})
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if s.Worker == nil || s.failed.Load() || !s.Worker.Running() {
			http.Error(w, "projection consumer unavailable", http.StatusServiceUnavailable)
			return
		}
		_, _ = fmt.Fprintln(w, "ready")
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		enabled, failed := 0, 0
		if s.Worker != nil {
			enabled = 1
		}
		if s.failed.Load() {
			failed = 1
		}
		_, _ = fmt.Fprintf(w, "projection_workflow_enabled %d\nprojection_workflow_consumer_failed %d\n", enabled, failed)
		if s.Worker != nil {
			_, _ = fmt.Fprint(w, s.Worker.Prometheus())
		}
		if s.LagMetrics != nil {
			_, _ = fmt.Fprint(w, "projection_workflow_lag_observation_enabled 1\n", s.LagMetrics(r.Context()))
		} else {
			_, _ = fmt.Fprint(w, "projection_workflow_lag_observation_enabled 0\n")
		}
	})
	return mux
}

// Serve owns the listener. Cancel and join this method before closing storage.
// Readiness means a running loop, not caught-up or reconciled financial reads.
func (s *Service) Serve(ctx context.Context, listener net.Listener) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	server := &http.Server{Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Serve(listener) }()
	var workerDone chan error
	if s.Worker != nil {
		workerDone = make(chan error, 1)
		go func() { workerDone <- s.Worker.Run(ctx) }()
	}
	var result error
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case err := <-serverDone:
			if !errors.Is(err, http.ErrServerClosed) {
				result = err
			}
			break loop
		case <-workerDone:
			workerDone = nil
			if ctx.Err() == nil {
				s.failed.Store(true)
			}
		}
	}
	cancel()
	shutdown, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	if err := server.Shutdown(shutdown); err != nil {
		_ = server.Close()
		result = errors.Join(result, err)
	}
	if workerDone != nil {
		<-workerDone
	}
	if s.failed.Load() {
		result = errors.Join(result, ErrConsumerStopped)
	}
	return result
}
