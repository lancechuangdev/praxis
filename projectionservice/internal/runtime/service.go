package runtime

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

var ErrConsumerStopped = errors.New("projection consumer stopped; operator intervention required")

type Consumer interface {
	Run(context.Context) error
	Running() bool
	Prometheus() string
}

// Service retains health and metrics after a consumer failure. It never retries
// or recreates a stopped consumer. Worker=nil is the disabled configuration.
type Service struct {
	Worker Consumer
	failed atomic.Bool
}

func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
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
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
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
