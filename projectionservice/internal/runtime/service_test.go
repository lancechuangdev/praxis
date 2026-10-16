package runtime

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type fakeConsumer struct {
	active  atomic.Bool
	started chan struct{}
	failure chan struct{}
}

func (f *fakeConsumer) Run(ctx context.Context) error {
	f.active.Store(true)
	close(f.started)
	defer f.active.Store(false)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-f.failure:
		return errors.New("sensitive source details")
	}
}
func (f *fakeConsumer) Running() bool      { return f.active.Load() }
func (f *fakeConsumer) Prometheus() string { return "projection_workflow_fetched_total 3\n" }

func TestDisabledEndpoints(t *testing.T) {
	s := &Service{}
	for _, tc := range []struct {
		path     string
		status   int
		contains string
	}{
		{"/healthz", 200, "ok"}, {"/readyz", 503, "unavailable"}, {"/metrics", 200, "projection_workflow_enabled 0\n"},
	} {
		response := httptest.NewRecorder()
		s.Handler().ServeHTTP(response, httptest.NewRequest("GET", tc.path, nil))
		if response.Code != tc.status || !strings.Contains(response.Body.String(), tc.contains) {
			t.Fatal(response.Code, response.Body.String())
		}
	}
}

func TestFailureRetainsMetricsAndStopsReadinessUntilShutdown(t *testing.T) {
	worker := &fakeConsumer{started: make(chan struct{}), failure: make(chan struct{})}
	s := &Service{Worker: worker}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx, listener) }()
	select {
	case <-worker.started:
	case <-time.After(time.Second):
		t.Fatal("worker did not start")
	}
	ready := httptest.NewRecorder()
	s.Handler().ServeHTTP(ready, httptest.NewRequest("GET", "/readyz", nil))
	if ready.Code != 200 {
		t.Fatal(ready.Code)
	}
	close(worker.failure)
	deadline := time.Now().Add(time.Second)
	for !s.failed.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !s.failed.Load() {
		t.Fatal("worker failure not recorded")
	}
	select {
	case <-done:
		t.Fatal("HTTP service exited on worker failure")
	default:
	}
	for _, tc := range []struct {
		path     string
		status   int
		contains string
	}{
		{"/healthz", 200, "ok"}, {"/readyz", 503, "unavailable"}, {"/metrics", 200, "projection_workflow_consumer_failed 1\n"},
	} {
		response := httptest.NewRecorder()
		s.Handler().ServeHTTP(response, httptest.NewRequest("GET", tc.path, nil))
		if response.Code != tc.status || !strings.Contains(response.Body.String(), tc.contains) || strings.Contains(response.Body.String(), "sensitive") {
			t.Fatal(response.Body.String())
		}
	}
	client := &http.Client{Timeout: time.Second}
	response, err := client.Get("http://" + listener.Addr().String() + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, ErrConsumerStopped) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown did not join")
	}
}

func TestShutdownJoinsRunningWorker(t *testing.T) {
	worker := &fakeConsumer{started: make(chan struct{}), failure: make(chan struct{})}
	s := &Service{Worker: worker}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx, listener) }()
	<-worker.started
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown stalled")
	}
	if worker.Running() {
		t.Fatal("worker still active after service shutdown")
	}
}
