package transport

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"praxis/orderservice/internal/order"
)

func TestReady(t *testing.T) {
	tests := []struct {
		name   string
		ready  func(context.Context) error
		status int
	}{
		{name: "dependencies ready", ready: func(context.Context) error { return nil }, status: http.StatusOK},
		{name: "dependency unavailable", ready: func(context.Context) error { return errors.New("unavailable") }, status: http.StatusServiceUnavailable},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/readyz", nil)
			response := httptest.NewRecorder()

			HTTP{Ready: test.ready}.Handler().ServeHTTP(response, request)

			if response.Code != test.status {
				t.Fatalf("status=%d, want %d", response.Code, test.status)
			}
		})
	}
}

func TestMetricsExposeSagaBacklogAndExpiredLeases(t *testing.T) {
	metrics := &order.Metrics{}
	metrics.SagaLeaseExpirations.Store(2)
	metrics.SagaRecoveryReplays.Store(3)
	metrics.SagaDue.Store(7)
	metrics.SagaOldestDueAgeMillis.Store(2500)
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()

	HTTP{Service: &order.Service{Metrics: metrics}}.Handler().ServeHTTP(response, request)

	body := response.Body.String()
	for _, metric := range []string{
		"order_saga_lease_expirations_total 2\n",
		"order_saga_recovery_replays_total 3\n",
		"order_saga_due 7\n",
		"order_saga_oldest_due_age_seconds 2.500\n",
	} {
		if !strings.Contains(body, metric) {
			t.Fatalf("metrics missing %q:\n%s", metric, body)
		}
	}
}

type captureLedger struct{ request order.Request }

func (c *captureLedger) Reserve(_ context.Context, request order.Request) (order.Reservation, error) {
	c.request = request
	return order.Reservation{ID: "reservation-1"}, nil
}
func (*captureLedger) Release(context.Context, order.ReleaseRequest) (order.Reservation, error) {
	return order.Reservation{}, nil
}
func (*captureLedger) Close() error { return nil }

type acceptingMatching struct{}

func (*acceptingMatching) Submit(context.Context, order.Request, order.Reservation) (order.MatchResult, error) {
	return order.MatchResult{Status: "accepted"}, nil
}
func (*acceptingMatching) Close() error { return nil }

type rejectingAdmission struct{}

func (rejectingAdmission) Check(context.Context, order.Request) (order.AdmissionDecision, error) {
	return order.AdmissionDecision{RetryAfter: 1500 * time.Millisecond}, nil
}

func TestRateLimitReturns429BeforeLedgerCall(t *testing.T) {
	ledger := &captureLedger{}
	service := &order.Service{Ledger: ledger, Matching: &acceptingMatching{}, Metrics: &order.Metrics{}, Admission: rejectingAdmission{}}
	request := httptest.NewRequest(http.MethodPost, "/v1/orders", strings.NewReader(`{"request_id":"command-1","order_id":"order-1","user_id":"alice","symbol":"BTC-USDT","side":"buy","order_type":"market","quantity":"1","reserve_asset_id":"asset_usdt","reserve_amount_atomic":"100"}`))
	response := httptest.NewRecorder()

	HTTP{Service: service}.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusTooManyRequests || response.Header().Get("Retry-After") != "2" {
		t.Fatalf("status=%d retry-after=%q body=%s", response.Code, response.Header().Get("Retry-After"), response.Body.String())
	}
	if ledger.request.RequestID != "" {
		t.Fatal("rate-limited request reached Ledger")
	}
	if !strings.Contains(response.Body.String(), `"code":"rate_limit_exceeded"`) {
		t.Fatalf("body=%s", response.Body.String())
	}
}

func TestRequestContextPropagatesIntoOrder(t *testing.T) {
	ledger := &captureLedger{}
	service := &order.Service{Ledger: ledger, Matching: &acceptingMatching{}, Metrics: &order.Metrics{}}
	request := httptest.NewRequest(http.MethodPost, "/v1/orders", strings.NewReader(`{
		"request_id":"command-1",
		"order_id":"order-1",
		"user_id":"alice",
		"symbol":"BTC-USDT",
		"side":"buy",
		"order_type":"market",
		"quantity":"1",
		"reserve_asset_id":"asset_usdt",
		"reserve_amount_atomic":"100"
	}`))
	request.Header.Set(requestIDHeader, "request-1")
	request.Header.Set(correlationIDHeader, "correlation-1")
	request.Header.Set(traceParentHeader, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	request.Header.Set(traceStateHeader, "vendor=value")
	response := httptest.NewRecorder()

	HTTP{Service: service}.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if ledger.request.CorrelationID != "correlation-1" || ledger.request.CausationID != "request-1" {
		t.Fatalf("context=%q,%q", ledger.request.CorrelationID, ledger.request.CausationID)
	}
	if ledger.request.TraceParent != "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" || ledger.request.TraceState != "vendor=value" {
		t.Fatalf("trace context=%q,%q", ledger.request.TraceParent, ledger.request.TraceState)
	}
	if response.Header().Get(requestIDHeader) != "request-1" || response.Header().Get(correlationIDHeader) != "correlation-1" {
		t.Fatalf("response context=%q,%q", response.Header().Get(requestIDHeader), response.Header().Get(correlationIDHeader))
	}
}

func TestTraceContextRejectsInvalidTraceParent(t *testing.T) {
	if value := validTraceParent("00-00000000000000000000000000000000-00f067aa0ba902b7-01"); value != "" {
		t.Fatalf("accepted zero trace id %q", value)
	}
	if value := validTraceParent("00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01"); value != "" {
		t.Fatalf("accepted zero span id %q", value)
	}
	if value := validTraceParent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"); value == "" {
		t.Fatal("rejected valid traceparent")
	}
}

func TestRequestContextReplacesUnsafeValues(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	request.Header.Set(requestIDHeader, "unsafe value\n")
	request.Header.Set(correlationIDHeader, "also unsafe/")
	response := httptest.NewRecorder()

	HTTP{}.Handler().ServeHTTP(response, request)

	requestID := response.Header().Get(requestIDHeader)
	correlationID := response.Header().Get(correlationIDHeader)
	if !strings.HasPrefix(requestID, "req_") {
		t.Fatalf("request id=%q", requestID)
	}
	if correlationID != requestID {
		t.Fatalf("correlation id=%q, request id=%q", correlationID, requestID)
	}
}
