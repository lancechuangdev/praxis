package transport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"praxis/orderservice/internal/order"
)

type HTTP struct {
	Service *order.Service
	Ready   func(context.Context) error
}

func (h HTTP) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", h.ready)
	mux.HandleFunc("GET /metrics", h.metrics)
	mux.HandleFunc("POST /v1/orders", h.admit)
	return withRequestMetadata(mux)
}

func (h HTTP) ready(w http.ResponseWriter, r *http.Request) {
	if h.Ready != nil {
		if err := h.Ready(r.Context()); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (h HTTP) admit(w http.ResponseWriter, r *http.Request) {
	var request order.Request
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	metadata := metadataFromContext(r.Context())
	request.CorrelationID = metadata.CorrelationID
	request.CausationID = metadata.RequestID
	request.TraceParent = metadata.TraceParent
	request.TraceState = metadata.TraceState
	response, err := h.Service.Admit(r.Context(), request)
	if err != nil {
		status := http.StatusServiceUnavailable
		if errors.Is(err, order.ErrRiskRejected) {
			status = http.StatusForbidden
		} else if validation := order.Validate(request); validation != nil {
			status = http.StatusBadRequest
		}
		writeJSON(w, status, map[string]string{"error": err.Error()})
		return
	}
	w.Header().Set("Server-Timing", fmt.Sprintf("risk;dur=%.3f, reserve;dur=%.3f, matching;dur=%.3f, total;dur=%.3f", response.Timings.RiskMS, response.Timings.ReserveMS, response.Timings.MatchingMS, response.Timings.TotalMS))
	writeJSON(w, http.StatusAccepted, response)
}

func (h HTTP) metrics(w http.ResponseWriter, _ *http.Request) {
	m := h.Service.Metrics
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = fmt.Fprintf(w, `order_requests_total %d
order_accepted_total %d
order_risk_rejected_total %d
order_failed_total %d
`, m.Requests.Load(), m.Accepted.Load(), m.RiskRejected.Load(), m.Failed.Load())
	_, _ = fmt.Fprint(w, m.RiskDuration.Prometheus("order_risk_duration_seconds"), m.ReserveDuration.Prometheus("order_reserve_duration_seconds"), m.MatchingDuration.Prometheus("order_matching_duration_seconds"), m.TotalDuration.Prometheus("order_total_duration_seconds"))
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
