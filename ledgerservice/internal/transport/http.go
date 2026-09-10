package transport

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"praxis/ledgerservice/internal/ledger"
)

type HTTP struct {
	Store ledger.Store
	DB    *pgxpool.Pool
}

func (h HTTP) Handler() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { write(w, 200, map[string]string{"status": "ok"}) })
	m.HandleFunc("GET /readyz", h.ready)
	m.HandleFunc("POST /v1/deposits:post", h.deposit)
	m.HandleFunc("POST /v1/holds:release", h.release)
	m.HandleFunc("POST /v1/orders:reserve", h.reserve)
	m.HandleFunc("GET /v1/balances/{user}/{asset}", h.balance)
	m.HandleFunc("GET /v1/reservations/{order}", h.reservation)
	return m
}
func decode(r *http.Request, v any) error {
	d := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	d.DisallowUnknownFields()
	return d.Decode(v)
}
func write(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func problem(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	if errors.Is(err, ledger.ErrNotFound) {
		status = 404
	} else if errors.Is(err, ledger.ErrInsufficientFunds) {
		status = 409
	} else if errors.Is(err, ledger.ErrConflict) {
		status = 409
	}
	write(w, status, map[string]string{"error": err.Error()})
}
func parseTime(v string) (time.Time, error) {
	if strings.TrimSpace(v) == "" {
		return time.Now().UTC(), nil
	}
	return time.Parse(time.RFC3339Nano, v)
}
func (h HTTP) ready(w http.ResponseWriter, r *http.Request) {
	if err := h.DB.Ping(r.Context()); err != nil {
		write(w, 503, map[string]string{"status": "unavailable"})
		return
	}
	write(w, 200, map[string]string{"status": "ready"})
}
func (h HTTP) deposit(w http.ResponseWriter, r *http.Request) {
	var v struct {
		CommandID         string `json:"command_id"`
		DepositID         string `json:"deposit_id"`
		UserID            string `json:"user_id"`
		AssetID           string `json:"asset_id"`
		CustodyPositionID string `json:"custody_position_id"`
		AmountAtomic      string `json:"amount_atomic"`
		TargetBucket      string `json:"target_bucket"`
		SourceSystem      string `json:"source_system"`
		CorrelationID     string `json:"correlation_id"`
		CausationID       string `json:"causation_id"`
		OccurredAt        string `json:"occurred_at"`
	}
	if e := decode(r, &v); e != nil {
		problem(w, e)
		return
	}
	at, e := parseTime(v.OccurredAt)
	if e != nil {
		problem(w, e)
		return
	}
	out, e := h.Store.PostDeposit(r.Context(), ledger.PostDeposit{CommandID: v.CommandID, DepositID: v.DepositID, UserID: v.UserID, AssetID: v.AssetID, CustodyPositionID: v.CustodyPositionID, AmountAtomic: v.AmountAtomic, TargetBucket: v.TargetBucket, SourceSystem: v.SourceSystem, CorrelationID: v.CorrelationID, CausationID: v.CausationID, OccurredAt: at})
	if e != nil {
		problem(w, e)
		return
	}
	write(w, 200, out)
}
func (h HTTP) release(w http.ResponseWriter, r *http.Request) {
	var v struct {
		CommandID     string `json:"command_id"`
		DepositID     string `json:"deposit_id"`
		UserID        string `json:"user_id"`
		AssetID       string `json:"asset_id"`
		AmountAtomic  string `json:"amount_atomic"`
		SourceSystem  string `json:"source_system"`
		CorrelationID string `json:"correlation_id"`
		CausationID   string `json:"causation_id"`
		OccurredAt    string `json:"occurred_at"`
	}
	if e := decode(r, &v); e != nil {
		problem(w, e)
		return
	}
	at, e := parseTime(v.OccurredAt)
	if e != nil {
		problem(w, e)
		return
	}
	out, e := h.Store.ReleaseHold(r.Context(), ledger.ReleaseHold{CommandID: v.CommandID, DepositID: v.DepositID, UserID: v.UserID, AssetID: v.AssetID, AmountAtomic: v.AmountAtomic, SourceSystem: v.SourceSystem, CorrelationID: v.CorrelationID, CausationID: v.CausationID, OccurredAt: at})
	if e != nil {
		problem(w, e)
		return
	}
	write(w, 200, out)
}
func (h HTTP) reserve(w http.ResponseWriter, r *http.Request) {
	var v struct {
		CommandID     string `json:"command_id"`
		OrderID       string `json:"order_id"`
		UserID        string `json:"user_id"`
		AssetID       string `json:"asset_id"`
		AmountAtomic  string `json:"amount_atomic"`
		CorrelationID string `json:"correlation_id"`
		CausationID   string `json:"causation_id"`
		OccurredAt    string `json:"occurred_at"`
	}
	if e := decode(r, &v); e != nil {
		problem(w, e)
		return
	}
	at, e := parseTime(v.OccurredAt)
	if e != nil {
		problem(w, e)
		return
	}
	out, e := h.Store.ReserveForOrder(r.Context(), ledger.ReserveOrder{CommandID: v.CommandID, OrderID: v.OrderID, UserID: v.UserID, AssetID: v.AssetID, AmountAtomic: v.AmountAtomic, CorrelationID: v.CorrelationID, CausationID: v.CausationID, OccurredAt: at})
	if e != nil {
		problem(w, e)
		return
	}
	write(w, 200, out)
}
func (h HTTP) balance(w http.ResponseWriter, r *http.Request) {
	v, e := h.Store.GetBalance(r.Context(), r.PathValue("user"), r.PathValue("asset"))
	if e != nil {
		problem(w, e)
		return
	}
	write(w, 200, v)
}
func (h HTTP) reservation(w http.ResponseWriter, r *http.Request) {
	v, e := h.Store.GetReservation(r.Context(), r.PathValue("order"))
	if e != nil {
		problem(w, e)
		return
	}
	write(w, 200, v)
}
