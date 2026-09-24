package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	matchingv1 "praxis/matchingengine/gen/matching/v1"
)

type Metrics struct {
	Submitted, Accepted, Failed atomic.Uint64
	EngineNS                    atomic.Uint64
	EngineDuration              DurationHistogram
}

type Store interface {
	Submit(context.Context, *matchingv1.SubmitOrderRequest) (*matchingv1.SubmitOrderResponse, error)
}

type Service struct {
	matchingv1.UnimplementedMatchingEngineServer
	Store   Store
	Latency time.Duration
	Metrics *Metrics
}

func (s *Service) SubmitOrder(ctx context.Context, req *matchingv1.SubmitOrderRequest) (*matchingv1.SubmitOrderResponse, error) {
	s.Metrics.Submitted.Add(1)
	if err := validate(req); err != nil {
		s.Metrics.Failed.Add(1)
		return nil, err
	}
	started := time.Now()
	if err := wait(ctx, s.Latency); err != nil {
		s.Metrics.Failed.Add(1)
		return nil, err
	}
	response, err := s.Store.Submit(ctx, req)
	duration := time.Since(started)
	s.Metrics.EngineNS.Add(uint64(duration))
	s.Metrics.EngineDuration.Observe(duration)
	if err != nil {
		s.Metrics.Failed.Add(1)
		return nil, err
	}
	s.Metrics.Accepted.Add(1)
	return response, nil
}

type PostgresStore struct {
	db    *pgxpool.Pool
	topic string
}

func NewPostgresStore(db *pgxpool.Pool, topic string) *PostgresStore {
	return &PostgresStore{db: db, topic: topic}
}

func (s *PostgresStore) Submit(ctx context.Context, req *matchingv1.SubmitOrderRequest) (*matchingv1.SubmitOrderResponse, error) {
	fingerprint, err := requestFingerprint(req)
	if err != nil {
		return nil, err
	}
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, req.RequestId); err != nil {
		return nil, err
	}
	var orderID, status, storedFingerprint string
	var sequence int64
	err = tx.QueryRow(ctx, `SELECT order_id,status,engine_sequence,request_fingerprint FROM matching_orders WHERE request_id=$1`, req.RequestId).Scan(&orderID, &status, &sequence, &storedFingerprint)
	if err == nil {
		if storedFingerprint != fingerprint {
			return nil, errors.New("request_id was already used with a different payload")
		}
		return &matchingv1.SubmitOrderResponse{OrderId: orderID, Status: status, EngineSequence: sequence}, tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}

	if _, err = tx.Exec(ctx, `INSERT INTO matching_books(symbol,next_sequence) VALUES($1,1) ON CONFLICT(symbol) DO NOTHING`, req.Symbol); err != nil {
		return nil, err
	}
	if err = tx.QueryRow(ctx, `UPDATE matching_books SET next_sequence=next_sequence+1 WHERE symbol=$1 RETURNING next_sequence-1`, req.Symbol).Scan(&sequence); err != nil {
		return nil, err
	}
	status = "accepted"
	_, err = tx.Exec(ctx, `INSERT INTO matching_orders(request_id,request_fingerprint,order_id,user_id,symbol,side,order_type,quantity,price,reservation_id,status,engine_sequence) VALUES($1,$2,$3,$4,$5,$6,$7,$8,NULLIF($9,''),$10,$11,$12)`, req.RequestId, fingerprint, req.OrderId, req.UserId, req.Symbol, req.Side, req.OrderType, req.Quantity, req.Price, req.ReservationId, status, sequence)
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(map[string]any{"id": req.RequestId, "type": "OrderAccepted", "aggregate_id": req.OrderId, "correlation_id": req.CorrelationId, "causation_id": req.CausationId, "trace_parent": req.TraceParent, "trace_state": req.TraceState, "occurred_at": time.Now().UTC(), "data": map[string]any{"order": req, "engine_sequence": sequence}})
	if err != nil {
		return nil, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO outbox_events(id,topic,event_type,message_key,payload) VALUES($1,$2,'OrderAccepted',$3,$4)`, req.RequestId, s.topic, req.Symbol, payload); err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &matchingv1.SubmitOrderResponse{OrderId: req.OrderId, Status: status, EngineSequence: sequence}, nil
}

func requestFingerprint(req *matchingv1.SubmitOrderRequest) (string, error) {
	// Observability context may legitimately change when a timed-out client retries.
	businessRequest := struct {
		OrderID, UserID, Symbol, Side, OrderType, Quantity, Price, ReservationID string
	}{req.OrderId, req.UserId, req.Symbol, req.Side, req.OrderType, req.Quantity, req.Price, req.ReservationId}
	body, err := json.Marshal(businessRequest)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}
func validate(req *matchingv1.SubmitOrderRequest) error {
	if req == nil || strings.TrimSpace(req.RequestId) == "" || strings.TrimSpace(req.OrderId) == "" || strings.TrimSpace(req.UserId) == "" || strings.TrimSpace(req.Symbol) == "" || strings.TrimSpace(req.ReservationId) == "" {
		return errors.New("request, order, user, symbol, and reservation IDs are required")
	}
	return nil
}
func wait(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
