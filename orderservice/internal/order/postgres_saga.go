package order

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresSagaStore struct{ DB *pgxpool.Pool }

func (p *PostgresSagaStore) Start(ctx context.Context, req Request) error {
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(body)
	fingerprint := hex.EncodeToString(sum[:])
	ct, err := p.DB.Exec(ctx, `INSERT INTO order_sagas(request_id,request_fingerprint,order_id,correlation_id,causation_id,state)
		VALUES($1,$2,$3,$4,$5,'started') ON CONFLICT(request_id) DO UPDATE SET updated_at=now()
		WHERE order_sagas.request_fingerprint=EXCLUDED.request_fingerprint`, req.RequestID, fingerprint, req.OrderID, req.CorrelationID, req.CausationID)
	if err == nil && ct.RowsAffected() == 0 {
		return errors.New("request_id was already used with a different order payload")
	}
	return err
}

func (p *PostgresSagaStore) Reserved(ctx context.Context, requestID, reservationID string) error {
	ct, err := p.DB.Exec(ctx, `UPDATE order_sagas SET reservation_id=$2,state='reserved',updated_at=now() WHERE request_id=$1 AND state IN ('started','reserved')`, requestID, reservationID)
	if err == nil && ct.RowsAffected() != 1 {
		return errors.New("order saga cannot return to reserved state")
	}
	return err
}

func (p *PostgresSagaStore) Accepted(ctx context.Context, requestID string) error {
	ct, err := p.DB.Exec(ctx, `UPDATE order_sagas SET state='accepted',updated_at=now() WHERE request_id=$1 AND state IN ('reserved','accepted')`, requestID)
	if err == nil && ct.RowsAffected() != 1 {
		return errors.New("order saga cannot enter accepted state")
	}
	return err
}

func (p *PostgresSagaStore) RequestRelease(ctx context.Context, orderID, reason string) (Saga, error) {
	if reason != "matching_failed" && reason != "execution_complete" {
		return Saga{}, errors.New("invalid release reason")
	}
	var s Saga
	err := p.DB.QueryRow(ctx, `UPDATE order_sagas SET state=CASE WHEN state='released' THEN state ELSE 'release_pending' END,
		release_reason=COALESCE(release_reason,$2),next_attempt_at=now(),updated_at=now()
		WHERE order_id=$1 AND (release_reason IS NULL OR release_reason=$2)
		AND (state='released' OR ($2='matching_failed' AND state='reserved') OR ($2='execution_complete' AND state='accepted') OR (state='release_pending' AND release_reason=$2))
		RETURNING request_id,order_id,correlation_id,causation_id,COALESCE(reservation_id,''),state,COALESCE(release_reason,''),attempt_count`, orderID, reason).
		Scan(&s.RequestID, &s.OrderID, &s.CorrelationID, &s.CausationID, &s.ReservationID, &s.State, &s.ReleaseReason, &s.Attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return Saga{}, errors.New("order saga not found or has a conflicting terminal action")
	}
	return s, err
}

func (p *PostgresSagaStore) Released(ctx context.Context, requestID string) error {
	_, err := p.DB.Exec(ctx, `UPDATE order_sagas SET state='released',last_error=NULL,updated_at=now() WHERE request_id=$1`, requestID)
	return err
}

func (p *PostgresSagaStore) ReleaseFailed(ctx context.Context, requestID string, cause error) error {
	_, err := p.DB.Exec(ctx, `UPDATE order_sagas SET attempt_count=attempt_count+1,last_error=$2,
		next_attempt_at=now()+make_interval(secs=>LEAST(300,power(2,LEAST(attempt_count,8))::int)),updated_at=now()
		WHERE request_id=$1 AND state='release_pending'`, requestID, cause.Error())
	return err
}

func (p *PostgresSagaStore) DueReleases(ctx context.Context, limit int) ([]Saga, error) {
	rows, err := p.DB.Query(ctx, `SELECT request_id,order_id,correlation_id,causation_id,COALESCE(reservation_id,''),state,release_reason,attempt_count
		FROM order_sagas WHERE state='release_pending' AND next_attempt_at<=now() ORDER BY next_attempt_at LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Saga
	for rows.Next() {
		var s Saga
		if err = rows.Scan(&s.RequestID, &s.OrderID, &s.CorrelationID, &s.CausationID, &s.ReservationID, &s.State, &s.ReleaseReason, &s.Attempts); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func OpenSagaDB(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, err
	}
	config.MaxConns = 8
	config.MaxConnLifetime = 30 * time.Minute
	return pgxpool.NewWithConfig(ctx, config)
}
