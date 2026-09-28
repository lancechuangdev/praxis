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

type PostgresSagaStore struct {
	DB                     *pgxpool.Pool
	MaxReleaseAttempts     int
	MaxReservationAttempts int
}

func (p *PostgresSagaStore) Start(ctx context.Context, req Request) error {
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(body)
	fingerprint := hex.EncodeToString(sum[:])
	ct, err := p.DB.Exec(ctx, `INSERT INTO order_sagas(request_id,request_fingerprint,request_payload,order_id,correlation_id,causation_id,state)
		VALUES($1,$2,$3,$4,$5,$6,'reservation_pending') ON CONFLICT(request_id) DO UPDATE SET updated_at=now()
		WHERE order_sagas.request_fingerprint=EXCLUDED.request_fingerprint AND order_sagas.state IN ('started','reservation_pending')`, req.RequestID, fingerprint, body, req.OrderID, req.CorrelationID, req.CausationID)
	if err == nil && ct.RowsAffected() == 0 {
		return errors.New("request_id was already used by a different or terminal order saga")
	}
	return err
}

func (p *PostgresSagaStore) Reserved(ctx context.Context, requestID, reservationID string) error {
	ct, err := p.DB.Exec(ctx, `UPDATE order_sagas SET reservation_id=$2,state='reserved',attempt_count=0,last_error=NULL,updated_at=now() WHERE request_id=$1 AND state IN ('started','reservation_pending','reserved')`, requestID, reservationID)
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

func (p *PostgresSagaStore) ReservationFailed(ctx context.Context, requestID string, cause error, retryable bool) (bool, error) {
	maxAttempts := p.MaxReservationAttempts
	if maxAttempts < 1 {
		maxAttempts = 10
	}
	tx, err := p.DB.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	var orderID, state string
	var attempts int
	err = tx.QueryRow(ctx, `UPDATE order_sagas SET attempt_count=attempt_count+1,last_error=$2,
		state=CASE WHEN NOT $3 OR attempt_count+1 >= $4 THEN 'dead_letter' ELSE 'reservation_pending' END,
		next_attempt_at=now()+make_interval(secs=>LEAST(300,power(2,LEAST(attempt_count,8))::int)),updated_at=now()
		WHERE request_id=$1 AND state IN ('started','reservation_pending') RETURNING order_id,state,attempt_count`, requestID, cause.Error(), retryable, maxAttempts).Scan(&orderID, &state, &attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, errors.New("order saga is not awaiting reservation")
	}
	if err != nil {
		return false, err
	}
	deadLettered := state == "dead_letter"
	if deadLettered {
		_, err = tx.Exec(ctx, `INSERT INTO order_saga_dead_letters(request_id,order_id,failure_stage,attempt_count,last_error)
			VALUES($1,$2,'reservation',$3,$4) ON CONFLICT(request_id) DO UPDATE SET attempt_count=EXCLUDED.attempt_count,last_error=EXCLUDED.last_error,failed_at=now(),resolved_at=NULL`, requestID, orderID, attempts, cause.Error())
		if err != nil {
			return false, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return false, err
	}
	return deadLettered, nil
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
		Scan(&s.Request.RequestID, &s.Request.OrderID, &s.Request.CorrelationID, &s.Request.CausationID, &s.ReservationID, &s.State, &s.ReleaseReason, &s.Attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return Saga{}, errors.New("order saga not found or has a conflicting terminal action")
	}
	return s, err
}

func (p *PostgresSagaStore) Released(ctx context.Context, requestID string) error {
	_, err := p.DB.Exec(ctx, `UPDATE order_sagas SET state='released',last_error=NULL,updated_at=now() WHERE request_id=$1`, requestID)
	return err
}

func (p *PostgresSagaStore) ReleaseFailed(ctx context.Context, requestID string, cause error) (bool, error) {
	maxAttempts := p.MaxReleaseAttempts
	if maxAttempts < 1 {
		maxAttempts = 10
	}
	tx, err := p.DB.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	var orderID, state, releaseReason string
	var attempts int
	err = tx.QueryRow(ctx, `UPDATE order_sagas SET attempt_count=attempt_count+1,last_error=$2,
		state=CASE WHEN attempt_count+1 >= $3 THEN 'dead_letter' ELSE 'release_pending' END,
		next_attempt_at=now()+make_interval(secs=>LEAST(300,power(2,LEAST(attempt_count,8))::int)),updated_at=now()
		WHERE request_id=$1 AND state='release_pending'
		RETURNING order_id,state,release_reason,attempt_count`, requestID, cause.Error(), maxAttempts).Scan(&orderID, &state, &releaseReason, &attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, errors.New("order saga is not awaiting release")
	}
	if err != nil {
		return false, err
	}
	deadLettered := state == "dead_letter"
	if deadLettered {
		_, err = tx.Exec(ctx, `INSERT INTO order_saga_dead_letters(request_id,order_id,failure_stage,release_reason,attempt_count,last_error)
			VALUES($1,$2,'release',$3,$4,$5) ON CONFLICT(request_id) DO UPDATE SET attempt_count=EXCLUDED.attempt_count,last_error=EXCLUDED.last_error,failed_at=now(),resolved_at=NULL`, requestID, orderID, releaseReason, attempts, cause.Error())
		if err != nil {
			return false, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return false, err
	}
	return deadLettered, nil
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
		if err = rows.Scan(&s.Request.RequestID, &s.Request.OrderID, &s.Request.CorrelationID, &s.Request.CausationID, &s.ReservationID, &s.State, &s.ReleaseReason, &s.Attempts); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func (p *PostgresSagaStore) DueReservations(ctx context.Context, limit int) ([]Saga, error) {
	rows, err := p.DB.Query(ctx, `SELECT request_id,order_id,correlation_id,causation_id,state,attempt_count,request_payload
		FROM order_sagas WHERE state='reservation_pending' AND request_payload IS NOT NULL AND next_attempt_at<=now()
		ORDER BY next_attempt_at LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Saga
	for rows.Next() {
		var s Saga
		var payload []byte
		var requestID, orderID, correlationID, causationID string
		if err = rows.Scan(&requestID, &orderID, &correlationID, &causationID, &s.State, &s.Attempts, &payload); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(payload, &s.Request); err != nil {
			return nil, err
		}
		s.Request.RequestID, s.Request.OrderID = requestID, orderID
		s.Request.CorrelationID, s.Request.CausationID = correlationID, causationID
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
