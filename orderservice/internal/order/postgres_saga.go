package order

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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

func (p *PostgresSagaStore) Reserved(ctx context.Context, requestID, reservationID, claimOwner string) error {
	ct, err := p.DB.Exec(ctx, `UPDATE order_sagas SET reservation_id=$2,state='reserved',attempt_count=0,last_error=NULL,
		claim_owner=CASE WHEN $3='' THEN NULL ELSE claim_owner END,claim_expires_at=CASE WHEN $3='' THEN NULL ELSE claim_expires_at END,updated_at=now()
		WHERE request_id=$1 AND state IN ('started','reservation_pending','reserved')
		AND (($3='' AND claim_owner IS NULL) OR (claim_owner=$3 AND claim_expires_at>now()))`, requestID, reservationID, claimOwner)
	if err == nil && ct.RowsAffected() != 1 {
		return errors.New("order saga cannot return to reserved state")
	}
	return err
}

func (p *PostgresSagaStore) Accepted(ctx context.Context, requestID, claimOwner string) error {
	ct, err := p.DB.Exec(ctx, `UPDATE order_sagas SET state='accepted',claim_owner=NULL,claim_expires_at=NULL,updated_at=now()
		WHERE request_id=$1 AND state IN ('reserved','accepted') AND (($2='' AND claim_owner IS NULL) OR (claim_owner=$2 AND claim_expires_at>now()))`, requestID, claimOwner)
	if err == nil && ct.RowsAffected() != 1 {
		return errors.New("order saga cannot enter accepted state")
	}
	return err
}

func (p *PostgresSagaStore) ReservationFailed(ctx context.Context, requestID string, cause error, retryable bool, claimOwner string) (bool, error) {
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
		next_attempt_at=now()+make_interval(secs=>random()*LEAST(300,power(2,LEAST(attempt_count,8))::int)),
		claim_owner=NULL,claim_expires_at=NULL,updated_at=now()
		WHERE request_id=$1 AND state IN ('started','reservation_pending') AND (($5='' AND claim_owner IS NULL) OR (claim_owner=$5 AND claim_expires_at>now()))
		RETURNING order_id,state,attempt_count`, requestID, cause.Error(), retryable, maxAttempts, claimOwner).Scan(&orderID, &state, &attempts)
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

func (p *PostgresSagaStore) RequestRelease(ctx context.Context, orderID, reason, claimOwner string) (Saga, error) {
	if reason != "matching_failed" && reason != "execution_complete" {
		return Saga{}, errors.New("invalid release reason")
	}
	var s Saga
	err := p.DB.QueryRow(ctx, `UPDATE order_sagas SET state=CASE WHEN state='released' THEN state ELSE 'release_pending' END,
		release_reason=COALESCE(release_reason,$2),next_attempt_at=now(),updated_at=now()
		WHERE order_id=$1 AND (release_reason IS NULL OR release_reason=$2)
		AND (($3='' AND claim_owner IS NULL) OR (claim_owner=$3 AND claim_expires_at>now()))
		AND (state='released' OR ($2='matching_failed' AND state='reserved') OR ($2='execution_complete' AND state='accepted') OR (state='release_pending' AND release_reason=$2))
		RETURNING request_id,order_id,correlation_id,causation_id,COALESCE(reservation_id,''),state,COALESCE(release_reason,''),attempt_count,COALESCE(claim_owner,'')`, orderID, reason, claimOwner).
		Scan(&s.Request.RequestID, &s.Request.OrderID, &s.Request.CorrelationID, &s.Request.CausationID, &s.ReservationID, &s.State, &s.ReleaseReason, &s.Attempts, &s.ClaimOwner)
	if errors.Is(err, pgx.ErrNoRows) {
		return Saga{}, errors.New("order saga not found or has a conflicting terminal action")
	}
	return s, err
}

func (p *PostgresSagaStore) Released(ctx context.Context, requestID, claimOwner string) error {
	ct, err := p.DB.Exec(ctx, `UPDATE order_sagas SET state='released',last_error=NULL,claim_owner=NULL,claim_expires_at=NULL,updated_at=now()
		WHERE request_id=$1 AND (($2='' AND claim_owner IS NULL) OR (claim_owner=$2 AND claim_expires_at>now()))`, requestID, claimOwner)
	if err == nil && ct.RowsAffected() != 1 {
		return errors.New("order saga release claim is stale")
	}
	return err
}

func (p *PostgresSagaStore) ReleaseFailed(ctx context.Context, requestID string, cause error, claimOwner string) (bool, error) {
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
		next_attempt_at=now()+make_interval(secs=>random()*LEAST(300,power(2,LEAST(attempt_count,8))::int)),
		claim_owner=NULL,claim_expires_at=NULL,updated_at=now()
		WHERE request_id=$1 AND state='release_pending' AND (($4='' AND claim_owner IS NULL) OR (claim_owner=$4 AND claim_expires_at>now()))
		RETURNING order_id,state,release_reason,attempt_count`, requestID, cause.Error(), maxAttempts, claimOwner).Scan(&orderID, &state, &releaseReason, &attempts)
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

func (p *PostgresSagaStore) DueReleases(ctx context.Context, limit int, owner string, lease time.Duration) ([]Saga, error) {
	return p.claim(ctx, "release_pending", limit, owner, lease)
}

func (p *PostgresSagaStore) claim(ctx context.Context, state string, limit int, owner string, lease time.Duration) ([]Saga, error) {
	if owner == "" || lease <= 0 {
		return nil, errors.New("claim owner and positive lease are required")
	}
	rows, err := p.DB.Query(ctx, `WITH due AS (
		SELECT request_id, claim_expires_at IS NOT NULL AS lease_expired FROM order_sagas WHERE state=$1 AND next_attempt_at<=now()
		AND (claim_expires_at IS NULL OR claim_expires_at<=now()) ORDER BY next_attempt_at
		FOR UPDATE SKIP LOCKED LIMIT $2
	) UPDATE order_sagas s SET claim_owner=$3,claim_expires_at=now()+$4::interval,updated_at=now()
	FROM due WHERE s.request_id=due.request_id
	RETURNING s.request_id,s.order_id,s.correlation_id,s.causation_id,COALESCE(s.reservation_id,''),s.state,COALESCE(s.release_reason,''),s.attempt_count,s.request_payload,due.lease_expired`,
		state, limit, owner, fmt.Sprintf("%d microseconds", lease.Microseconds()))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Saga
	for rows.Next() {
		var s Saga
		var payload []byte
		if err = rows.Scan(&s.Request.RequestID, &s.Request.OrderID, &s.Request.CorrelationID, &s.Request.CausationID, &s.ReservationID, &s.State, &s.ReleaseReason, &s.Attempts, &payload, &s.LeaseExpired); err != nil {
			return nil, err
		}
		requestID, orderID, correlationID, causationID := s.Request.RequestID, s.Request.OrderID, s.Request.CorrelationID, s.Request.CausationID
		if state == "reservation_pending" && json.Unmarshal(payload, &s.Request) != nil {
			return nil, errors.New("decode claimed saga request")
		}
		s.Request.RequestID, s.Request.OrderID, s.Request.CorrelationID, s.Request.CausationID = requestID, orderID, correlationID, causationID
		s.ClaimOwner = owner
		out = append(out, s)
	}
	return out, rows.Err()
}

func (p *PostgresSagaStore) Backlog(ctx context.Context) (SagaBacklog, error) {
	var backlog SagaBacklog
	var oldestAgeSeconds float64
	err := p.DB.QueryRow(ctx, `SELECT count(*), COALESCE(EXTRACT(EPOCH FROM now()-min(next_attempt_at)),0)
		FROM order_sagas WHERE state IN ('reservation_pending','release_pending') AND next_attempt_at<=now()`).
		Scan(&backlog.DueCount, &oldestAgeSeconds)
	if err != nil {
		return SagaBacklog{}, err
	}
	backlog.OldestAge = time.Duration(oldestAgeSeconds * float64(time.Second))
	return backlog, nil
}

func (p *PostgresSagaStore) DueReservations(ctx context.Context, limit int, owner string, lease time.Duration) ([]Saga, error) {
	return p.claim(ctx, "reservation_pending", limit, owner, lease)
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
