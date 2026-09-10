package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"praxis/outboxrelay/internal/relay"
)

type Postgres struct{ DB *pgxpool.Pool }

func (p Postgres) Claim(ctx context.Context, worker string, limit int, lease time.Duration) ([]relay.Event, error) {
	rows, err := p.DB.Query(ctx, `WITH candidates AS (SELECT id FROM outbox_events WHERE published_at IS NULL AND next_attempt_at<=now() AND (claimed_until IS NULL OR claimed_until<now()) ORDER BY sequence_number FOR UPDATE SKIP LOCKED LIMIT $1) UPDATE outbox_events o SET claimed_by=$2,claimed_until=now()+($3*interval '1 millisecond') FROM candidates c WHERE o.id=c.id RETURNING o.sequence_number,o.id,o.topic,o.event_type,o.message_key,o.payload`, limit, worker, lease.Milliseconds())
	if err != nil {
		return nil, fmt.Errorf("claim outbox: %w", err)
	}
	defer rows.Close()
	var out []relay.Event
	for rows.Next() {
		var e relay.Event
		if err = rows.Scan(&e.SequenceNumber, &e.ID, &e.Topic, &e.EventType, &e.MessageKey, &e.Payload); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
func (p Postgres) MarkPublished(ctx context.Context, id, worker string) error {
	tag, err := p.DB.Exec(ctx, `UPDATE outbox_events SET published_at=now(),attempt_count=attempt_count+1,last_error=NULL,claimed_by=NULL,claimed_until=NULL WHERE id=$1 AND claimed_by=$2 AND published_at IS NULL`, id, worker)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("event %s lease lost before publish acknowledgement", id)
	}
	return nil
}
func (p Postgres) MarkFailed(ctx context.Context, id, worker, message string) error {
	tag, err := p.DB.Exec(ctx, `UPDATE outbox_events SET attempt_count=attempt_count+1,last_error=$3,next_attempt_at=now()+interval '1 second'*least(60,power(2,least(attempt_count,6))),claimed_by=NULL,claimed_until=NULL WHERE id=$1 AND claimed_by=$2 AND published_at IS NULL`, id, worker, message)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("event %s lease lost before failure update", id)
	}
	return nil
}
