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
func (p Postgres) MarkPublished(ctx context.Context, ids []string, worker string) error {
	if len(ids) == 0 {
		return nil
	}
	tag, err := p.DB.Exec(ctx, `UPDATE outbox_events SET published_at=now(),attempt_count=attempt_count+1,last_error=NULL,claimed_by=NULL,claimed_until=NULL WHERE id=ANY($1::text[]) AND claimed_by=$2 AND published_at IS NULL`, ids, worker)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != int64(len(ids)) {
		return fmt.Errorf("%d of %d event leases lost before publish acknowledgement", int64(len(ids))-tag.RowsAffected(), len(ids))
	}
	return nil
}
func (p Postgres) MarkFailed(ctx context.Context, failures []relay.Failure, worker string) error {
	if len(failures) == 0 {
		return nil
	}
	ids := make([]string, len(failures))
	messages := make([]string, len(failures))
	for i := range failures {
		ids[i] = failures[i].ID
		messages[i] = failures[i].Message
	}
	tag, err := p.DB.Exec(ctx, `UPDATE outbox_events AS o SET attempt_count=o.attempt_count+1,last_error=f.message,next_attempt_at=now()+interval '1 second'*least(60,power(2,least(o.attempt_count,6))),claimed_by=NULL,claimed_until=NULL FROM unnest($1::text[],$3::text[]) AS f(id,message) WHERE o.id=f.id AND o.claimed_by=$2 AND o.published_at IS NULL`, ids, worker, messages)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != int64(len(failures)) {
		return fmt.Errorf("%d of %d event leases lost before failure update", int64(len(failures))-tag.RowsAffected(), len(failures))
	}
	return nil
}
