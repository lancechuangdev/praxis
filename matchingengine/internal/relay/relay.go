package relay

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"praxis/matchingengine/internal/kafkaauth"
)

type Worker struct {
	DB           *pgxpool.Pool
	Writer       *kafka.Writer
	PollInterval time.Duration
}
type event struct {
	id, topic, key string
	payload        []byte
}

func New(db *pgxpool.Pool, brokers []string, auth kafkaauth.Config) *Worker {
	return &Worker{DB: db, PollInterval: 100 * time.Millisecond, Writer: &kafka.Writer{Addr: kafka.TCP(brokers...), Transport: auth.Transport(), Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll, Async: false, BatchSize: 500, BatchBytes: 512 * 1024, BatchTimeout: 5 * time.Millisecond, Compression: kafka.Lz4, MaxAttempts: 5}}
}
func (w *Worker) Close() error { return w.Writer.Close() }
func (w *Worker) Run(ctx context.Context) error {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			count, err := w.publishBatch(ctx)
			if err != nil {
				return err
			}
			if count == 0 {
				timer.Reset(w.PollInterval)
			} else {
				timer.Reset(0)
			}
		}
	}
}
func (w *Worker) publishBatch(ctx context.Context) (int, error) {
	tx, err := w.DB.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(context.Background())
	rows, err := tx.Query(ctx, `SELECT id,topic,message_key,payload FROM outbox_events WHERE published_at IS NULL AND next_attempt_at<=now() ORDER BY sequence_number FOR UPDATE SKIP LOCKED LIMIT 500`)
	if err != nil {
		return 0, err
	}
	var events []event
	for rows.Next() {
		var e event
		if err = rows.Scan(&e.id, &e.topic, &e.key, &e.payload); err != nil {
			rows.Close()
			return 0, err
		}
		events = append(events, e)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return 0, err
	}
	if len(events) == 0 {
		if err = tx.Commit(ctx); err != nil {
			return 0, err
		}
		return 0, nil
	}
	messages := make([]kafka.Message, len(events))
	ids := make([]string, len(events))
	for i, e := range events {
		ids[i] = e.id
		messages[i] = kafka.Message{Topic: e.topic, Key: []byte(e.key), Value: e.payload, Headers: []kafka.Header{{Key: "event-id", Value: []byte(e.id)}}}
	}
	if err = w.Writer.WriteMessages(ctx, messages...); err != nil {
		return 0, err
	}
	if _, err = tx.Exec(ctx, `UPDATE outbox_events SET published_at=now(),attempt_count=attempt_count+1 WHERE id=ANY($1::text[])`, ids); err != nil {
		return 0, err
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(events), nil
}
