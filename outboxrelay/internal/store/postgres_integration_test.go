package store_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"praxis/outboxrelay/internal/relay"
	"praxis/outboxrelay/internal/store"
	"praxis/outboxrelay/migrations"
)

func TestBatchPublicationOutcomes(t *testing.T) {
	url := os.Getenv("OUTBOX_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("OUTBOX_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	db, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = migrations.Apply(ctx, db); err != nil {
		t.Fatal(err)
	}

	prefix := fmt.Sprintf("phase5-%d", time.Now().UnixNano())
	ids := []string{prefix + "-published-1", prefix + "-published-2", prefix + "-failed"}
	worker := prefix + "-worker"
	for _, id := range ids {
		if _, err = db.Exec(ctx, `INSERT INTO outbox_events(id,topic,event_type,aggregate_id,message_key,payload,occurred_at,claimed_by,claimed_until) VALUES($1,'phase5','test',$1,$1,'{}',now(),$2,now()+interval '1 minute')`, id, worker); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		_, _ = db.Exec(context.Background(), `DELETE FROM outbox_events WHERE id=ANY($1::text[])`, ids)
	})

	repo := store.Postgres{DB: db}
	if err = repo.MarkPublished(ctx, ids[:2], worker); err != nil {
		t.Fatal(err)
	}
	if err = repo.MarkFailed(ctx, []relay.Failure{{ID: ids[2], Message: "broker unavailable"}}, worker); err != nil {
		t.Fatal(err)
	}

	var published, failed int
	if err = db.QueryRow(ctx, `SELECT count(*) FILTER (WHERE published_at IS NOT NULL), count(*) FILTER (WHERE published_at IS NULL AND last_error='broker unavailable' AND attempt_count=1) FROM outbox_events WHERE id=ANY($1::text[])`, ids).Scan(&published, &failed); err != nil {
		t.Fatal(err)
	}
	if published != 2 || failed != 1 {
		t.Fatalf("published=%d failed=%d", published, failed)
	}
}
