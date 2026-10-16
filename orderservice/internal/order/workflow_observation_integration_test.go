package order_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"praxis/orderservice/internal/order"
)

func TestWorkflowObservationIncludesDelayedSuccessorsAndExcludesPublished(t *testing.T) {
	store, db, req := workflowStore(t)
	ctx := context.Background()
	observerDB, err := order.OpenWorkflowObservationDB(ctx, db.Config().ConnString())
	if err != nil {
		t.Fatal(err)
	}
	defer observerDB.Close()
	observer := order.WorkflowOutboxObserver{DB: observerDB}
	var pending, retried int64
	if err := db.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE attempt_count>0) FROM order_workflow_outbox WHERE published_at IS NULL`).Scan(&pending, &retried); err != nil {
		t.Fatal(err)
	}
	if err := store.Start(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := store.Reserved(ctx, req.RequestID, "reservation", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, `UPDATE order_workflow_outbox SET
 occurred_at=now()-interval '2 minutes', next_attempt_at=now()+interval '1 hour',
 attempt_count=CASE WHEN workflow_version=1 THEN 0 ELSE 1 END WHERE order_id=$1`, req.OrderID); err != nil {
		t.Fatal(err)
	}
	check := func(want int64) {
		t.Helper()
		sample := observer.Prometheus(ctx)
		for _, metric := range []string{"order_workflow_outbox_observation_up 1\n", fmt.Sprintf("order_workflow_outbox_pending %d\n", want), fmt.Sprintf("order_workflow_outbox_retried_pending %d\n", retried+1)} {
			if !strings.Contains(sample, metric) {
				t.Fatal(sample)
			}
		}
		var age float64
		for _, line := range strings.Split(sample, "\n") {
			if strings.HasPrefix(line, "order_workflow_outbox_oldest_pending_age_seconds ") {
				if _, err := fmt.Sscanf(line, "order_workflow_outbox_oldest_pending_age_seconds %f", &age); err != nil {
					t.Fatal(err)
				}
			}
		}
		if age < 120 {
			t.Fatalf("oldest age=%f", age)
		}
	}
	check(pending + 2)
	if _, err := db.Exec(ctx, `UPDATE order_workflow_outbox SET published_at=now() WHERE order_id=$1 AND workflow_version=1`, req.OrderID); err != nil {
		t.Fatal(err)
	}
	check(pending + 1)
	var readOnly string
	if err := observerDB.QueryRow(ctx, "SHOW default_transaction_read_only").Scan(&readOnly); err != nil || readOnly != "on" {
		t.Fatalf("read-only=%s err=%v", readOnly, err)
	}
}
