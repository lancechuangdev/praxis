package order_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	lifecycle "praxis/api/events/lifecycle/v1"
	"praxis/orderservice/internal/order"

	"github.com/jackc/pgx/v5/pgxpool"
)

func workflowStore(t *testing.T) (*order.PostgresSagaStore, *pgxpool.Pool, order.Request) {
	t.Helper()
	_, base := sagaTestStore(t)
	db, err := order.OpenSagaDBWithLifecycle(context.Background(), base.Config().ConnString(), true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	prefix := fmt.Sprintf("workflow-it-%d", time.Now().UnixNano())
	req := order.Request{RequestID: prefix + "-request", OrderID: prefix + "-order", UserID: "alice", Symbol: "BTC-USDT", Side: "buy", OrderType: "market", Quantity: "9007199254740993", ReserveAssetID: "USDT", ReserveAmountAtomic: "900719925474099300"}
	t.Cleanup(func() {
		_, _ = db.Exec(context.Background(), `DELETE FROM order_workflow_outbox WHERE order_id=$1`, req.OrderID)
		_, _ = db.Exec(context.Background(), `DELETE FROM order_saga_dead_letters WHERE request_id=$1`, req.RequestID)
		_, _ = db.Exec(context.Background(), `DELETE FROM order_sagas WHERE request_id=$1`, req.RequestID)
	})
	return &order.PostgresSagaStore{DB: db}, db, req
}

func workflowCount(t *testing.T, db *pgxpool.Pool, id string, want int) {
	t.Helper()
	var count int
	if err := db.QueryRow(context.Background(), `SELECT count(*) FROM order_workflow_outbox WHERE order_id=$1`, id).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("outbox count=%d, want %d", count, want)
	}
}

func TestWorkflowCaptureSequenceAndIdempotentReplays(t *testing.T) {
	store, db, req := workflowStore(t)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if err := store.Start(ctx, req); err != nil {
			t.Fatal(err)
		}
	}
	workflowCount(t, db, req.OrderID, 1)
	if _, err := store.ReservationFailed(ctx, req.RequestID, fmt.Errorf("temporary outage"), true, ""); err != nil {
		t.Fatal(err)
	}
	workflowCount(t, db, req.OrderID, 1)
	for i := 0; i < 2; i++ {
		if err := store.Reserved(ctx, req.RequestID, "reservation-1", ""); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		if err := store.Accepted(ctx, req.RequestID, ""); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		if _, err := store.RequestRelease(ctx, req.OrderID, "execution_complete", ""); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		if err := store.Released(ctx, req.RequestID, ""); err != nil {
			t.Fatal(err)
		}
	}
	workflowCount(t, db, req.OrderID, 5)
	rows, err := db.Query(ctx, `SELECT event_id,workflow_version,payload FROM order_workflow_outbox WHERE order_id=$1 ORDER BY workflow_version`, req.OrderID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	version := int64(0)
	for rows.Next() {
		var id string
		var got int64
		var snapshot []byte
		if err := rows.Scan(&id, &got, &snapshot); err != nil {
			t.Fatal(err)
		}
		version++
		if got != version {
			t.Fatalf("version=%d want %d", got, version)
		}
		raw, err := order.EncodeWorkflowOutbox(id, req.OrderID, got, snapshot)
		if err != nil {
			t.Fatal(err)
		}
		event, err := lifecycle.Decode(raw)
		if err != nil {
			t.Fatal(err)
		}
		if event.GetOrderWorkflow().Quantity != req.Quantity || event.CorrelationId != req.RequestID || event.CausationId != req.RequestID {
			t.Fatalf("snapshot lost exact values or business context: %v", event)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestWorkflowCaptureRollsBackWithFailedOutboxInsert(t *testing.T) {
	store, db, req := workflowStore(t)
	ctx := context.Background()
	if err := store.Start(ctx, req); err != nil {
		t.Fatal(err)
	}
	// Simulate a broken outbox constraint/write before the next domain transition.
	_, err := db.Exec(ctx, `INSERT INTO order_workflow_outbox(event_id,order_id,workflow_version,occurred_at,payload)
 VALUES('collision',$1,2,now(),'{}')`, req.OrderID)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Reserved(ctx, req.RequestID, "reservation-1", ""); err == nil {
		t.Fatal("expected outbox insertion failure")
	}
	var state string
	var version int64
	if err := db.QueryRow(ctx, `SELECT state,workflow_version FROM order_sagas WHERE request_id=$1`, req.RequestID).Scan(&state, &version); err != nil {
		t.Fatal(err)
	}
	if state != "reservation_pending" || version != 1 {
		t.Fatalf("domain update survived outbox failure: %s/%d", state, version)
	}
}

func TestWorkflowCaptureConcurrentReplaysAndStaleLease(t *testing.T) {
	store, db, req := workflowStore(t)
	ctx := context.Background()
	if err := store.Start(ctx, req); err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		group.Add(1)
		go func() { defer group.Done(); errs <- store.Reserved(ctx, req.RequestID, "reservation-1", "") }()
	}
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	workflowCount(t, db, req.OrderID, 2)
	if _, err := db.Exec(ctx, `UPDATE order_sagas SET claim_owner='current-worker',claim_expires_at=now()+interval '1 minute' WHERE request_id=$1`, req.RequestID); err != nil {
		t.Fatal(err)
	}
	if err := store.Accepted(ctx, req.RequestID, "stale-worker"); err == nil {
		t.Fatal("stale lease advanced workflow")
	}
	workflowCount(t, db, req.OrderID, 2)
}

func TestWorkflowCaptureDisabledKeepsExplicitVersionGap(t *testing.T) {
	store, db, req := workflowStore(t)
	ctx := context.Background()
	if err := store.Start(ctx, req); err != nil {
		t.Fatal(err)
	}
	disabled, err := order.OpenSagaDB(ctx, db.Config().ConnString())
	if err != nil {
		t.Fatal(err)
	}
	defer disabled.Close()
	other := &order.PostgresSagaStore{DB: disabled}
	if err := other.Reserved(ctx, req.RequestID, "reservation-1", ""); err != nil {
		t.Fatal(err)
	}
	workflowCount(t, db, req.OrderID, 1)
	if err := store.Accepted(ctx, req.RequestID, ""); err != nil {
		t.Fatal(err)
	}
	workflowCount(t, db, req.OrderID, 2)
	var version int64
	if err := db.QueryRow(ctx, `SELECT max(workflow_version) FROM order_workflow_outbox WHERE order_id=$1`, req.OrderID).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 3 {
		t.Fatalf("capture rollback concealed missing version: %d", version)
	}
}

func TestWorkflowSourceRejectsCoordinateMismatch(t *testing.T) {
	store, db, req := workflowStore(t)
	ctx := context.Background()
	if err := store.Start(ctx, req); err != nil {
		t.Fatal(err)
	}
	var id string
	var snapshot []byte
	if err := db.QueryRow(ctx, `SELECT event_id,payload FROM order_workflow_outbox WHERE order_id=$1`, req.OrderID).Scan(&id, &snapshot); err != nil {
		t.Fatal(err)
	}
	for _, args := range []struct {
		id, order string
		version   int64
	}{{"different", req.OrderID, 1}, {id, "different", 1}, {id, req.OrderID, 2}} {
		if _, err := order.EncodeWorkflowOutbox(args.id, args.order, args.version, snapshot); err == nil {
			t.Fatal("mismatched source coordinates accepted")
		}
	}
}

func TestWorkflowCaptureTerminalFailureSnapshots(t *testing.T) {
	for _, stage := range []string{"reservation", "release"} {
		t.Run(stage, func(t *testing.T) {
			store, db, req := workflowStore(t)
			ctx := context.Background()
			if err := store.Start(ctx, req); err != nil {
				t.Fatal(err)
			}
			if stage == "reservation" {
				if dead, err := store.ReservationFailed(ctx, req.RequestID, fmt.Errorf("private dependency diagnostic"), false, ""); err != nil || !dead {
					t.Fatalf("dead=%v err=%v", dead, err)
				}
			} else {
				if err := store.Reserved(ctx, req.RequestID, "reservation-1", ""); err != nil {
					t.Fatal(err)
				}
				if _, err := store.RequestRelease(ctx, req.OrderID, "matching_failed", ""); err != nil {
					t.Fatal(err)
				}
				store.MaxReleaseAttempts = 1
				if dead, err := store.ReleaseFailed(ctx, req.RequestID, fmt.Errorf("private dependency diagnostic"), ""); err != nil || !dead {
					t.Fatalf("dead=%v err=%v", dead, err)
				}
			}
			var id string
			var version int64
			var snapshot []byte
			if err := db.QueryRow(ctx, `SELECT event_id,workflow_version,payload FROM order_workflow_outbox WHERE order_id=$1 ORDER BY workflow_version DESC LIMIT 1`, req.OrderID).Scan(&id, &version, &snapshot); err != nil {
				t.Fatal(err)
			}
			raw, err := order.EncodeWorkflowOutbox(id, req.OrderID, version, snapshot)
			if err != nil {
				t.Fatal(err)
			}
			event, err := lifecycle.Decode(raw)
			if err != nil {
				t.Fatal(err)
			}
			if event.GetOrderWorkflow().State.String() != "ORDER_WORKFLOW_STATE_DEAD_LETTER" {
				t.Fatal("terminal failure state missing")
			}
		})
	}
}

func TestWorkflowFirstCaptureAfterDisabledStartRequiresBootstrap(t *testing.T) {
	store, db, req := workflowStore(t)
	ctx := context.Background()
	disabled, err := order.OpenSagaDB(ctx, db.Config().ConnString())
	if err != nil {
		t.Fatal(err)
	}
	defer disabled.Close()
	if err := (&order.PostgresSagaStore{DB: disabled}).Start(ctx, req); err != nil {
		t.Fatal(err)
	}
	workflowCount(t, db, req.OrderID, 0)
	if err := store.Reserved(ctx, req.RequestID, "reservation-1", ""); err != nil {
		t.Fatal(err)
	}
	var first int64
	if err := db.QueryRow(ctx, `SELECT min(workflow_version) FROM order_workflow_outbox WHERE order_id=$1`, req.OrderID).Scan(&first); err != nil {
		t.Fatal(err)
	}
	if first != 2 {
		t.Fatalf("missing baseline concealed by version %d", first)
	}
}
