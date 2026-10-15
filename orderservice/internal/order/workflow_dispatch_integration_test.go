package order_test

import (
	"context"
	"errors"
	"testing"
	"time"

	lifecycle "praxis/api/events/lifecycle/v1"
	"praxis/orderservice/internal/order"
)

type workflowPublisherFunc func(context.Context, order.WorkflowPublication) error

func (f workflowPublisherFunc) PublishWorkflow(ctx context.Context, e order.WorkflowPublication) error {
	return f(ctx, e)
}

func TestWorkflowDispatchOrderingAndRetry(t *testing.T) {
	store, db, req := workflowStore(t)
	ctx := context.Background()
	if err := store.Start(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := store.Reserved(ctx, req.RequestID, "reservation", ""); err != nil {
		t.Fatal(err)
	}
	var first order.WorkflowPublication
	entered, release := make(chan struct{}), make(chan struct{})
	d := order.WorkflowDispatcher{DB: db, Timeout: 5 * time.Second, Publisher: workflowPublisherFunc(func(ctx context.Context, e order.WorkflowPublication) error {
		first = e
		close(entered)
		select {
		case <-release:
			return errors.New("broker unavailable")
		case <-ctx.Done():
			return ctx.Err()
		}
	})}
	done := make(chan error, 1)
	go func() { _, err := d.DispatchOnce(ctx); done <- err }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("dispatch never entered publisher")
	}
	second := order.WorkflowDispatcher{DB: db, Timeout: time.Second, Publisher: workflowPublisherFunc(func(context.Context, order.WorkflowPublication) error {
		t.Error("successor overtook locked predecessor")
		return nil
	})}
	if found, err := second.DispatchOnce(ctx); err != nil || found {
		t.Fatalf("competing dispatch found=%v err=%v", found, err)
	}
	close(release)
	if err := <-done; err == nil {
		t.Fatal("expected publish failure")
	}
	if found, err := second.DispatchOnce(ctx); err != nil || found {
		t.Fatalf("backoff allowed successor: found=%v err=%v", found, err)
	}
	if _, err := db.Exec(ctx, `UPDATE order_workflow_outbox SET next_attempt_at=now() WHERE order_id=$1`, req.OrderID); err != nil {
		t.Fatal(err)
	}
	var versions []int64
	second.Publisher = workflowPublisherFunc(func(_ context.Context, e order.WorkflowPublication) error {
		decoded, err := lifecycle.Decode(e.Payload)
		if err != nil {
			return err
		}
		if decoded.EventId != e.EventID || decoded.GetOrderWorkflow().OrderId != e.OrderID {
			t.Error("wire coordinates changed")
		}
		if e.Version == 1 && (e.EventID != first.EventID || string(e.Payload) != string(first.Payload)) {
			t.Error("retry changed immutable fact")
		}
		versions = append(versions, e.Version)
		return nil
	})
	for range 2 {
		if found, err := second.DispatchOnce(ctx); err != nil || !found {
			t.Fatalf("dispatch found=%v err=%v", found, err)
		}
	}
	if len(versions) != 2 || versions[0] != 1 || versions[1] != 2 {
		t.Fatalf("versions=%v", versions)
	}
	if found, err := second.DispatchOnce(ctx); err != nil || found {
		t.Fatalf("published replay: found=%v err=%v", found, err)
	}
}

func TestWorkflowDispatchAcknowledgementCrashWindow(t *testing.T) {
	store, db, req := workflowStore(t)
	ctx := context.Background()
	if err := store.Start(ctx, req); err != nil {
		t.Fatal(err)
	}
	var first order.WorkflowPublication
	// Cancel after external acknowledgement but before the database mark commits.
	sendCtx, cancel := context.WithCancel(ctx)
	d := order.WorkflowDispatcher{DB: db, Timeout: time.Second, Publisher: workflowPublisherFunc(func(_ context.Context, e order.WorkflowPublication) error { first = e; cancel(); return nil })}
	if found, err := d.DispatchOnce(sendCtx); !found || err == nil {
		t.Fatalf("cancelled acknowledgement found=%v err=%v", found, err)
	}
	d.Publisher = workflowPublisherFunc(func(_ context.Context, e order.WorkflowPublication) error {
		if e.EventID != first.EventID || string(e.Payload) != string(first.Payload) {
			t.Error("crash retry changed fact")
		}
		return nil
	})
	if found, err := d.DispatchOnce(ctx); !found || err != nil {
		t.Fatalf("crash retry found=%v err=%v", found, err)
	}
}

func TestWorkflowDispatchInvalidFactBlocksSuccessor(t *testing.T) {
	store, db, req := workflowStore(t)
	ctx := context.Background()
	if err := store.Start(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := store.Reserved(ctx, req.RequestID, "reservation", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, `UPDATE order_workflow_outbox SET payload='{}' WHERE order_id=$1 AND workflow_version=1`, req.OrderID); err != nil {
		t.Fatal(err)
	}
	d := order.WorkflowDispatcher{DB: db, Timeout: time.Second, Publisher: workflowPublisherFunc(func(context.Context, order.WorkflowPublication) error {
		t.Error("invalid fact was published")
		return nil
	})}
	if found, err := d.DispatchOnce(ctx); !found || err == nil {
		t.Fatalf("invalid fact found=%v err=%v", found, err)
	}
	if found, err := d.DispatchOnce(ctx); found || err != nil {
		t.Fatalf("invalid predecessor did not block successor: found=%v err=%v", found, err)
	}
	var attempts int
	if err := db.QueryRow(ctx, `SELECT attempt_count FROM order_workflow_outbox WHERE order_id=$1 AND workflow_version=1`, req.OrderID).Scan(&attempts); err != nil || attempts != 1 {
		t.Fatalf("attempts=%d err=%v", attempts, err)
	}
}
