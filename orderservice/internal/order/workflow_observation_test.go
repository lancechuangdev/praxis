package order

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

type workflowObservationRow struct {
	pending, retried int64
	age              float64
	err              error
}

func (r workflowObservationRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	*dest[0].(*int64), *dest[1].(*int64), *dest[2].(*float64) = r.pending, r.retried, r.age
	return nil
}

type workflowObservationDBFunc func(context.Context, string, ...any) pgx.Row

func (f workflowObservationDBFunc) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return f(ctx, sql, args...)
}

func TestWorkflowObservationEmptyBacklogAndFailures(t *testing.T) {
	for _, tc := range []struct {
		name string
		row  workflowObservationRow
		want string
	}{
		{"empty", workflowObservationRow{}, "order_workflow_outbox_observation_up 1\norder_workflow_outbox_pending 0\norder_workflow_outbox_retried_pending 0\norder_workflow_outbox_oldest_pending_age_seconds 0.000\n"},
		{"pending", workflowObservationRow{pending: 5, retried: 2, age: 12.5}, "order_workflow_outbox_observation_up 1\norder_workflow_outbox_pending 5\norder_workflow_outbox_retried_pending 2\norder_workflow_outbox_oldest_pending_age_seconds 12.500\n"},
		{"failure", workflowObservationRow{err: errors.New("secret database details")}, "order_workflow_outbox_observation_up 0\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := WorkflowOutboxObserver{DB: workflowObservationDBFunc(func(ctx context.Context, _ string, _ ...any) pgx.Row {
				deadline, ok := ctx.Deadline()
				if !ok || time.Until(deadline) > workflowObservationTimeout {
					t.Error("unbounded observation")
				}
				return tc.row
			})}
			if got := o.Prometheus(context.Background()); got != tc.want {
				t.Fatalf("got %q want %q", got, tc.want)
			}
		})
	}
	if got := (WorkflowOutboxObserver{}).Prometheus(context.Background()); got != "order_workflow_outbox_observation_up 0\n" {
		t.Fatal(got)
	}
}

func TestWorkflowObservationPreservesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	o := WorkflowOutboxObserver{DB: workflowObservationDBFunc(func(ctx context.Context, _ string, _ ...any) pgx.Row {
		if !errors.Is(ctx.Err(), context.Canceled) {
			t.Error("lost cancellation")
		}
		return workflowObservationRow{err: ctx.Err()}
	})}
	if got := o.Prometheus(ctx); strings.Contains(got, "pending") {
		t.Fatal("failed observation emitted healthy gauges")
	}
}

func TestWorkflowObservationPoolIsBoundedAndReadOnly(t *testing.T) {
	db, err := OpenWorkflowObservationDB(context.Background(), "postgres://localhost/test?pool_max_conns=20&pool_min_conns=10&pool_min_idle_conns=5&statement_timeout=0&default_transaction_read_only=off")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cfg := db.Config()
	if cfg.MaxConns != 1 || cfg.MinConns != 0 || cfg.MinIdleConns != 0 || cfg.ConnConfig.ConnectTimeout != workflowObservationTimeout {
		t.Fatal("unbounded observer pool")
	}
	if cfg.ConnConfig.RuntimeParams["default_transaction_read_only"] != "on" || cfg.ConnConfig.RuntimeParams["statement_timeout"] != "500ms" {
		t.Fatal("observer lacks server-side bounds")
	}
}
