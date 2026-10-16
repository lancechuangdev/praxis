package order

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const workflowObservationTimeout = 500 * time.Millisecond

// OpenWorkflowObservationDB reserves a separate, lazy single-connection pool.
// Read-only sessions and a server statement timeout bound monitoring independently
// of the request pool. Include this connection in the per-pod database budget.
func OpenWorkflowObservationDB(ctx context.Context, url string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns, cfg.MinConns, cfg.MinIdleConns = 1, 0, 0
	cfg.ConnConfig.ConnectTimeout = workflowObservationTimeout
	cfg.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "500ms"
	cfg.ConnConfig.RuntimeParams["application_name"] = "order-workflow-observer"
	return pgxpool.NewWithConfig(ctx, cfg)
}

type WorkflowObservationDB interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

type WorkflowOutboxObserver struct{ DB WorkflowObservationDB }

// Prometheus collects a single consistent snapshot of unpublished source facts,
// including delayed retries and blocked successors. Failures emit up=0 and omit
// gauges; an unavailable source must never look like an empty backlog. No raw
// database errors, identifiers, or payloads are exposed.
func (o WorkflowOutboxObserver) Prometheus(ctx context.Context) string {
	if o.DB == nil {
		return "order_workflow_outbox_observation_up 0\n"
	}
	ctx, cancel := context.WithTimeout(ctx, workflowObservationTimeout)
	defer cancel()
	var pending, retried int64
	var age float64
	err := o.DB.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE attempt_count>0),
 COALESCE(GREATEST(0,extract(epoch FROM now()-min(occurred_at)))::double precision,0)
 FROM order_workflow_outbox WHERE published_at IS NULL`).Scan(&pending, &retried, &age)
	if err != nil {
		return "order_workflow_outbox_observation_up 0\n"
	}
	return fmt.Sprintf("order_workflow_outbox_observation_up 1\norder_workflow_outbox_pending %d\norder_workflow_outbox_retried_pending %d\norder_workflow_outbox_oldest_pending_age_seconds %.3f\n", pending, retried, age)
}
