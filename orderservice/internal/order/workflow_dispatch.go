package order

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// WorkflowPublication is an immutable, validated wire fact. Publishers must
// synchronously acknowledge durable delivery, key by OrderID, and preserve ID.
type WorkflowPublication struct {
	EventID, OrderID string
	Version          int64
	Payload          []byte
}

type WorkflowPublisher interface {
	PublishWorkflow(context.Context, WorkflowPublication) error
}

// WorkflowDispatcher is a source-side dispatch foundation, not a Kafka adapter.
// Each call locks and sends at most one fact. Keep the transaction open through
// acknowledgement: a time-limited lease could allow a later version to overtake
// a still-running sender. Separate orders may be dispatched concurrently.
// Delivery remains at least once; consumers must deduplicate stable event IDs
// and reject stale versions, including late sends after connection loss.
type WorkflowDispatcher struct {
	DB        *pgxpool.Pool
	Publisher WorkflowPublisher
	Timeout   time.Duration
}

func (d WorkflowDispatcher) DispatchOnce(ctx context.Context) (bool, error) {
	if d.DB == nil || d.Publisher == nil || d.Timeout <= 0 {
		return false, errors.New("workflow dispatch requires database, publisher and positive timeout")
	}
	ctx, cancel := context.WithTimeout(ctx, d.Timeout)
	defer cancel()
	tx, err := d.DB.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = tx.Rollback(cleanup)
	}()
	// NOT EXISTS must include locked and delayed predecessors. SKIP LOCKED alone
	// permits another worker to publish version N+1 before version N completes.
	rows, err := tx.Query(ctx, `SELECT o.event_id,o.order_id,o.workflow_version,o.payload
 FROM order_workflow_outbox o
 WHERE o.published_at IS NULL AND o.next_attempt_at<=now()
 AND NOT EXISTS (SELECT 1 FROM order_workflow_outbox earlier
 WHERE earlier.order_id=o.order_id AND earlier.workflow_version<o.workflow_version
 AND earlier.published_at IS NULL)
 ORDER BY o.occurred_at,o.event_id FOR UPDATE OF o SKIP LOCKED LIMIT 1`)
	if err != nil {
		return false, err
	}
	var event WorkflowPublication
	var snapshot []byte
	if !rows.Next() {
		err = rows.Err()
		rows.Close()
		return false, err
	}
	err = rows.Scan(&event.EventID, &event.OrderID, &event.Version, &snapshot)
	rows.Close()
	if err != nil {
		return false, err
	}
	event.Payload, err = EncodeWorkflowOutbox(event.EventID, event.OrderID, event.Version, snapshot)
	if err == nil {
		err = d.Publisher.PublishWorkflow(ctx, event)
	}
	if err != nil {
		// Persist bounded backoff when the transaction is still usable. On timeout
		// or lost connection rollback leaves the original fact available for retry.
		_, updateErr := tx.Exec(ctx, `UPDATE order_workflow_outbox SET attempt_count=attempt_count+1,
   next_attempt_at=now()+interval '1 second'*least(60,power(2,least(attempt_count,6)))
   WHERE event_id=$1`, event.EventID)
		if updateErr == nil {
			updateErr = tx.Commit(ctx)
		}
		return true, errors.Join(fmt.Errorf("dispatch workflow %s: %w", event.EventID, err), updateErr)
	}
	_, err = tx.Exec(ctx, `UPDATE order_workflow_outbox SET published_at=now(),attempt_count=attempt_count+1 WHERE event_id=$1`, event.EventID)
	if err != nil {
		return true, err
	}
	return true, tx.Commit(ctx)
}
