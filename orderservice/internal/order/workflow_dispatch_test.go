package order_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"praxis/orderservice/internal/order"
)

func TestWorkflowDispatcherRejectsIncompleteConfiguration(t *testing.T) {
	publisher := workflowPublisherFunc(func(context.Context, order.WorkflowPublication) error { t.Fatal("unexpected publication"); return nil })
	for _, d := range []order.WorkflowDispatcher{
		{},
		{Publisher: publisher, Timeout: time.Second},
		{DB: &pgxpool.Pool{}, Timeout: time.Second},
		{DB: &pgxpool.Pool{}, Publisher: publisher},
		{DB: &pgxpool.Pool{}, Publisher: publisher, Timeout: -time.Second},
	} {
		if found, err := d.DispatchOnce(context.Background()); found || err == nil {
			t.Fatalf("invalid dispatcher found=%v err=%v", found, err)
		}
	}
}
