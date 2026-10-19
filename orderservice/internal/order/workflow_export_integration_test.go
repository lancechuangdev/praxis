package order_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"praxis/orderservice/internal/order"
)

func TestWorkflowExportValidatesCompleteCapture(t *testing.T) {
	s, db, req := workflowStore(t)
	ctx := context.Background()
	if err := s.Start(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := s.Reserved(ctx, req.RequestID, "reservation", ""); err != nil {
		t.Fatal(err)
	}
	var export bytes.Buffer
	if err := order.ExportWorkflow(ctx, db, &export); err != nil {
		t.Fatal(err)
	}
	if len(strings.Split(strings.TrimSpace(export.String()), "\n")) != 2 {
		t.Fatal(export.String())
	}
	if _, err := db.Exec(ctx, `DELETE FROM order_workflow_outbox WHERE order_id=$1 AND workflow_version=1`, req.OrderID); err != nil {
		t.Fatal(err)
	}
	export.Reset()
	if err := order.ExportWorkflow(ctx, db, &export); err == nil {
		t.Fatal("incomplete workflow capture exported successfully")
	}
}
