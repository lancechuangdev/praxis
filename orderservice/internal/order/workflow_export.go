package order

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	lifecycle "praxis/api/events/lifecycle/v1"
	wire "praxis/api/gen/events/lifecycle/v1"
)

func ExportWorkflow(ctx context.Context, db *pgxpool.Pool, output io.Writer) error {
	tx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	// Every enrolled saga must have a complete captured chain ending at current state.
	var missing bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM order_sagas s LEFT JOIN (SELECT order_id,min(workflow_version) first,max(workflow_version) last,count(*) n FROM order_workflow_outbox GROUP BY order_id) o ON o.order_id=s.order_id WHERE o.order_id IS NULL OR o.first<>1 OR o.last<>s.workflow_version OR o.n<>s.workflow_version)`).Scan(&missing); err != nil {
		return err
	}
	if missing {
		return errors.New("workflow history requires reviewed bootstrap or capture-gap repair")
	}
	rows, err := tx.Query(ctx, `SELECT event_id,order_id,workflow_version,payload FROM order_workflow_outbox ORDER BY order_id,workflow_version`)
	if err != nil {
		return err
	}
	latest := map[string]*wire.OrderWorkflowSnapshot{}
	encoder := json.NewEncoder(output)
	offset := int64(0)
	for rows.Next() {
		if offset >= 100000 {
			rows.Close()
			return errors.New("workflow export exceeds record bound")
		}
		var eventID, orderID string
		var version int64
		var snapshot []byte
		if err := rows.Scan(&eventID, &orderID, &version, &snapshot); err != nil {
			rows.Close()
			return err
		}
		raw, err := EncodeWorkflowOutbox(eventID, orderID, version, snapshot)
		if err != nil {
			rows.Close()
			return err
		}
		e, err := lifecycle.Decode(raw)
		if err != nil {
			rows.Close()
			return err
		}
		latest[orderID] = e.GetOrderWorkflow()
		if err := encoder.Encode(struct {
			Topic     string `json:"topic"`
			Partition int    `json:"partition"`
			Offset    int64  `json:"offset"`
			Key       []byte `json:"key"`
			Payload   []byte `json:"payload"`
		}{"order.events.v1", 0, offset, []byte(orderID), raw}); err != nil {
			rows.Close()
			return err
		}
		offset++
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if offset == 0 {
		return errors.New("no captured workflows")
	}
	for orderID, w := range latest {
		var raw []byte
		var requestID, state, reservation, release string
		if err := tx.QueryRow(ctx, `SELECT request_id,state,request_payload,COALESCE(reservation_id,''),COALESCE(release_reason,'') FROM order_sagas WHERE order_id=$1`, orderID).Scan(&requestID, &state, &raw, &reservation, &release); err != nil {
			return err
		}
		var request Request
		if err := json.Unmarshal(raw, &request); err != nil {
			return err
		}
		if state == "started" {
			state = "reservation_pending"
		}
		expectedState := wire.OrderWorkflowState_value["ORDER_WORKFLOW_STATE_"+strings.ToUpper(state)]
		price := request.Price
		if request.OrderType != "limit" {
			price = ""
		}
		if requestID != w.RequestId || request.UserID != w.UserId || request.OrderID != w.OrderId || request.Symbol != w.Symbol || request.Side != w.Side || request.OrderType != w.OrderType || request.Quantity != w.Quantity || price != w.Price || request.ReserveAssetID != w.ReserveAssetId || request.ReserveAmountAtomic != w.ReserveAmountAtomic || reservation != w.ReservationId || release != w.ReleaseReason || int32(w.State) != expectedState {
			return errors.New("authoritative saga differs from latest captured snapshot")
		}
	}
	return tx.Commit(ctx)
}
