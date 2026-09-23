package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"praxis/ledgerservice/internal/ledger"
)

type Postgres struct {
	Writer *pgxpool.Pool
	Reader *pgxpool.Pool
}

const reserveForOrderSQL = `
SELECT result_outcome,
       result_reservation_id,
       result_order_id,
       result_status,
       result_original_atomic,
       result_remaining_atomic,
       result_balance_version
FROM reserve_for_order(
    $1::text,
    $2::text,
    $3::numeric,
    $4::text,
    $5::text,
    $6::text,
    $7::text,
    $8::timestamptz,
    $9::text,
    $10::text,
    $11::text,
    $12::jsonb
)`

func New(writer, reader *pgxpool.Pool) *Postgres { return &Postgres{Writer: writer, Reader: reader} }

func stableID(prefix string, parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write([]byte{0})
		h.Write([]byte(p))
	}
	return prefix + hex.EncodeToString(h.Sum(nil)[:16])
}

func begin(ctx context.Context, db *pgxpool.Pool) (pgx.Tx, error) {
	return db.BeginTx(ctx, pgx.TxOptions{})
}

func ensureUser(ctx context.Context, tx pgx.Tx, userID, assetID string) (string, error) {
	id := stableID("uaa_", userID, assetID)
	_, err := tx.Exec(ctx, `INSERT INTO user_asset_accounts(id,user_id,asset_id) VALUES($1,$2,$3) ON CONFLICT(user_id,asset_id) DO NOTHING`, id, userID, assetID)
	if err != nil {
		return "", fmt.Errorf("ensure user asset account: %w", err)
	}
	if err = tx.QueryRow(ctx, `SELECT id FROM user_asset_accounts WHERE user_id=$1 AND asset_id=$2`, userID, assetID).Scan(&id); err != nil {
		return "", err
	}
	_, err = tx.Exec(ctx, `INSERT INTO user_asset_balances(user_asset_account_id) VALUES($1) ON CONFLICT DO NOTHING`, id)
	return id, err
}

func createJournal(ctx context.Context, tx pgx.Tx, id, refType, refID, kind, system, eventID, correlation, causation, description string, at time.Time, metadata any) (bool, error) {
	if at.IsZero() {
		at = time.Now().UTC()
	}
	b, _ := json.Marshal(metadata)
	var inserted string
	err := tx.QueryRow(ctx, `INSERT INTO ledger_journals(id,reference_type,reference_id,journal_type,source_system,source_event_id,correlation_id,causation_id,description,metadata,occurred_at) VALUES($1,$2,$3,$4,$5,$6,NULLIF($7,''),NULLIF($8,''),$9,$10,$11) ON CONFLICT(source_system,source_event_id) DO NOTHING RETURNING id`, id, refType, refID, kind, system, eventID, correlation, causation, description, b, at).Scan(&inserted)
	if errors.Is(err, pgx.ErrNoRows) {
		var existingRef, existingKind string
		if e := tx.QueryRow(ctx, `SELECT id,reference_id,journal_type FROM ledger_journals WHERE source_system=$1 AND source_event_id=$2`, system, eventID).Scan(&id, &existingRef, &existingKind); e != nil {
			return false, e
		}
		if existingRef != refID || existingKind != kind {
			return false, ledger.ErrConflict
		}
		return false, nil
	}
	return true, err
}

func entry(ctx context.Context, tx pgx.Tx, id, journalID, accountID, userAccountID, assetID, custodyID, bucket, side, amount string) error {
	_, err := tx.Exec(ctx, `INSERT INTO ledger_entries(id,journal_id,ledger_account_id,user_asset_account_id,asset_id,custody_position_id,bucket,side,amount_atomic) VALUES($1,$2,$3,NULLIF($4,''),$5,NULLIF($6,''),NULLIF($7,''),$8,$9::numeric)`, id, journalID, accountID, userAccountID, assetID, custodyID, bucket, side, amount)
	return err
}

func outbox(ctx context.Context, tx pgx.Tx, id, eventType, aggregate, key, correlationID, causationID string, data any) error {
	b, err := makeOutboxPayload(id, eventType, aggregate, correlationID, causationID, "", "", time.Now().UTC(), data)
	if err != nil {
		return err
	}
	return insertOutbox(ctx, tx, id, eventType, aggregate, key, b)
}

func makeOutboxPayload(id, eventType, aggregate, correlationID, causationID, traceParent, traceState string, occurredAt time.Time, data any) ([]byte, error) {
	return json.Marshal(map[string]any{"id": id, "type": eventType, "aggregate_id": aggregate, "correlation_id": correlationID, "causation_id": causationID, "trace_parent": traceParent, "trace_state": traceState, "occurred_at": occurredAt, "data": data})
}

func insertOutbox(ctx context.Context, tx pgx.Tx, id, eventType, aggregate, key string, payload []byte) error {
	_, err := tx.Exec(ctx, `INSERT INTO outbox_events(id,topic,event_type,aggregate_id,message_key,payload,occurred_at) VALUES($1,'ledger-events',$2,$3,$4,$5,now())`, id, eventType, aggregate, key, payload)
	return err
}

func (p *Postgres) PostDeposit(ctx context.Context, v ledger.PostDeposit) (ledger.OperationResult, error) {
	if err := ledger.ValidateDeposit(v); err != nil {
		return ledger.OperationResult{}, err
	}
	tx, err := begin(ctx, p.Writer)
	if err != nil {
		return ledger.OperationResult{}, err
	}
	defer tx.Rollback(ctx)
	uaa, err := ensureUser(ctx, tx, v.UserID, v.AssetID)
	if err != nil {
		return ledger.OperationResult{}, err
	}
	var custodyAsset, status string
	if err = tx.QueryRow(ctx, `SELECT an.asset_id,cp.status FROM custody_positions cp JOIN asset_networks an ON an.id=cp.asset_network_id WHERE cp.id=$1 FOR SHARE OF cp,an`, v.CustodyPositionID).Scan(&custodyAsset, &status); err != nil {
		return ledger.OperationResult{}, err
	}
	if custodyAsset != v.AssetID || status != "active" {
		return ledger.OperationResult{}, errors.New("custody position is inactive or belongs to another asset")
	}
	jid := stableID("jrn_", v.SourceSystem, v.CommandID)
	created, err := createJournal(ctx, tx, jid, "deposit", v.DepositID, "deposit_confirmed", v.SourceSystem, v.CommandID, v.CorrelationID, v.CausationID, "post confirmed deposit", v.OccurredAt, map[string]string{"custody_position_id": v.CustodyPositionID})
	if err != nil {
		return ledger.OperationResult{}, err
	}
	if !created {
		if err = tx.Commit(ctx); err != nil {
			return ledger.OperationResult{}, err
		}
		return ledger.OperationResult{JournalID: jid, Replay: true}, nil
	}
	account := map[string]string{"available": "account_customer_available", "hold": "account_customer_hold"}[v.TargetBucket]
	if err = entry(ctx, tx, jid+":custody", jid, "account_crypto_custody", "", v.AssetID, v.CustodyPositionID, "", "debit", v.AmountAtomic); err != nil {
		return ledger.OperationResult{}, err
	}
	if err = entry(ctx, tx, jid+":user", jid, account, uaa, v.AssetID, "", v.TargetBucket, "credit", v.AmountAtomic); err != nil {
		return ledger.OperationResult{}, err
	}
	column := map[string]string{"available": "available_atomic", "hold": "hold_atomic"}[v.TargetBucket]
	if _, err = tx.Exec(ctx, `UPDATE user_asset_balances SET `+column+`=`+column+`+$1::numeric,version=version+1,updated_at=now() WHERE user_asset_account_id=$2`, v.AmountAtomic, uaa); err != nil {
		return ledger.OperationResult{}, err
	}
	if err = outbox(ctx, tx, stableID("evt_", jid), "LedgerPosted", v.DepositID, uaa, v.CorrelationID, v.CausationID, map[string]any{"journal_id": jid, "deposit_id": v.DepositID, "bucket": v.TargetBucket}); err != nil {
		return ledger.OperationResult{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return ledger.OperationResult{}, err
	}
	return ledger.OperationResult{JournalID: jid}, nil
}

func (p *Postgres) ReleaseHold(ctx context.Context, v ledger.ReleaseHold) (ledger.OperationResult, error) {
	if v.CommandID == "" || v.DepositID == "" || v.UserID == "" || v.AssetID == "" || v.SourceSystem == "" {
		return ledger.OperationResult{}, errors.New("required release field is empty")
	}
	if err := atomicPositive(v.AmountAtomic); err != nil {
		return ledger.OperationResult{}, err
	}
	tx, err := begin(ctx, p.Writer)
	if err != nil {
		return ledger.OperationResult{}, err
	}
	defer tx.Rollback(ctx)
	uaa, err := ensureUser(ctx, tx, v.UserID, v.AssetID)
	if err != nil {
		return ledger.OperationResult{}, err
	}
	jid := stableID("jrn_", v.SourceSystem, v.CommandID)
	created, err := createJournal(ctx, tx, jid, "deposit", v.DepositID, "deposit_hold_released", v.SourceSystem, v.CommandID, v.CorrelationID, v.CausationID, "release held deposit", v.OccurredAt, nil)
	if err != nil {
		return ledger.OperationResult{}, err
	}
	if !created {
		_ = tx.Commit(ctx)
		return ledger.OperationResult{JournalID: jid, Replay: true}, nil
	}
	ct, err := tx.Exec(ctx, `UPDATE user_asset_balances SET hold_atomic=hold_atomic-$1::numeric,available_atomic=available_atomic+$1::numeric,version=version+1,updated_at=now() WHERE user_asset_account_id=$2 AND hold_atomic>=$1::numeric`, v.AmountAtomic, uaa)
	if err != nil {
		return ledger.OperationResult{}, err
	}
	if ct.RowsAffected() != 1 {
		return ledger.OperationResult{}, ledger.ErrInsufficientFunds
	}
	if err = entry(ctx, tx, jid+":hold", jid, "account_customer_hold", uaa, v.AssetID, "", "hold", "debit", v.AmountAtomic); err != nil {
		return ledger.OperationResult{}, err
	}
	if err = entry(ctx, tx, jid+":available", jid, "account_customer_available", uaa, v.AssetID, "", "available", "credit", v.AmountAtomic); err != nil {
		return ledger.OperationResult{}, err
	}
	if err = outbox(ctx, tx, stableID("evt_", jid), "HoldReleased", v.DepositID, uaa, v.CorrelationID, v.CausationID, map[string]any{"journal_id": jid}); err != nil {
		return ledger.OperationResult{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return ledger.OperationResult{}, err
	}
	return ledger.OperationResult{JournalID: jid}, nil
}

func atomicPositive(s string) error {
	n, ok := new(big.Int).SetString(s, 10)
	if !ok || n.Sign() <= 0 {
		return errors.New("amount must be a positive integer")
	}
	return nil
}

func (p *Postgres) ReserveForOrder(ctx context.Context, v ledger.ReserveOrder) (ledger.Reservation, error) {
	if err := ledger.ValidateReserve(v); err != nil {
		return ledger.Reservation{}, err
	}
	if v.OccurredAt.IsZero() {
		v.OccurredAt = time.Now().UTC()
	}
	rid := stableID("rsv_", v.OrderID)
	jid := stableID("jrn_", "order-service", v.CommandID)
	eid := stableID("evt_", jid)
	outboxPayload, err := makeOutboxPayload(eid, "FundsReserved", v.OrderID, v.CorrelationID, v.CausationID, v.TraceParent, v.TraceState, time.Now().UTC(), map[string]any{"journal_id": jid, "reservation_id": rid, "amount_atomic": v.AmountAtomic})
	if err != nil {
		return ledger.Reservation{}, err
	}
	var outcome string
	var r ledger.Reservation
	err = p.Writer.QueryRow(ctx, reserveForOrderSQL, v.UserID, v.AssetID, v.AmountAtomic, v.OrderID, v.CommandID, v.CorrelationID, v.CausationID, v.OccurredAt, rid, jid, eid, outboxPayload).Scan(&outcome, &r.ID, &r.OrderID, &r.Status, &r.OriginalAtomic, &r.RemainingAtomic, &r.BalanceVersion)
	if err != nil {
		return ledger.Reservation{}, err
	}
	switch outcome {
	case "reserved":
		return r, nil
	case "replay":
		r.Replay = true
		return r, nil
	case "not_found":
		return ledger.Reservation{}, ledger.ErrNotFound
	case "insufficient_funds":
		return ledger.Reservation{}, ledger.ErrInsufficientFunds
	case "conflict":
		return ledger.Reservation{}, ledger.ErrConflict
	default:
		return ledger.Reservation{}, fmt.Errorf("reserve for order returned unknown outcome %q", outcome)
	}
}

func reservationTx(ctx context.Context, tx pgx.Tx, order string) (ledger.Reservation, error) {
	var r ledger.Reservation
	err := tx.QueryRow(ctx, `SELECT r.id,r.order_id,r.status,r.original_atomic::text,r.remaining_atomic::text,b.version FROM fund_reservations r JOIN user_asset_balances b ON b.user_asset_account_id=r.user_asset_account_id WHERE r.order_id=$1`, order).Scan(&r.ID, &r.OrderID, &r.Status, &r.OriginalAtomic, &r.RemainingAtomic, &r.BalanceVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		err = ledger.ErrNotFound
	}
	return r, err
}

func (p *Postgres) GetReservation(ctx context.Context, order string) (ledger.Reservation, error) {
	return reservationPool(ctx, p.Reader, order)
}
func reservationPool(ctx context.Context, db *pgxpool.Pool, order string) (ledger.Reservation, error) {
	var r ledger.Reservation
	err := db.QueryRow(ctx, `SELECT r.id,r.order_id,r.status,r.original_atomic::text,r.remaining_atomic::text,b.version FROM fund_reservations r JOIN user_asset_balances b ON b.user_asset_account_id=r.user_asset_account_id WHERE r.order_id=$1`, order).Scan(&r.ID, &r.OrderID, &r.Status, &r.OriginalAtomic, &r.RemainingAtomic, &r.BalanceVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		err = ledger.ErrNotFound
	}
	return r, err
}

func (p *Postgres) GetBalance(ctx context.Context, user, asset string) (ledger.Balance, error) {
	var b ledger.Balance
	err := p.Reader.QueryRow(ctx, `SELECT u.id,u.user_id,u.asset_id,b.available_atomic::text,b.reserved_atomic::text,b.withdrawal_pending_atomic::text,b.deposit_pending_atomic::text,b.hold_atomic::text,b.version FROM user_asset_accounts u JOIN user_asset_balances b ON b.user_asset_account_id=u.id WHERE u.user_id=$1 AND u.asset_id=$2`, user, asset).Scan(&b.UserAssetAccountID, &b.UserID, &b.AssetID, &b.AvailableAtomic, &b.ReservedAtomic, &b.WithdrawalPendingAtomic, &b.DepositPendingAtomic, &b.HoldAtomic, &b.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		err = ledger.ErrNotFound
	}
	return b, err
}

type lockedReservation struct{ id, order, userAccount, userID, asset, remaining string }

func lockReservations(ctx context.Context, tx pgx.Tx, orders ...string) (map[string]lockedReservation, error) {
	sort.Strings(orders)
	rows, err := tx.Query(ctx, `SELECT r.id,r.order_id,r.user_asset_account_id,u.user_id,r.asset_id,r.remaining_atomic::text FROM fund_reservations r JOIN user_asset_accounts u ON u.id=r.user_asset_account_id WHERE r.order_id=ANY($1) ORDER BY r.order_id FOR UPDATE OF r`, orders)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]lockedReservation{}
	for rows.Next() {
		var r lockedReservation
		if err = rows.Scan(&r.id, &r.order, &r.userAccount, &r.userID, &r.asset, &r.remaining); err != nil {
			return nil, err
		}
		out[r.order] = r
	}
	if len(out) != len(orders) {
		return nil, ledger.ErrNotFound
	}
	return out, rows.Err()
}

func lockBalances(ctx context.Context, tx pgx.Tx, ids ...string) error {
	unique := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		unique[id] = struct{}{}
	}
	ids = ids[:0]
	for id := range unique {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	rows, err := tx.Query(ctx, `SELECT user_asset_account_id FROM user_asset_balances WHERE user_asset_account_id=ANY($1) ORDER BY user_asset_account_id FOR UPDATE`, ids)
	if err != nil {
		return err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return err
		}
		count++
	}
	if err = rows.Err(); err != nil {
		return err
	}
	if count != len(ids) {
		return ledger.ErrNotFound
	}
	return nil
}

func checkSequence(ctx context.Context, tx pgx.Tx, engine string, partition int32, sequence int64) error {
	var last int64
	err := tx.QueryRow(ctx, `SELECT last_sequence_number FROM ledger_engine_offsets WHERE engine_id=$1 AND engine_partition=$2 FOR UPDATE`, engine, partition).Scan(&last)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if sequence != last+1 {
		return ledger.ErrSequenceGap
	}
	return nil
}
func setSequence(ctx context.Context, tx pgx.Tx, engine string, partition int32, sequence int64) error {
	_, err := tx.Exec(ctx, `INSERT INTO ledger_engine_offsets(engine_id,engine_partition,last_sequence_number) VALUES($1,$2,$3) ON CONFLICT(engine_id,engine_partition) DO UPDATE SET last_sequence_number=EXCLUDED.last_sequence_number`, engine, partition, sequence)
	return err
}

func (p *Postgres) BookTrade(ctx context.Context, v ledger.TradeExecuted) (ledger.OperationResult, error) {
	if err := ledger.ValidateTrade(v); err != nil {
		return ledger.OperationResult{}, err
	}
	tx, err := begin(ctx, p.Writer)
	if err != nil {
		return ledger.OperationResult{}, err
	}
	defer tx.Rollback(ctx)
	jid := stableID("jrn_", v.EngineID, v.EventID)
	created, err := createJournal(ctx, tx, jid, "trade", v.TradeID, "trade_executed", v.EngineID, v.EventID, v.CorrelationID, v.CausationID, "book matching engine execution", v.OccurredAt, map[string]any{"partition": v.EnginePartition, "sequence": v.SequenceNumber})
	if err != nil {
		return ledger.OperationResult{}, err
	}
	if !created {
		_ = tx.Commit(ctx)
		return ledger.OperationResult{JournalID: jid, Replay: true}, nil
	}
	if err = checkSequence(ctx, tx, v.EngineID, v.EnginePartition, v.SequenceNumber); err != nil {
		return ledger.OperationResult{}, err
	}
	rsv, err := lockReservations(ctx, tx, v.BuyerOrderID, v.SellerOrderID)
	if err != nil {
		return ledger.OperationResult{}, err
	}
	buyer, seller := rsv[v.BuyerOrderID], rsv[v.SellerOrderID]
	if buyer.asset != v.QuoteAssetID || seller.asset != v.BaseAssetID {
		return ledger.OperationResult{}, errors.New("reservation asset mismatch")
	}
	if buyer.userID != v.BuyerUserID || seller.userID != v.SellerUserID {
		return ledger.OperationResult{}, errors.New("trade user does not own referenced reservation")
	}
	quoteConsume := add(v.QuoteAmountAtomic, v.BuyerFeeQuoteAtomic)
	baseReceive := sub(v.BaseAmountAtomic, v.SellerFeeBaseAtomic)
	if baseReceive.Sign() < 0 {
		return ledger.OperationResult{}, errors.New("seller fee exceeds base amount")
	}
	if less(buyer.remaining, quoteConsume.String()) || less(seller.remaining, v.BaseAmountAtomic) {
		return ledger.OperationResult{}, ledger.ErrInsufficientFunds
	}
	buyerBase, err := ensureUser(ctx, tx, v.BuyerUserID, v.BaseAssetID)
	if err != nil {
		return ledger.OperationResult{}, err
	}
	sellerQuote, err := ensureUser(ctx, tx, v.SellerUserID, v.QuoteAssetID)
	if err != nil {
		return ledger.OperationResult{}, err
	}
	if err = lockBalances(ctx, tx, buyer.userAccount, seller.userAccount, buyerBase, sellerQuote); err != nil {
		return ledger.OperationResult{}, err
	}
	for _, q := range []struct{ account, asset, delta string }{{buyer.userAccount, v.QuoteAssetID, quoteConsume.String()}, {seller.userAccount, v.BaseAssetID, v.BaseAmountAtomic}} {
		ct, e := tx.Exec(ctx, `UPDATE user_asset_balances SET reserved_atomic=reserved_atomic-$1::numeric,version=version+1,updated_at=now() WHERE user_asset_account_id=$2 AND reserved_atomic>=$1::numeric`, q.delta, q.account)
		if e != nil {
			return ledger.OperationResult{}, e
		}
		if ct.RowsAffected() != 1 {
			return ledger.OperationResult{}, ledger.ErrInsufficientFunds
		}
	}
	if _, err = tx.Exec(ctx, `UPDATE user_asset_balances SET available_atomic=available_atomic+$1::numeric,version=version+1,updated_at=now() WHERE user_asset_account_id=$2`, v.QuoteAmountAtomic, sellerQuote); err != nil {
		return ledger.OperationResult{}, err
	}
	if _, err = tx.Exec(ctx, `UPDATE user_asset_balances SET available_atomic=available_atomic+$1::numeric,version=version+1,updated_at=now() WHERE user_asset_account_id=$2`, baseReceive.String(), buyerBase); err != nil {
		return ledger.OperationResult{}, err
	}
	for _, r := range []struct {
		lr     lockedReservation
		amount string
	}{{buyer, quoteConsume.String()}, {seller, v.BaseAmountAtomic}} {
		if _, err = tx.Exec(ctx, `UPDATE fund_reservations SET remaining_atomic=remaining_atomic-$1::numeric,status=CASE WHEN remaining_atomic-$1::numeric=0 THEN 'consumed' ELSE 'partially_consumed' END,version=version+1,updated_at=now() WHERE id=$2`, r.amount, r.lr.id); err != nil {
			return ledger.OperationResult{}, err
		}
	}
	lines := []struct{ id, acct, uaa, asset, side, amount string }{{"buyer_quote", "account_customer_reserved", buyer.userAccount, v.QuoteAssetID, "debit", quoteConsume.String()}, {"seller_quote", "account_customer_available", sellerQuote, v.QuoteAssetID, "credit", v.QuoteAmountAtomic}, {"seller_base", "account_customer_reserved", seller.userAccount, v.BaseAssetID, "debit", v.BaseAmountAtomic}, {"buyer_base", "account_customer_available", buyerBase, v.BaseAssetID, "credit", baseReceive.String()}}
	if v.BuyerFeeQuoteAtomic != "0" {
		lines = append(lines, struct{ id, acct, uaa, asset, side, amount string }{"buyer_fee", "account_trading_fee_revenue", "", v.QuoteAssetID, "credit", v.BuyerFeeQuoteAtomic})
	}
	if v.SellerFeeBaseAtomic != "0" {
		lines = append(lines, struct{ id, acct, uaa, asset, side, amount string }{"seller_fee", "account_trading_fee_revenue", "", v.BaseAssetID, "credit", v.SellerFeeBaseAtomic})
	}
	for _, l := range lines {
		bucket := ""
		if l.acct == "account_customer_reserved" {
			bucket = "reserved"
		} else if l.acct == "account_customer_available" {
			bucket = "available"
		}
		if err = entry(ctx, tx, jid+":"+l.id, jid, l.acct, l.uaa, l.asset, "", bucket, l.side, l.amount); err != nil {
			return ledger.OperationResult{}, err
		}
	}
	if err = setSequence(ctx, tx, v.EngineID, v.EnginePartition, v.SequenceNumber); err != nil {
		return ledger.OperationResult{}, err
	}
	if err = outbox(ctx, tx, stableID("evt_", jid), "TradeBooked", v.TradeID, v.EngineID+fmt.Sprint(":", v.EnginePartition), v.CorrelationID, v.CausationID, map[string]any{"journal_id": jid, "trade_id": v.TradeID, "sequence_number": v.SequenceNumber}); err != nil {
		return ledger.OperationResult{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return ledger.OperationResult{}, err
	}
	return ledger.OperationResult{JournalID: jid}, nil
}

func add(a, b string) *big.Int {
	x, _ := new(big.Int).SetString(a, 10)
	y, _ := new(big.Int).SetString(b, 10)
	return x.Add(x, y)
}
func sub(a, b string) *big.Int {
	x, _ := new(big.Int).SetString(a, 10)
	y, _ := new(big.Int).SetString(b, 10)
	return x.Sub(x, y)
}
func less(a, b string) bool {
	x, _ := new(big.Int).SetString(a, 10)
	y, _ := new(big.Int).SetString(b, 10)
	return x.Cmp(y) < 0
}

func (p *Postgres) CancelOrder(ctx context.Context, v ledger.CancelOrder) (ledger.OperationResult, error) {
	if v.EventID == "" || v.OrderID == "" || v.EngineID == "" || v.SequenceNumber <= 0 {
		return ledger.OperationResult{}, errors.New("event, order, engine, and sequence are required")
	}
	tx, err := begin(ctx, p.Writer)
	if err != nil {
		return ledger.OperationResult{}, err
	}
	defer tx.Rollback(ctx)
	jid := stableID("jrn_", v.EngineID, v.EventID)
	created, err := createJournal(ctx, tx, jid, "order", v.OrderID, "order_reservation_released", v.EngineID, v.EventID, v.CorrelationID, v.CausationID, "release cancelled order reservation", v.OccurredAt, nil)
	if err != nil {
		return ledger.OperationResult{}, err
	}
	if !created {
		_ = tx.Commit(ctx)
		return ledger.OperationResult{JournalID: jid, Replay: true}, nil
	}
	if err = checkSequence(ctx, tx, v.EngineID, v.EnginePartition, v.SequenceNumber); err != nil {
		return ledger.OperationResult{}, err
	}
	rsv, err := lockReservations(ctx, tx, v.OrderID)
	if err != nil {
		return ledger.OperationResult{}, err
	}
	r := rsv[v.OrderID]
	if r.remaining == "0" {
		return ledger.OperationResult{}, errors.New("reservation has no remaining funds")
	}
	ct, err := tx.Exec(ctx, `UPDATE user_asset_balances SET reserved_atomic=reserved_atomic-$1::numeric,available_atomic=available_atomic+$1::numeric,version=version+1,updated_at=now() WHERE user_asset_account_id=$2 AND reserved_atomic>=$1::numeric`, r.remaining, r.userAccount)
	if err != nil {
		return ledger.OperationResult{}, err
	}
	if ct.RowsAffected() != 1 {
		return ledger.OperationResult{}, ledger.ErrInsufficientFunds
	}
	if _, err = tx.Exec(ctx, `UPDATE fund_reservations SET remaining_atomic=0,status='released',version=version+1,updated_at=now() WHERE id=$1`, r.id); err != nil {
		return ledger.OperationResult{}, err
	}
	if err = entry(ctx, tx, jid+":reserved", jid, "account_customer_reserved", r.userAccount, r.asset, "", "reserved", "debit", r.remaining); err != nil {
		return ledger.OperationResult{}, err
	}
	if err = entry(ctx, tx, jid+":available", jid, "account_customer_available", r.userAccount, r.asset, "", "available", "credit", r.remaining); err != nil {
		return ledger.OperationResult{}, err
	}
	if err = setSequence(ctx, tx, v.EngineID, v.EnginePartition, v.SequenceNumber); err != nil {
		return ledger.OperationResult{}, err
	}
	if err = outbox(ctx, tx, stableID("evt_", jid), "FundsReleased", v.OrderID, r.userAccount, v.CorrelationID, v.CausationID, map[string]any{"journal_id": jid, "amount_atomic": r.remaining}); err != nil {
		return ledger.OperationResult{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return ledger.OperationResult{}, err
	}
	return ledger.OperationResult{JournalID: jid}, nil
}
