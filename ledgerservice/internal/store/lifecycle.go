package store

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/timestamppb"
	lifecycle "praxis/api/events/lifecycle/v1"
	wire "praxis/api/gen/events/lifecycle/v1"
)

// commitLedger inserts a validated immutable journal fact in the same transaction
// as the financial mutation. Disabled mode keeps the original commit fast path.
func (p *Postgres) commitLedger(ctx context.Context, tx pgx.Tx) error {
	if p.LifecycleCapture {
		if err := captureJournal(ctx, tx); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func captureJournal(ctx context.Context, tx pgx.Tx) error {
	var enabled, journalID, beforeJSON string
	if err := tx.QueryRow(ctx, `SELECT COALESCE(current_setting('praxis.ledger_lifecycle_capture',true),''),
 COALESCE(current_setting('praxis.ledger_lifecycle_journal',true),''),
 COALESCE(NULLIF(current_setting('praxis.ledger_lifecycle_before',true),''),'{}')`).Scan(&enabled, &journalID, &beforeJSON); err != nil {
		return err
	}
	if enabled != "on" {
		return errors.New("Ledger lifecycle writer configuration mismatch")
	}
	if journalID == "" {
		return nil
	} // Idempotent replay inserted no new journal.
	before := map[string]string{}
	if err := json.Unmarshal([]byte(beforeJSON), &before); err != nil {
		return err
	}
	j := &wire.LedgerJournalPosted{}
	e := &wire.LifecycleEvent{EventId: "evt_lj_" + journalID, EventType: lifecycle.LedgerJournalType, SchemaVersion: lifecycle.SchemaVersion, Producer: "ledger-service", Payload: &wire.LifecycleEvent_LedgerJournal{LedgerJournal: j}}
	var occurred time.Time
	if err := tx.QueryRow(ctx, `SELECT id,journal_type,reference_id,source_system,source_event_id,
 COALESCE(NULLIF(correlation_id,''),source_event_id),COALESCE(NULLIF(causation_id,''),source_event_id),occurred_at
 FROM ledger_journals WHERE id=$1`, journalID).Scan(&j.JournalId, &j.BusinessType, &j.ReferenceId, &j.SourceSystem, &j.CommandId, &e.CorrelationId, &e.CausationId, &occurred); err != nil {
		return err
	}
	e.OccurredAt = timestamppb.New(occurred)
	rows, err := tx.Query(ctx, `SELECT id,ledger_account_id,asset_id,side,amount_atomic::text,
 COALESCE(user_asset_account_id,''),COALESCE(bucket,''),COALESCE(custody_position_id,'')
 FROM ledger_entries WHERE journal_id=$1 ORDER BY id LIMIT 1025`, journalID)
	if err != nil {
		return err
	}
	for rows.Next() {
		entry := &wire.JournalEntry{}
		var side string
		if err := rows.Scan(&entry.EntryId, &entry.AccountId, &entry.AssetId, &side, &entry.AmountAtomic, &entry.UserAssetAccountId, &entry.Bucket, &entry.CustodyPositionId); err != nil {
			rows.Close()
			return err
		}
		if side == "debit" {
			entry.Side = wire.PostingSide_POSTING_SIDE_DEBIT
		} else if side == "credit" {
			entry.Side = wire.PostingSide_POSTING_SIDE_CREDIT
		}
		j.Entries = append(j.Entries, entry)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	rows, err = tx.Query(ctx, `SELECT a.id,a.user_id,a.asset_id,b.version,
 b.available_atomic::text,b.reserved_atomic::text,b.withdrawal_pending_atomic::text,b.deposit_pending_atomic::text,b.hold_atomic::text
 FROM user_asset_accounts a JOIN user_asset_balances b ON b.user_asset_account_id=a.id
 WHERE a.id IN (SELECT user_asset_account_id FROM ledger_entries WHERE journal_id=$1)
 ORDER BY a.id LIMIT 1025`, journalID)
	if err != nil {
		return err
	}
	for rows.Next() {
		balance := &wire.AccountBalanceSnapshot{}
		if err := rows.Scan(&balance.UserAssetAccountId, &balance.UserId, &balance.AssetId, &balance.Version, &balance.AvailableAtomic, &balance.ReservedAtomic, &balance.WithdrawalPendingAtomic, &balance.DepositPendingAtomic, &balance.HoldAtomic); err != nil {
			rows.Close()
			return err
		}
		previous, ok := before[balance.UserAssetAccountId]
		if !ok {
			rows.Close()
			return errors.New("journal customer balance was not updated in this transaction")
		}
		balance.PreviousVersion, err = strconv.ParseUint(previous, 10, 64)
		if err != nil {
			rows.Close()
			return err
		}
		delete(before, balance.UserAssetAccountId)
		j.Balances = append(j.Balances, balance)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if len(before) != 0 {
		return errors.New("transaction changed customer balances absent from journal")
	}
	payload, err := lifecycle.Encode(e)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO ledger_journal_outbox(event_id,journal_id,payload,occurred_at) VALUES($1,$2,$3,$4)`, e.EventId, journalID, payload, occurred)
	return err
}
