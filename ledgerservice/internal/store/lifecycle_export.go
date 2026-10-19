package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	lifecycle "praxis/api/events/lifecycle/v1"
	wire "praxis/api/gen/events/lifecycle/v1"
)

// ExportJournals verifies a read-only repeatable-read authoritative snapshot and
// emits journals in account-dependency order, never relying on timestamps.
// This keeps a hot account's complete history replayable with a bounded queue.
func ExportJournals(ctx context.Context, db *pgxpool.Pool, output io.Writer) error {
	tx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	rows, err := tx.Query(ctx, `SELECT event_id,journal_id,payload FROM ledger_journal_outbox ORDER BY journal_id`)
	if err != nil {
		return err
	}
	type chain struct {
		next   map[uint64]uint64
		facts  map[uint64]int
		latest *wire.AccountBalanceSnapshot
	}
	type coordinate struct {
		id       string
		previous uint64
	}
	type fact struct {
		id       string
		raw      []byte
		accounts []coordinate
	}
	accounts := map[string]*chain{}
	facts := []fact{}
	bytes := 0
	for rows.Next() {
		if len(facts) >= 100000 {
			rows.Close()
			return errors.New("journal export exceeds record bound")
		}
		var eventID, journalID string
		var raw []byte
		if err := rows.Scan(&eventID, &journalID, &raw); err != nil {
			rows.Close()
			return err
		}
		bytes += len(raw)
		if bytes > 96*1024*1024 {
			rows.Close()
			return errors.New("journal export exceeds memory bound")
		}
		e, err := lifecycle.Decode(raw)
		if err != nil {
			rows.Close()
			return err
		}
		j := e.GetLedgerJournal()
		if j == nil || j.JournalId != journalID || e.EventId != eventID {
			rows.Close()
			return errors.New("journal source coordinates conflict")
		}
		f := fact{id: journalID, raw: raw}
		for _, b := range j.Balances {
			c := accounts[b.UserAssetAccountId]
			if c == nil {
				c = &chain{next: map[uint64]uint64{}, facts: map[uint64]int{}}
				accounts[b.UserAssetAccountId] = c
			}
			if _, exists := c.next[b.PreviousVersion]; exists {
				rows.Close()
				return errors.New("ambiguous account version chain")
			}
			if _, exists := c.facts[b.Version]; exists {
				rows.Close()
				return errors.New("conflicting account version")
			}
			c.next[b.PreviousVersion] = b.Version
			c.facts[b.Version] = len(facts)
			if c.latest == nil || b.Version > c.latest.Version {
				c.latest = b
			}
			f.accounts = append(f.accounts, coordinate{id: b.UserAssetAccountId, previous: b.PreviousVersion})
		}
		facts = append(facts, f)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if len(facts) == 0 {
		return errors.New("no captured journals")
	}
	for id, c := range accounts {
		version := uint64(0)
		for range c.next {
			next, ok := c.next[version]
			if !ok {
				return errors.New("account history requires reviewed bootstrap or capture-gap repair")
			}
			version = next
		}
		if version != c.latest.Version {
			return errors.New("account version chain is incomplete")
		}
		b := c.latest
		var user, asset string
		var current uint64
		var available, reserved, withdrawal, deposit, hold string
		err := tx.QueryRow(ctx, `SELECT a.user_id,a.asset_id,b.version,b.available_atomic::text,b.reserved_atomic::text,b.withdrawal_pending_atomic::text,b.deposit_pending_atomic::text,b.hold_atomic::text FROM user_asset_accounts a JOIN user_asset_balances b ON b.user_asset_account_id=a.id WHERE a.id=$1`, id).Scan(&user, &asset, &current, &available, &reserved, &withdrawal, &deposit, &hold)
		if err != nil {
			return err
		}
		if user != b.UserId || asset != b.AssetId || current != b.Version || available != b.AvailableAtomic || reserved != b.ReservedAtomic || withdrawal != b.WithdrawalPendingAtomic || deposit != b.DepositPendingAtomic || hold != b.HoldAtomic {
			return fmt.Errorf("authoritative account differs from latest captured snapshot")
		}
	}
	// Kahn's algorithm over journal dependencies. Deduplicate parents for a
	// journal touching two accounts last changed by the same predecessor.
	edges := make([][]int, len(facts))
	incoming := make([]int, len(facts))
	for index, f := range facts {
		parents := map[int]bool{}
		for _, b := range f.accounts {
			if b.previous > 0 {
				parent, ok := accounts[b.id].facts[b.previous]
				if !ok {
					return errors.New("missing journal predecessor")
				}
				parents[parent] = true
			}
		}
		incoming[index] = len(parents)
		for parent := range parents {
			edges[parent] = append(edges[parent], index)
		}
	}
	ready := []int{}
	for index, n := range incoming {
		if n == 0 {
			ready = append(ready, index)
		}
	}
	ordered := make([]int, 0, len(facts))
	for head := 0; head < len(ready); head++ {
		index := ready[head]
		ordered = append(ordered, index)
		for _, next := range edges[index] {
			incoming[next]--
			if incoming[next] == 0 {
				ready = append(ready, next)
			}
		}
	}
	if len(ordered) != len(facts) {
		return errors.New("cyclic journal dependency graph")
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	encoder := json.NewEncoder(output)
	for offset, index := range ordered {
		if err := ctx.Err(); err != nil {
			return err
		}
		f := facts[index]
		if err := encoder.Encode(struct {
			Topic     string `json:"topic"`
			Partition int    `json:"partition"`
			Offset    int64  `json:"offset"`
			Key       []byte `json:"key"`
			Payload   []byte `json:"payload"`
		}{"ledger.journals.v1", 0, int64(offset), []byte(f.id), f.raw}); err != nil {
			return err
		}
	}
	return nil
}
