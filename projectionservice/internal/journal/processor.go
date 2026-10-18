// Package journal defines atomic, nonauthoritative Ledger journal projection.
package journal

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"regexp"
	"sort"

	lifecycle "praxis/api/events/lifecycle/v1"
	wire "praxis/api/gen/events/lifecycle/v1"
)

var (
	ErrInvalid  = errors.New("invalid journal projection record")
	ErrGap      = errors.New("journal projection version gap")
	ErrStale    = errors.New("unrecognized stale journal fact")
	ErrConflict = errors.New("conflicting journal fact")
)

type Mode string

const (
	Shadow  Mode = "shadow"
	Rebuild Mode = "rebuild"
)

type Target struct {
	Generation string
	Mode       Mode
}

var generationName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$`)

func (t Target) Validate() error {
	if !generationName.MatchString(t.Generation) || (t.Mode != Shadow && t.Mode != Rebuild) {
		return errors.New("journal projection requires an isolated generation and shadow/rebuild mode")
	}
	return nil
}

type Outcome string

const (
	Applied   Outcome = "applied"
	Duplicate Outcome = "duplicate"
)

// Version zero represents only an explicitly empty account. Positive versions
// must retain stable owner and asset identities. Bootstrap is a separate process.
type State struct {
	Version         uint64
	UserID, AssetID string
}
type Receipt struct {
	Digest    [32]byte
	JournalID string
}

// All operations are scoped to one Target. LockAccount serializes even absent
// accounts (or forces a transaction retry on conflicting conditional writes).
// The processor locks account IDs in lexical order before any writes.
// PutJournal stores the complete immutable event with a unique journal ID;
// PutAccount stores every bucket and the exact supplied version and owner.
// Inbox event IDs are unique across journals within a target.
type Transaction interface {
	Inbox(context.Context, string) (Receipt, bool, error)
	Journal(context.Context, string) (bool, error)
	LockAccount(context.Context, string) (State, error)
	PutAccount(context.Context, *wire.AccountBalanceSnapshot) error
	PutJournal(context.Context, *wire.LifecycleEvent) error
	PutInbox(context.Context, string, Receipt) error
}

// Within atomically commits every account, journal and inbox write or rolls
// them all back. Nil means durable commit, including duplicate reads. It may
// retry the callback serially on transaction conflicts, must honor cancellation,
// and must return errors on ambiguous commits. Redelivery retains event bytes.
// Modes and generations must never share state or inbox identities.
type Store interface {
	Within(context.Context, Target, func(Transaction) error) error
}
type Processor struct {
	target Target
	store  Store
}

func New(target Target, store Store) (*Processor, error) {
	if err := target.Validate(); err != nil {
		return nil, err
	}
	if store == nil {
		return nil, errors.New("journal projection requires transactional storage")
	}
	return &Processor{target: target, store: store}, nil
}

// Process permits source acknowledgement only after durable atomic commit.
// Journals can arrive out of account order: a gap must be held or repaired,
// never skipped or resolved by arrival timestamp. This processor does not
// fetch authoritative state, commit Kafka offsets or perform gap repair.
func (p *Processor) Process(ctx context.Context, topic string, key, raw []byte) (Outcome, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if topic != "ledger.journals.v1" {
		return "", fmt.Errorf("%w: unexpected topic", ErrInvalid)
	}
	event, err := lifecycle.Decode(raw)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	journal := event.GetLedgerJournal()
	if journal == nil || string(key) != journal.JournalId {
		return "", fmt.Errorf("%w: expected journal payload and matching key", ErrInvalid)
	}
	receipt := Receipt{Digest: sha256.Sum256(raw), JournalID: journal.JournalId}
	balances := append([]*wire.AccountBalanceSnapshot(nil), journal.Balances...)
	sort.Slice(balances, func(i, j int) bool { return balances[i].UserAssetAccountId < balances[j].UserAssetAccountId })
	var outcome Outcome
	err = p.store.Within(ctx, p.target, func(tx Transaction) error {
		outcome = ""
		prior, exists, err := tx.Inbox(ctx, event.EventId)
		if err != nil {
			return err
		}
		if exists {
			if prior != receipt {
				return fmt.Errorf("%w: event ID reused", ErrConflict)
			}
			outcome = Duplicate
			return nil
		}
		exists, err = tx.Journal(ctx, journal.JournalId)
		if err != nil {
			return err
		}
		if exists {
			return fmt.Errorf("%w: journal ID reused", ErrConflict)
		}
		// Check every account before writing any of them. A mixed stale/new journal
		// is never partially applied; the store still must guarantee full rollback.
		for _, balance := range balances {
			state, err := tx.LockAccount(ctx, balance.UserAssetAccountId)
			if err != nil {
				return err
			}
			if state.Version > 0 && (state.UserID != balance.UserId || state.AssetID != balance.AssetId) {
				return fmt.Errorf("%w: account identity changed", ErrConflict)
			}
			if balance.Version <= state.Version {
				return ErrStale
			}
			if balance.PreviousVersion > state.Version {
				return ErrGap
			}
			if balance.PreviousVersion < state.Version {
				return ErrConflict
			}
		}
		for _, balance := range balances {
			if err := tx.PutAccount(ctx, balance); err != nil {
				return err
			}
		}
		if err := tx.PutJournal(ctx, event); err != nil {
			return err
		}
		if err := tx.PutInbox(ctx, event.EventId, receipt); err != nil {
			return err
		}
		outcome = Applied
		return nil
	})
	if err != nil {
		return "", err
	}
	if outcome == "" {
		return "", errors.New("journal store returned without executing its transaction")
	}
	return outcome, nil
}
