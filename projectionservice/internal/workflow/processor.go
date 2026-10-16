// Package workflow supplies the Order workflow projection transaction protocol.
// It has no authoritative service clients, broker offset commits or external
// delivery hooks. Storage and broker adapters remain separate acceptance work.
package workflow

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"regexp"

	lifecycle "praxis/api/events/lifecycle/v1"
	wire "praxis/api/gen/events/lifecycle/v1"
)

var (
	ErrInvalid  = errors.New("invalid workflow projection record")
	ErrGap      = errors.New("workflow projection version gap")
	ErrStale    = errors.New("unrecognized stale workflow fact")
	ErrConflict = errors.New("conflicting workflow fact")
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

type Outcome string

const (
	Applied   Outcome = "applied"
	Duplicate Outcome = "duplicate"
)

// State is read under the same transaction serialization as the inbox check.
// Version zero is an empty projection, never an implicit historical bootstrap.
type State struct {
	Version uint64
	UserID  string
}
type Receipt struct {
	Digest  [32]byte
	OrderID string
	Version uint64
}

// Transaction operations are scoped to the complete Target passed to Within.
// LockWorkflow must serialize concurrent changes to the order even if no
// document exists yet (or force a transaction retry on conditional-write
// conflict). Inbox IDs must also be unique across orders within that target.
// PutWorkflow stores the full immutable event, without inferring execution state.
type Transaction interface {
	LockWorkflow(context.Context, string) (State, error)
	Inbox(context.Context, string) (Receipt, bool, error)
	PutWorkflow(context.Context, *wire.LifecycleEvent) error
	PutInbox(context.Context, string, Receipt) error
}

// Store must atomically commit all callback writes or roll them all back.
// A nil result means durable commit, including for a duplicate read. The adapter
// must isolate generations and modes and may retry the callback on transaction
// conflicts. Within must honor cancellation and never invoke callbacks in
// parallel for one call. An ambiguous commit returns an error; redelivery uses
// the same event ID. This interface is a contract, not a storage implementation.
type Store interface {
	Within(context.Context, Target, func(Transaction) error) error
}

type Processor struct {
	target Target
	store  Store
}

var generationName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$`)

func (t Target) Validate() error {
	if !generationName.MatchString(t.Generation) || (t.Mode != Shadow && t.Mode != Rebuild) {
		return errors.New("workflow projection requires an isolated generation and shadow/rebuild mode")
	}
	return nil
}

func New(target Target, store Store) (*Processor, error) {
	if target.Validate() != nil || store == nil {
		return nil, errors.New("workflow projection requires an isolated generation, shadow/rebuild mode and transactional store")
	}
	return &Processor{target: target, store: store}, nil
}

// Process accepts only order.events.v1 lifecycle facts. A successful return is
// eligible for source acknowledgement only after the broker adapter also
// preserves contiguous partition commits. Every error leaves the source offset
// unacknowledged. In particular a gap must not be DLQed and silently skipped.
func (p *Processor) Process(ctx context.Context, topic string, key, raw []byte) (Outcome, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if topic != "order.events.v1" {
		return "", fmt.Errorf("%w: unexpected topic", ErrInvalid)
	}
	event, err := lifecycle.Decode(raw)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	snapshot := event.GetOrderWorkflow()
	if snapshot == nil || string(key) != snapshot.OrderId {
		return "", fmt.Errorf("%w: expected workflow payload and matching order key", ErrInvalid)
	}
	receipt := Receipt{Digest: sha256.Sum256(raw), OrderID: snapshot.OrderId, Version: snapshot.WorkflowVersion}
	var result Outcome
	err = p.store.Within(ctx, p.target, func(tx Transaction) error {
		result = "" // A storage adapter may retry this callback.
		state, err := tx.LockWorkflow(ctx, snapshot.OrderId)
		if err != nil {
			return err
		}
		prior, exists, err := tx.Inbox(ctx, event.EventId)
		if err != nil {
			return err
		}
		if exists {
			if prior != receipt {
				return fmt.Errorf("%w: event ID reused with different bytes or coordinates", ErrConflict)
			}
			result = Duplicate
			return nil
		}
		if state.Version > 0 && state.UserID != snapshot.UserId {
			return fmt.Errorf("%w: workflow owner changed", ErrConflict)
		}
		if snapshot.WorkflowVersion <= state.Version {
			return ErrStale
		}
		// Subtraction avoids overflow at the largest stored version.
		if snapshot.WorkflowVersion-state.Version != 1 {
			return ErrGap
		}
		if err := tx.PutWorkflow(ctx, event); err != nil {
			return err
		}
		if err := tx.PutInbox(ctx, event.EventId, receipt); err != nil {
			return err
		}
		result = Applied
		return nil
	})
	if err != nil {
		return "", err
	}
	if result == "" {
		return "", errors.New("workflow projection store returned without executing its transaction")
	}
	return result, nil
}
