package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	lifecycle "praxis/api/events/lifecycle/v1"
)

// JournalPublication retains source identity and exact immutable wire bytes.
type JournalPublication struct {
	EventID, JournalID string
	Payload            []byte
}

type JournalPublisher interface {
	PublishJournal(context.Context, JournalPublication) error
}

// JournalDispatcher sends one fact per bounded transaction, retaining its row
// lock through synchronous acknowledgement. Journals have no account ordering
// promise; consumers must enforce the balance version chains in their payloads.
// Lost acknowledgements or commits can cause duplicate delivery.
type JournalDispatcher struct {
	DB        *pgxpool.Pool
	Publisher JournalPublisher
	Timeout   time.Duration
}

func (d JournalDispatcher) DispatchOnce(ctx context.Context) (bool, error) {
	if d.DB == nil || d.Publisher == nil || d.Timeout <= 0 || d.Timeout > 30*time.Second {
		return false, errors.New("journal dispatch requires database, publisher and timeout in (0, 30s]")
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
	var fact JournalPublication
	err = tx.QueryRow(ctx, `SELECT o.event_id,o.journal_id,o.payload
 FROM ledger_journal_delivery d JOIN ledger_journal_outbox o USING(event_id)
 WHERE d.published_at IS NULL AND d.next_attempt_at<=now()
 ORDER BY d.next_attempt_at,d.event_id FOR UPDATE OF d SKIP LOCKED LIMIT 1`).Scan(&fact.EventID, &fact.JournalID, &fact.Payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	event, err := lifecycle.Decode(fact.Payload)
	if err == nil {
		if journal := event.GetLedgerJournal(); journal == nil || event.EventId != fact.EventID || journal.JournalId != fact.JournalID {
			err = errors.New("journal source coordinates do not match payload")
		}
	}
	if err == nil {
		err = d.Publisher.PublishJournal(ctx, fact)
	}
	if err != nil {
		_, updateErr := tx.Exec(ctx, `UPDATE ledger_journal_delivery SET attempt_count=attempt_count+1,
 next_attempt_at=now()+interval '1 second'*least(60,power(2,least(attempt_count,6))) WHERE event_id=$1`, fact.EventID)
		if updateErr == nil {
			updateErr = tx.Commit(ctx)
		}
		return true, errors.Join(fmt.Errorf("dispatch journal: %w", err), updateErr)
	}
	_, err = tx.Exec(ctx, `UPDATE ledger_journal_delivery SET published_at=now(),attempt_count=attempt_count+1 WHERE event_id=$1`, fact.EventID)
	if err != nil {
		return true, err
	}
	return true, tx.Commit(ctx)
}
