package mongostore

import (
	"context"
	"errors"
	"math"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"google.golang.org/protobuf/proto"

	lifecycle "praxis/api/events/lifecycle/v1"
	wire "praxis/api/gen/events/lifecycle/v1"
	"praxis/projectionservice/internal/journal"
	"praxis/projectionservice/internal/workflow"
)

// JournalStore uses three target-scoped collections, all committed in the same
// majority/snapshot transaction. The caller owns its MongoDB client.
type JournalStore struct {
	core                      *Store
	target                    journal.Target
	accounts, journals, inbox *mongo.Collection
}

var _ journal.Store = (*JournalStore)(nil)

func NewJournal(client *mongo.Client, database string, target journal.Target, timeout time.Duration) (*JournalStore, error) {
	if err := target.Validate(); err != nil {
		return nil, err
	}
	core, err := New(client, database, workflow.Target{Generation: target.Generation, Mode: workflow.Mode(target.Mode)}, timeout)
	if err != nil {
		return nil, err
	}
	suffix := string(target.Mode) + "_" + target.Generation
	return &JournalStore{core: core, target: target, accounts: core.db.Collection("journal_accounts_" + suffix), journals: core.db.Collection("journals_" + suffix), inbox: core.db.Collection("journal_inbox_" + suffix)}, nil
}
func (s *JournalStore) Verify(ctx context.Context) error {
	return s.core.verifyCollections(ctx, s.accounts, s.journals, s.inbox)
}
func (s *JournalStore) Prepare(ctx context.Context) error {
	s.core.verified.Store(false)
	ctx, cancel := context.WithTimeout(ctx, s.core.timeout)
	defer cancel()
	for _, collection := range []*mongo.Collection{s.accounts, s.journals, s.inbox} {
		err := s.core.db.CreateCollection(ctx, collection.Name())
		var command mongo.CommandError
		if err != nil && !(errors.As(err, &command) && command.Code == 48) {
			return err
		}
	}
	if _, err := s.journals.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "user_ids", Value: 1}, {Key: "occurred_at", Value: 1}, {Key: "_id", Value: 1}}}); err != nil {
		return err
	}
	return s.Verify(ctx)
}
func (s *JournalStore) Within(ctx context.Context, target journal.Target, callback func(journal.Transaction) error) error {
	if target != s.target || callback == nil {
		return errors.New("Mongo journal target mismatch or missing callback")
	}
	if !s.core.verified.Load() {
		return ErrUnverified
	}
	return s.core.withTransaction(ctx, func(ctx context.Context) error {
		return callback(&journalTransaction{ctx: ctx, store: s, states: map[string]journal.State{}})
	})
}

type journalDocument struct {
	UserIDs    []string  `bson:"user_ids"`
	ID         string    `bson:"_id"`
	EventID    string    `bson:"event_id"`
	OccurredAt time.Time `bson:"occurred_at"`
	Payload    []byte    `bson:"payload"`
}
type accountDocument struct {
	ID      string `bson:"_id"`
	UserID  string `bson:"user_id"`
	AssetID string `bson:"asset_id"`
	Version int64  `bson:"version"`
	Payload []byte `bson:"payload"`
}
type journalInboxDocument struct {
	ID        string `bson:"_id"`
	JournalID string `bson:"journal_id"`
	Digest    []byte `bson:"digest"`
}

func encodeAccount(b *wire.AccountBalanceSnapshot) (accountDocument, error) {
	if err := lifecycle.ValidateBalance(b); err != nil {
		return accountDocument{}, err
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(b)
	return accountDocument{ID: b.UserAssetAccountId, UserID: b.UserId, AssetID: b.AssetId, Version: int64(b.Version), Payload: payload}, err
}
func decodeAccount(doc accountDocument) (journal.State, error) {
	var b wire.AccountBalanceSnapshot
	if len(doc.Payload) > lifecycle.MaxEventBytes {
		return journal.State{}, errors.New("stored account exceeds size limit")
	}
	if err := proto.Unmarshal(doc.Payload, &b); err != nil {
		return journal.State{}, err
	}
	if err := lifecycle.ValidateBalance(&b); err != nil {
		return journal.State{}, err
	}
	if doc.ID != b.UserAssetAccountId || doc.UserID != b.UserId || doc.AssetID != b.AssetId || doc.Version <= 0 || uint64(doc.Version) != b.Version {
		return journal.State{}, errors.New("stored account coordinates conflict")
	}
	return journal.State{Version: b.Version, UserID: b.UserId, AssetID: b.AssetId}, nil
}

type journalTransaction struct {
	ctx    context.Context
	store  *JournalStore
	states map[string]journal.State
}

func (t *journalTransaction) Inbox(_ context.Context, id string) (journal.Receipt, bool, error) {
	var doc journalInboxDocument
	err := t.store.inbox.FindOne(t.ctx, bson.D{{Key: "_id", Value: id}}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return journal.Receipt{}, false, nil
	}
	if err != nil {
		return journal.Receipt{}, false, err
	}
	if doc.ID != id || doc.JournalID == "" || len(doc.Digest) != 32 {
		return journal.Receipt{}, false, errors.New("invalid stored journal receipt")
	}
	receipt := journal.Receipt{JournalID: doc.JournalID}
	copy(receipt.Digest[:], doc.Digest)
	return receipt, true, nil
}
func (t *journalTransaction) Journal(_ context.Context, id string) (bool, error) {
	var doc journalDocument
	err := t.store.journals.FindOne(t.ctx, bson.D{{Key: "_id", Value: id}}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	event, err := lifecycle.Decode(doc.Payload)
	if err != nil {
		return false, err
	}
	if event.GetLedgerJournal() == nil || event.GetLedgerJournal().JournalId != id || doc.ID != id || doc.EventID != event.EventId {
		return false, errors.New("stored journal coordinates conflict")
	}
	return true, nil
}
func (t *journalTransaction) LockAccount(_ context.Context, id string) (journal.State, error) {
	var doc accountDocument
	err := t.store.accounts.FindOne(t.ctx, bson.D{{Key: "_id", Value: id}}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		t.states[id] = journal.State{}
		return journal.State{}, nil
	}
	if err != nil {
		return journal.State{}, err
	}
	state, err := decodeAccount(doc)
	if err != nil {
		return journal.State{}, err
	}
	t.states[id] = state
	return state, nil
}
func (t *journalTransaction) PutAccount(_ context.Context, b *wire.AccountBalanceSnapshot) error {
	if b == nil {
		return errors.New("journal account snapshot is required")
	}
	state, ok := t.states[b.UserAssetAccountId]
	if !ok || b.PreviousVersion != state.Version || b.Version <= state.Version || b.Version > math.MaxInt64 || (state.Version > 0 && (state.UserID != b.UserId || state.AssetID != b.AssetId)) {
		return errors.New("journal account write requires locked version chain and stable identity")
	}
	doc, err := encodeAccount(b)
	if err != nil {
		return err
	}
	if state.Version == 0 {
		_, err = t.store.accounts.InsertOne(t.ctx, doc)
		return err
	}
	result, err := t.store.accounts.ReplaceOne(t.ctx, bson.D{{Key: "_id", Value: doc.ID}, {Key: "version", Value: int64(state.Version)}, {Key: "user_id", Value: state.UserID}, {Key: "asset_id", Value: state.AssetID}}, doc)
	if err != nil {
		return err
	}
	if result.MatchedCount != 1 {
		return errConcurrentChange
	}
	return nil
}
func (t *journalTransaction) PutJournal(_ context.Context, event *wire.LifecycleEvent) error {
	payload, err := lifecycle.Encode(event)
	if err != nil {
		return err
	}
	j := event.GetLedgerJournal()
	if j == nil {
		return errors.New("expected journal fact")
	}
	_, err = t.store.journals.InsertOne(t.ctx, journalDocument{ID: j.JournalId, UserIDs: journalOwners(j), EventID: event.EventId, OccurredAt: event.OccurredAt.AsTime(), Payload: payload})
	return err
}
func (t *journalTransaction) PutInbox(_ context.Context, id string, r journal.Receipt) error {
	if id == "" || r.JournalID == "" {
		return errors.New("invalid journal inbox coordinates")
	}
	_, err := t.store.inbox.InsertOne(t.ctx, journalInboxDocument{ID: id, JournalID: r.JournalID, Digest: r.Digest[:]})
	return err
}

func journalOwners(j *wire.LedgerJournalPosted) []string {
	owners := []string{}
	seen := map[string]bool{}
	for _, b := range j.Balances {
		if !seen[b.UserId] {
			seen[b.UserId] = true
			owners = append(owners, b.UserId)
		}
	}
	return owners
}
