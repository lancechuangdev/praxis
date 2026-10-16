// Package mongostore implements the isolated workflow projection transaction
// protocol. Clients, credentials, networking and consumer activation are owned
// by future deployment/runtime code.
package mongostore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
	"google.golang.org/protobuf/proto"
	lifecycle "praxis/api/events/lifecycle/v1"
	wire "praxis/api/gen/events/lifecycle/v1"
	"praxis/projectionservice/internal/workflow"
)

var errConcurrentChange = errors.New("workflow changed concurrently")

type Store struct {
	client           *mongo.Client
	db               *mongo.Database
	target           workflow.Target
	snapshots, inbox *mongo.Collection
	timeout          time.Duration
}

var _ workflow.Store = (*Store)(nil)

// New binds the store permanently to one target. Call Prepare explicitly before
// use with a new generation; it is never called implicitly during processing.
// The client remains caller-owned and must use deployment-approved TLS/auth.
func New(client *mongo.Client, database string, target workflow.Target, timeout time.Duration) (*Store, error) {
	if client == nil || database == "" || target.Validate() != nil || timeout <= 0 || timeout > 30*time.Second {
		return nil, errors.New("Mongo workflow store requires client, database, valid target and timeout in (0, 30s]")
	}
	db := client.Database(database, options.Database().SetWriteConcern(writeconcern.Majority()).SetReadPreference(readpref.Primary()))
	suffix := string(target.Mode) + "_" + target.Generation
	return &Store{client: client, db: db, target: target, timeout: timeout,
		snapshots: db.Collection("workflow_" + suffix), inbox: db.Collection("workflow_inbox_" + suffix)}, nil
}

// Prepare creates ordinary, unsharded collections. Their built-in unique _id
// indexes enforce order-ID and event-ID uniqueness. No TTL or destructive
// cleanup is installed. Provision privileges can be separated from runtime.
func (s *Store) Prepare(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	for _, name := range []string{s.snapshots.Name(), s.inbox.Name()} {
		err := s.db.CreateCollection(ctx, name)
		var command mongo.CommandError
		if err != nil && !(errors.As(err, &command) && command.Code == 48) {
			return err
		}
	}
	return nil
}

func (s *Store) Within(ctx context.Context, target workflow.Target, callback func(workflow.Transaction) error) error {
	if target != s.target || callback == nil {
		return errors.New("Mongo workflow target mismatch or missing callback")
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	session, err := s.client.StartSession()
	if err != nil {
		return err
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		session.EndSession(cleanup)
	}()
	txnOptions := options.Transaction().SetReadConcern(readconcern.Snapshot()).SetWriteConcern(writeconcern.Majority()).SetReadPreference(readpref.Primary())
	// Duplicate-key races on first insert may not carry the driver's transient
	// transaction label. Retry the whole aborted transaction, never a write in
	// an aborted transaction. The shared deadline includes every retry/commit.
	for attempt := 0; attempt < 4; attempt++ {
		_, err = session.WithTransaction(ctx, func(sessionCtx context.Context) (any, error) {
			tx := &transaction{ctx: sessionCtx, store: s, states: make(map[string]workflow.State)}
			return nil, callback(tx)
		}, txnOptions)
		if err == nil || (!mongo.IsDuplicateKeyError(err) && !errors.Is(err, errConcurrentChange)) {
			return err
		}
		if attempt == 3 {
			break
		}
		timer := time.NewTimer(time.Duration(5+rand.IntN(16)) * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return err
}

type snapshotDocument struct {
	ID         string    `bson:"_id"`
	Version    int64     `bson:"workflow_version"`
	UserID     string    `bson:"user_id"`
	EventID    string    `bson:"event_id"`
	OccurredAt time.Time `bson:"occurred_at"`
	Payload    []byte    `bson:"payload"`
}
type inboxDocument struct {
	ID      string `bson:"_id"`
	OrderID string `bson:"order_id"`
	Version int64  `bson:"workflow_version"`
	Digest  []byte `bson:"digest"`
}
type transaction struct {
	ctx    context.Context // Includes the Mongo session and the caller's deadline.
	store  *Store
	states map[string]workflow.State
}

func (t *transaction) LockWorkflow(_ context.Context, id string) (workflow.State, error) {
	var doc snapshotDocument
	err := t.store.snapshots.FindOne(t.ctx, bson.D{{Key: "_id", Value: id}}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		t.states[id] = workflow.State{}
		return workflow.State{}, nil
	}
	if err != nil {
		return workflow.State{}, err
	}
	event, err := lifecycle.Decode(doc.Payload)
	if err != nil {
		return workflow.State{}, fmt.Errorf("invalid stored workflow: %w", err)
	}
	w := event.GetOrderWorkflow()
	if w == nil || doc.ID != id || doc.Version <= 0 || w.OrderId != id || w.UserId != doc.UserID || w.WorkflowVersion != uint64(doc.Version) || event.EventId != doc.EventID {
		return workflow.State{}, errors.New("stored workflow coordinates conflict")
	}
	state := workflow.State{Version: uint64(doc.Version), UserID: doc.UserID}
	t.states[id] = state
	return state, nil
}

func (t *transaction) Inbox(_ context.Context, id string) (workflow.Receipt, bool, error) {
	var doc inboxDocument
	err := t.store.inbox.FindOne(t.ctx, bson.D{{Key: "_id", Value: id}}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return workflow.Receipt{}, false, nil
	}
	if err != nil {
		return workflow.Receipt{}, false, err
	}
	if len(doc.Digest) != 32 || doc.Version <= 0 || doc.OrderID == "" {
		return workflow.Receipt{}, false, errors.New("invalid stored workflow inbox receipt")
	}
	receipt := workflow.Receipt{OrderID: doc.OrderID, Version: uint64(doc.Version)}
	copy(receipt.Digest[:], doc.Digest)
	return receipt, true, nil
}

func (t *transaction) PutWorkflow(_ context.Context, event *wire.LifecycleEvent) error {
	doc, err := encodeSnapshot(event)
	if err != nil {
		return err
	}
	state, ok := t.states[doc.ID]
	if !ok || uint64(doc.Version) <= state.Version || uint64(doc.Version)-state.Version != 1 || (state.Version > 0 && state.UserID != doc.UserID) {
		return errors.New("workflow write requires a locked contiguous version and stable owner")
	}
	if state.Version == 0 {
		_, err = t.store.snapshots.InsertOne(t.ctx, doc)
		return err
	}
	result, err := t.store.snapshots.ReplaceOne(t.ctx, bson.D{{Key: "_id", Value: doc.ID}, {Key: "workflow_version", Value: int64(state.Version)}, {Key: "user_id", Value: state.UserID}}, doc)
	if err != nil {
		return err
	}
	if result.MatchedCount != 1 {
		return errConcurrentChange
	}
	return nil
}

func encodeSnapshot(event *wire.LifecycleEvent) (snapshotDocument, error) {
	if err := lifecycle.Validate(event); err != nil {
		return snapshotDocument{}, err
	}
	w := event.GetOrderWorkflow()
	if w == nil {
		return snapshotDocument{}, errors.New("expected Order workflow fact")
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(event)
	if err != nil {
		return snapshotDocument{}, err
	}
	return snapshotDocument{ID: w.OrderId, Version: int64(w.WorkflowVersion), UserID: w.UserId, EventID: event.EventId, OccurredAt: event.OccurredAt.AsTime(), Payload: payload}, nil
}

func (t *transaction) PutInbox(_ context.Context, id string, receipt workflow.Receipt) error {
	if id == "" || receipt.OrderID == "" || receipt.Version == 0 || receipt.Version > math.MaxInt64 {
		return errors.New("invalid workflow inbox coordinates")
	}
	_, err := t.store.inbox.InsertOne(t.ctx, inboxDocument{ID: id, OrderID: receipt.OrderID, Version: int64(receipt.Version), Digest: receipt.Digest[:]})
	return err
}
