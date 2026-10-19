package mongostore

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"google.golang.org/protobuf/encoding/protojson"

	lifecycle "praxis/api/events/lifecycle/v1"
	"praxis/projectionservice/internal/readapi"
	"praxis/projectionservice/internal/workflow"
)

// ReadStore is permanently bound to one generation; cutover revokes old
// instances through a route CAS. It has no authoritative mutation clients.
type ReadStore struct {
	core       *Store
	stream     string
	collection *mongo.Collection
}

func NewReads(client *mongo.Client, database string, target workflow.Target, stream string, timeout time.Duration) (*ReadStore, error) {
	core, err := New(client, database, target, timeout)
	if err != nil {
		return nil, err
	}
	name := "workflow_"
	if stream == "journal" {
		name = "journals_"
	} else if stream != "workflow" {
		return nil, errors.New("invalid read stream")
	}
	return &ReadStore{core: core, stream: stream, collection: core.db.Collection(name + string(target.Mode) + "_" + target.Generation)}, nil
}
func (s *ReadStore) Ready(ctx context.Context) error {
	var quota struct {
		Records int64 `bson:"records"`
	}
	quotaName := "queue_quota_" + s.stream + "_" + string(s.core.target.Mode) + "_" + s.core.target.Generation
	if err := s.core.db.Collection(quotaName).FindOne(ctx, bson.D{{Key: "_id", Value: "active"}}).Decode(&quota); err != nil {
		return err
	}
	if quota.Records != 0 {
		return errors.New("projection has unapplied or quarantined source records")
	}
	var route Route
	if err := s.core.db.Collection("projection_routes").FindOne(ctx, bson.D{{Key: "_id", Value: s.stream}}).Decode(&route); err != nil {
		return err
	}
	if route.Generation != s.core.target.Generation || route.Mode != string(s.core.target.Mode) || route.Revision < 1 || route.ReportID == "" {
		return errors.New("projection generation is not the active read route")
	}
	var report Report
	if err := s.core.db.Collection("projection_reports").FindOne(ctx, bson.D{{Key: "_id", Value: route.ReportID}}).Decode(&report); err != nil {
		return err
	}
	if report.SchemaVersion != projectionSchemaVersion || report.Generation != route.Generation || report.Mode != route.Mode || report.Stream != s.stream || !report.Verified {
		return errors.New("read route has no matching reconciliation report")
	}
	return nil
}
func (s *ReadStore) Get(ctx context.Context, kind, owner, id string) (readapi.Document, error) {
	filter := bson.D{{Key: "_id", Value: id}}
	if s.stream == "workflow" && kind == "orders" {
		filter = append(filter, bson.E{Key: "user_id", Value: owner})
		var d snapshotDocument
		err := s.collection.FindOne(ctx, filter).Decode(&d)
		if errors.Is(err, mongo.ErrNoDocuments) {
			return readapi.Document{}, readapi.ErrNotFound
		}
		if err != nil {
			return readapi.Document{}, err
		}
		return workflowRead(d, owner)
	}
	if s.stream == "journal" && kind == "activity" {
		filter = append(filter, bson.E{Key: "user_ids", Value: owner})
		var d journalDocument
		err := s.collection.FindOne(ctx, filter).Decode(&d)
		if errors.Is(err, mongo.ErrNoDocuments) {
			return readapi.Document{}, readapi.ErrNotFound
		}
		if err != nil {
			return readapi.Document{}, err
		}
		return activityRead(d, owner)
	}
	return readapi.Document{}, readapi.ErrNotFound
}

type readCursor struct {
	Scope string    `json:"s"`
	Time  time.Time `json:"t"`
	ID    string    `json:"i"`
}

func (s *ReadStore) cursorScope(kind, owner string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s:%s:%s:%s:%s", s.stream, s.core.target.Mode, s.core.target.Generation, kind, owner)))
	return hex.EncodeToString(sum[:])
}
func (s *ReadStore) List(ctx context.Context, kind, owner, after string, limit int) (readapi.Page, error) {
	if strings.HasPrefix(kind, "order-history:") {
		return s.orderHistory(ctx, owner, strings.TrimPrefix(kind, "order-history:"), after, limit)
	}
	page := readapi.Page{Items: []readapi.Document{}}
	if limit < 1 || limit > 50 {
		return page, readapi.ErrCursor
	}
	field := "user_id"
	if s.stream == "journal" && kind == "activity" {
		field = "user_ids"
	} else if s.stream != "workflow" || kind != "orders" {
		return page, readapi.ErrNotFound
	}
	filter := bson.D{{Key: field, Value: owner}}
	scope := s.cursorScope(kind, owner)
	if after != "" {
		if len(after) > 2048 {
			return page, readapi.ErrCursor
		}
		raw, err := base64.RawURLEncoding.DecodeString(after)
		if err != nil {
			return page, readapi.ErrCursor
		}
		var c readCursor
		decoder := json.NewDecoder(strings.NewReader(string(raw)))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&c); err != nil || c.Scope != scope || c.ID == "" || len(c.ID) > 256 || c.Time.IsZero() {
			return page, readapi.ErrCursor
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			return page, readapi.ErrCursor
		}
		filter = append(filter, bson.E{Key: "$or", Value: bson.A{bson.D{{Key: "occurred_at", Value: bson.D{{Key: "$gt", Value: c.Time}}}}, bson.D{{Key: "occurred_at", Value: c.Time}, {Key: "_id", Value: bson.D{{Key: "$gt", Value: c.ID}}}}}})
	}
	cursor, err := s.collection.Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "occurred_at", Value: 1}, {Key: "_id", Value: 1}}).SetLimit(int64(limit+1)))
	if err != nil {
		return page, err
	}
	defer cursor.Close(ctx)
	var last readCursor
	for cursor.Next(ctx) {
		if len(page.Items) == limit {
			raw, _ := json.Marshal(last)
			page.Next = base64.RawURLEncoding.EncodeToString(raw)
			break
		}
		var doc readapi.Document
		if s.stream == "workflow" {
			var d snapshotDocument
			if err := cursor.Decode(&d); err != nil {
				return page, err
			}
			doc, err = workflowRead(d, owner)
			last = readCursor{Scope: scope, Time: d.OccurredAt, ID: d.ID}
		} else {
			var d journalDocument
			if err := cursor.Decode(&d); err != nil {
				return page, err
			}
			doc, err = activityRead(d, owner)
			last = readCursor{Scope: scope, Time: d.OccurredAt, ID: d.ID}
		}
		if err != nil {
			return page, err
		}
		page.Items = append(page.Items, doc)
	}
	return page, cursor.Err()
}
func workflowRead(doc snapshotDocument, owner string) (readapi.Document, error) {
	e, err := lifecycle.Decode(doc.Payload)
	if err != nil {
		return readapi.Document{}, err
	}
	w := e.GetOrderWorkflow()
	if w == nil || w.UserId != owner || doc.UserID != owner || doc.ID != w.OrderId || doc.Version <= 0 || uint64(doc.Version) != w.WorkflowVersion || doc.EventID != e.EventId || !doc.OccurredAt.Equal(e.OccurredAt.AsTime().Truncate(time.Millisecond)) {
		return readapi.Document{}, readapi.ErrNotFound
	}
	data, err := protojson.Marshal(w)
	return readapi.Document{OccurredAt: e.OccurredAt.AsTime(), ID: doc.ID, Version: fmt.Sprint(doc.Version), Data: data}, err
}
func activityRead(doc journalDocument, owner string) (readapi.Document, error) {
	e, err := lifecycle.Decode(doc.Payload)
	if err != nil {
		return readapi.Document{}, err
	}
	j := e.GetLedgerJournal()
	if j == nil || j.JournalId != doc.ID || e.EventId != doc.EventID {
		return readapi.Document{}, readapi.ErrNotFound
	}
	balances := []json.RawMessage{}
	entries := []json.RawMessage{}
	owned := map[string]bool{}
	for _, b := range j.Balances {
		if b.UserId == owner {
			owned[b.UserAssetAccountId] = true
			raw, err := protojson.Marshal(b)
			if err != nil {
				return readapi.Document{}, err
			}
			balances = append(balances, raw)
		}
	}
	if len(owned) == 0 {
		return readapi.Document{}, readapi.ErrNotFound
	}
	for _, entry := range j.Entries {
		if owned[entry.UserAssetAccountId] {
			raw, err := protojson.Marshal(entry)
			if err != nil {
				return readapi.Document{}, err
			}
			entries = append(entries, raw)
		}
	}
	// This is a customer activity view, not a canonical complete Ledger fact.
	data, err := json.Marshal(struct {
		Type       string            `json:"type"`
		OccurredAt time.Time         `json:"occurred_at"`
		Entries    []json.RawMessage `json:"entries"`
		Balances   []json.RawMessage `json:"balances"`
	}{j.BusinessType, e.OccurredAt.AsTime(), entries, balances})
	return readapi.Document{OccurredAt: e.OccurredAt.AsTime(), ID: doc.ID, Version: e.EventId, Data: data}, err
}

// Verify requires owner-scoped indexes before opening read traffic.
func (s *ReadStore) Verify(ctx context.Context) error {
	collections := []*mongo.Collection{s.collection}
	if s.stream == "workflow" {
		collections = append(collections, s.core.history)
	}
	if err := s.core.verifyCollections(ctx, collections...); err != nil {
		return err
	}
	field := "user_id"
	if s.stream == "journal" {
		field = "user_ids"
	}
	if err := s.verifyReadIndex(ctx, s.collection, []string{field, "occurred_at", "_id"}); err != nil {
		return err
	}
	if s.stream == "workflow" {
		return s.verifyReadIndex(ctx, s.core.history, []string{"user_id", "order_id", "workflow_version"})
	}
	return nil
}
func (s *ReadStore) verifyReadIndex(ctx context.Context, collection *mongo.Collection, names []string) error {
	ctx, cancel := context.WithTimeout(ctx, s.core.timeout)
	defer cancel()
	cursor, err := collection.Indexes().List(ctx)
	if err != nil {
		return err
	}
	var indexes []bson.Raw
	if err := cursor.All(ctx, &indexes); err != nil {
		return err
	}
	for _, index := range indexes {
		key, ok := index.Lookup("key").DocumentOK()
		if !ok {
			continue
		}
		elements, err := key.Elements()
		if err != nil {
			return err
		}
		if len(elements) != len(names) {
			continue
		}
		valid := true
		for i, e := range elements {
			value, ok := integerValue(e.Value())
			if e.Key() != names[i] || !ok || value != 1 {
				valid = false
			}
		}
		if valid {
			return nil
		}
	}
	return errors.New("required owner-scoped pagination index is missing")
}

func (s *ReadStore) orderHistory(ctx context.Context, owner, orderID, after string, limit int) (readapi.Page, error) {
	page := readapi.Page{Items: []readapi.Document{}}
	if s.stream != "workflow" {
		return page, readapi.ErrNotFound
	}
	if limit < 1 || limit > 50 || orderID == "" || len(orderID) > 256 || len(after) > 2048 {
		return page, readapi.ErrCursor
	}
	type cursor struct {
		Scope   string `json:"s"`
		Version int64  `json:"v"`
	}
	scope := s.cursorScope("order-history:"+orderID, owner)
	filter := bson.D{{Key: "user_id", Value: owner}, {Key: "order_id", Value: orderID}}
	if after != "" {
		raw, err := base64.RawURLEncoding.DecodeString(after)
		if err != nil {
			return page, readapi.ErrCursor
		}
		var c cursor
		decoder := json.NewDecoder(strings.NewReader(string(raw)))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&c); err != nil || c.Scope != scope || c.Version <= 0 {
			return page, readapi.ErrCursor
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			return page, readapi.ErrCursor
		}
		filter = append(filter, bson.E{Key: "workflow_version", Value: bson.D{{Key: "$gt", Value: c.Version}}})
	}
	collection := s.core.db.Collection("workflow_history_" + string(s.core.target.Mode) + "_" + s.core.target.Generation)
	rows, err := collection.Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "workflow_version", Value: 1}}).SetLimit(int64(limit+1)))
	if err != nil {
		return page, err
	}
	defer rows.Close(ctx)
	var last int64
	for rows.Next(ctx) {
		if len(page.Items) == limit {
			raw, _ := json.Marshal(cursor{Scope: scope, Version: last})
			page.Next = base64.RawURLEncoding.EncodeToString(raw)
			break
		}
		var doc workflowHistoryDocument
		if err := rows.Decode(&doc); err != nil {
			return page, err
		}
		e, err := lifecycle.Decode(doc.Payload)
		if err != nil {
			return page, err
		}
		w := e.GetOrderWorkflow()
		if w == nil || w.UserId != owner || doc.UserID != owner || doc.OrderID != orderID || w.OrderId != orderID || doc.ID != e.EventId || doc.Version <= 0 || uint64(doc.Version) != w.WorkflowVersion {
			return page, errors.New("stored workflow history coordinates conflict")
		}
		data, err := protojson.Marshal(w)
		if err != nil {
			return page, err
		}
		page.Items = append(page.Items, readapi.Document{OccurredAt: e.OccurredAt.AsTime(), ID: doc.ID, Version: fmt.Sprint(doc.Version), Data: data})
		last = doc.Version
	}
	return page, rows.Err()
}
