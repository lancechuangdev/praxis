package mongostore

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"sort"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	lifecycle "praxis/api/events/lifecycle/v1"
	"praxis/projectionservice/internal/journal"
	"praxis/projectionservice/internal/workflow"
)

// ExportRecord is the portable source format used for replay and reconciliation.
// Payload contains original Protobuf bytes (base64 in JSON), never a command.
type ExportRecord struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Key       []byte `json:"key"`
	Payload   []byte `json:"payload"`
}

const projectionSchemaVersion = 1
const comparisonDocuments = 400000

type Report struct {
	SchemaVersion int       `bson:"schema_version" json:"schema_version"`
	ID            string    `bson:"_id" json:"id"`
	Stream        string    `bson:"stream" json:"stream"`
	Generation    string    `bson:"generation" json:"generation"`
	Mode          string    `bson:"mode" json:"mode"`
	SourceDigest  string    `bson:"source_digest" json:"source_digest"`
	TargetDigest  string    `bson:"target_digest" json:"target_digest"`
	Documents     int       `bson:"documents" json:"documents"`
	Verified      bool      `bson:"verified" json:"verified"`
	CreatedAt     time.Time `bson:"created_at" json:"created_at"`
}
type Route struct {
	ID         string `bson:"_id" json:"stream"`
	Generation string `bson:"generation" json:"generation"`
	Mode       string `bson:"mode" json:"mode"`
	ReportID   string `bson:"report_id" json:"report_id"`
	Revision   int64  `bson:"revision" json:"revision"`
}
type Operations struct {
	core   *Store
	stream string
	queue  *Queue
}

func NewOperations(client *mongo.Client, database string, target workflow.Target, stream string, timeout time.Duration) (*Operations, error) {
	q, err := NewQueue(client, database, target, stream, timeout)
	if err != nil {
		return nil, err
	}
	return &Operations{core: q.core, queue: q, stream: stream}, nil
}
func (o *Operations) Prepare(ctx context.Context) error {
	for _, name := range []string{"projection_reports", "projection_routes", "projection_replays", "projection_route_history"} {
		err := o.core.db.CreateCollection(ctx, name)
		var command mongo.CommandError
		if err != nil && !(errors.As(err, &command) && command.Code == 48) {
			return err
		}
	}
	return o.verifyControls(ctx)
}
func (o *Operations) verifyControls(ctx context.Context) error {
	return o.core.verifyCollections(ctx, o.core.db.Collection("projection_reports"), o.core.db.Collection("projection_routes"), o.core.db.Collection("projection_replays"), o.core.db.Collection("projection_route_history"))
}

// ScanExport is bounded in record size and total input size. Operations never
// execute commands or mutate authoritative services during replay.
func ScanExport(input io.Reader, fn func(ExportRecord) error) (string, error) {
	hash := sha256.New()
	limited := &io.LimitedReader{R: input, N: 128*1024*1024 + 1}
	scanner := bufio.NewScanner(io.TeeReader(limited, hash))
	scanner.Buffer(make([]byte, 64*1024), 3*lifecycle.MaxEventBytes)
	count := 0
	for scanner.Scan() {
		count++
		if count > 100000 {
			return "", errors.New("export exceeds 100000 records")
		}
		var record ExportRecord
		decoder := json.NewDecoder(strings.NewReader(scanner.Text()))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&record); err != nil {
			return "", err
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			return "", errors.New("trailing export data")
		}
		if err := fn(record); err != nil {
			return "", err
		}
	}
	if err := scanner.Err(); err != nil {
		return "", err
	}
	if limited.N <= 0 {
		return "", errors.New("export exceeds 128 MiB")
	}
	if count == 0 {
		return "", errors.New("export is empty")
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
func expectedExport(input io.Reader, stream string) (map[string]string, string, error) {
	expected := map[string]string{}
	versions := map[string]uint64{}
	digest, err := ScanExport(input, func(r ExportRecord) error {
		e, err := lifecycle.Decode(r.Payload)
		if err != nil {
			return err
		}
		key, err := lifecycle.Key(e)
		if err != nil || string(r.Key) != key {
			return errors.New("export key mismatch")
		}
		payload, err := lifecycle.Encode(e)
		if err != nil {
			return err
		}
		if stream == "workflow" {
			w := e.GetOrderWorkflow()
			if r.Topic != "order.events.v1" || w == nil {
				return errors.New("wrong workflow export stream")
			}
			historyKey := "workflow-history:" + e.EventId
			if old, ok := expected[historyKey]; ok && old != payloadHash(payload) {
				return errors.New("conflicting workflow history export")
			}
			expected[historyKey] = payloadHash(payload)
			inboxKey := "workflow-inbox:" + e.EventId
			receipt := workflow.Receipt{Digest: sha256.Sum256(r.Payload), OrderID: w.OrderId, Version: w.WorkflowVersion}
			receiptRaw, _ := json.Marshal(receipt)
			if old, ok := expected[inboxKey]; ok && old != payloadHash(receiptRaw) {
				return errors.New("conflicting workflow inbox export")
			}
			expected[inboxKey] = payloadHash(receiptRaw)
			id := "workflow:" + w.OrderId
			hash := payloadHash(payload)
			if w.WorkflowVersion == versions[id] && expected[id] != hash {
				return errors.New("conflicting workflow export version")
			}
			if w.WorkflowVersion > versions[id] {
				versions[id] = w.WorkflowVersion
				expected[id] = hash
			}
		} else {
			j := e.GetLedgerJournal()
			if r.Topic != "ledger.journals.v1" || j == nil {
				return errors.New("wrong journal export stream")
			}
			id := "journal:" + j.JournalId
			hash := payloadHash(payload)
			if old, ok := expected[id]; ok && old != hash {
				return errors.New("conflicting journal export")
			}
			expected[id] = hash
			receiptRaw, _ := json.Marshal(journal.Receipt{Digest: sha256.Sum256(r.Payload), JournalID: j.JournalId})
			inboxKey := "journal-inbox:" + e.EventId
			if old, ok := expected[inboxKey]; ok && old != payloadHash(receiptRaw) {
				return errors.New("conflicting journal inbox export")
			}
			expected[inboxKey] = payloadHash(receiptRaw)
			for _, b := range j.Balances {
				id := "account:" + b.UserAssetAccountId
				doc, err := encodeAccount(b)
				if err != nil {
					return err
				}
				hash := payloadHash(doc.Payload)
				if b.Version == versions[id] && expected[id] != hash {
					return errors.New("conflicting account export version")
				}
				if b.Version > versions[id] {
					versions[id] = b.Version
					expected[id] = hash
				}
			}
		}
		if len(expected) > comparisonDocuments {
			return errors.New("comparison exceeds document bound")
		}
		return nil
	})
	return expected, digest, err
}
func payloadHash(raw []byte) string { sum := sha256.Sum256(raw); return hex.EncodeToString(sum[:]) }
func mapDigest(records map[string]string) string {
	keys := make([]string, 0, len(records))
	for k := range records {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	hash := sha256.New()
	for _, k := range keys {
		fmt.Fprintf(hash, "%d:%s%d:%s", len(k), k, len(records[k]), records[k])
	}
	return hex.EncodeToString(hash.Sum(nil))
}
func (o *Operations) snapshot(ctx context.Context) (map[string]string, error) {
	var records map[string]string
	err := o.core.withTransaction(ctx, func(ctx context.Context) error { var err error; records, err = o.readSnapshot(ctx); return err })
	return records, err
}
func (o *Operations) readSnapshot(ctx context.Context) (map[string]string, error) {
	records := map[string]string{}
	suffix := string(o.core.target.Mode) + "_" + o.core.target.Generation
	names := []string{"workflow_" + suffix, "workflow_history_" + suffix, "workflow_inbox_" + suffix}
	if o.stream == "journal" {
		names = []string{"journals_" + suffix, "journal_accounts_" + suffix, "journal_inbox_" + suffix}
	}
	for _, name := range names {
		cursor, err := o.core.db.Collection(name).Find(ctx, bson.D{}, options.Find().SetBatchSize(64))
		if err != nil {
			return nil, err
		}
		err = func() error {
			defer cursor.Close(ctx)
			for cursor.Next(ctx) {
				if len(records) >= comparisonDocuments {
					return errors.New("projection comparison exceeds document limit")
				}
				if strings.HasPrefix(name, "workflow_inbox_") {
					var doc inboxDocument
					if err := cursor.Decode(&doc); err != nil {
						return err
					}
					if len(doc.Digest) != 32 || doc.Version <= 0 || doc.OrderID == "" {
						return errors.New("invalid workflow receipt")
					}
					receipt := workflow.Receipt{OrderID: doc.OrderID, Version: uint64(doc.Version)}
					copy(receipt.Digest[:], doc.Digest)
					raw, _ := json.Marshal(receipt)
					records["workflow-inbox:"+doc.ID] = payloadHash(raw)
				} else if strings.HasPrefix(name, "journal_inbox_") {
					var doc journalInboxDocument
					if err := cursor.Decode(&doc); err != nil {
						return err
					}
					if len(doc.Digest) != 32 || doc.JournalID == "" {
						return errors.New("invalid journal receipt")
					}
					receipt := journal.Receipt{JournalID: doc.JournalID}
					copy(receipt.Digest[:], doc.Digest)
					raw, _ := json.Marshal(receipt)
					records["journal-inbox:"+doc.ID] = payloadHash(raw)
				} else if o.stream == "workflow" && strings.HasPrefix(name, "workflow_history_") {
					var doc workflowHistoryDocument
					if err := cursor.Decode(&doc); err != nil {
						return err
					}
					e, err := lifecycle.Decode(doc.Payload)
					if err != nil {
						return err
					}
					w := e.GetOrderWorkflow()
					if w == nil || doc.ID != e.EventId || doc.OrderID != w.OrderId || doc.UserID != w.UserId || doc.Version <= 0 || uint64(doc.Version) != w.WorkflowVersion || !doc.OccurredAt.Equal(e.OccurredAt.AsTime().Truncate(time.Millisecond)) {
						return errors.New("stored workflow history identity mismatch")
					}
					payload, err := lifecycle.Encode(e)
					if err != nil {
						return err
					}
					records["workflow-history:"+doc.ID] = payloadHash(payload)
				} else if o.stream == "workflow" {
					var doc snapshotDocument
					if err := cursor.Decode(&doc); err != nil {
						return err
					}
					e, err := lifecycle.Decode(doc.Payload)
					if err != nil {
						return err
					}
					w := e.GetOrderWorkflow()
					if w == nil || doc.ID != w.OrderId || doc.UserID != w.UserId || doc.Version <= 0 || uint64(doc.Version) != w.WorkflowVersion || doc.EventID != e.EventId || !doc.OccurredAt.Equal(e.OccurredAt.AsTime().Truncate(time.Millisecond)) {
						return errors.New("stored workflow identity mismatch")
					}
					payload, err := lifecycle.Encode(e)
					if err != nil {
						return err
					}
					records["workflow:"+doc.ID] = payloadHash(payload)
				} else if strings.HasPrefix(name, "journals_") {
					var doc journalDocument
					if err := cursor.Decode(&doc); err != nil {
						return err
					}
					e, err := lifecycle.Decode(doc.Payload)
					if err != nil {
						return err
					}
					j := e.GetLedgerJournal()
					if j == nil || doc.ID != j.JournalId || doc.EventID != e.EventId || !doc.OccurredAt.Equal(e.OccurredAt.AsTime().Truncate(time.Millisecond)) {
						return errors.New("stored journal identity mismatch")
					}
					owners := journalOwners(j)
					sort.Strings(owners)
					sort.Strings(doc.UserIDs)
					if !slices.Equal(owners, doc.UserIDs) {
						return errors.New("stored journal owner index mismatch")
					}
					payload, err := lifecycle.Encode(e)
					if err != nil {
						return err
					}
					records["journal:"+doc.ID] = payloadHash(payload)
				} else {
					var doc accountDocument
					if err := cursor.Decode(&doc); err != nil {
						return err
					}
					if _, err := decodeAccount(doc); err != nil {
						return err
					}
					records["account:"+doc.ID] = payloadHash(doc.Payload)
				}
			}
			return cursor.Err()
		}()
		if err != nil {
			return nil, err
		}
	}
	return records, nil
}
func (o *Operations) emptyQueue(ctx context.Context) error {
	if err := o.queue.Verify(ctx); err != nil {
		return err
	}
	var quota struct {
		Records int64 `bson:"records"`
		Bytes   int64 `bson:"bytes"`
	}
	if err := o.queue.quota.FindOne(ctx, bson.D{{Key: "_id", Value: "active"}}).Decode(&quota); err != nil {
		return err
	}
	if quota.Records != 0 || quota.Bytes != 0 {
		return errors.New("pending or quarantined records block reconciliation and cutover")
	}
	return nil
}

// Reconcile requires an independently exported authoritative source snapshot.
// Pause consumers and drain before this operation; reports are immutable and
// cutover rechecks the complete target digest and empty recovery queue.
func (o *Operations) Reconcile(ctx context.Context, input io.Reader) (Report, error) {
	expected, source, err := expectedExport(input, o.stream)
	if err != nil {
		return Report{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if o.core.target.Mode == workflow.Rebuild {
		err := o.core.db.Collection("projection_replays").FindOne(ctx, bson.D{{Key: "stream", Value: o.stream}, {Key: "mode", Value: string(o.core.target.Mode)}, {Key: "generation", Value: o.core.target.Generation}, {Key: "artifact_digest", Value: source}, {Key: "complete", Value: true}}).Err()
		if err != nil {
			return Report{}, errors.New("rebuild has no complete checkpoint for this authoritative artifact")
		}
	}
	if err := o.verifyControls(ctx); err != nil {
		return Report{}, err
	}
	if err := o.emptyQueue(ctx); err != nil {
		return Report{}, err
	}
	actual, err := o.snapshot(ctx)
	if err != nil {
		return Report{}, err
	}
	digest := mapDigest(expected)
	if digest != mapDigest(actual) || len(expected) != len(actual) {
		return Report{}, errors.New("projection differs from authoritative export")
	}
	id := payloadHash([]byte(fmt.Sprintf("%d:%s:%s:%s:%s:%s", projectionSchemaVersion, o.stream, o.core.target.Mode, o.core.target.Generation, source, digest)))
	report := Report{SchemaVersion: projectionSchemaVersion, ID: id, Stream: o.stream, Generation: o.core.target.Generation, Mode: string(o.core.target.Mode), SourceDigest: source, TargetDigest: digest, Documents: len(actual), Verified: true, CreatedAt: time.Now().UTC()}
	_, err = o.core.db.Collection("projection_reports").InsertOne(ctx, report)
	if mongo.IsDuplicateKeyError(err) {
		var old Report
		if err := o.core.db.Collection("projection_reports").FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&old); err != nil {
			return Report{}, err
		}
		if old.TargetDigest != digest || old.SourceDigest != source || !old.Verified {
			return Report{}, errors.New("conflicting reconciliation report")
		}
		return old, nil
	}
	return report, err
}
func (o *Operations) Activate(ctx context.Context, reportID string, expectedRevision int64) (Route, error) {
	if expectedRevision < 0 || expectedRevision == math.MaxInt64 {
		return Route{}, errors.New("expected route revision must be nonnegative")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var report Report
	if err := o.core.db.Collection("projection_reports").FindOne(ctx, bson.D{{Key: "_id", Value: reportID}}).Decode(&report); err != nil {
		return Route{}, err
	}
	if report.SchemaVersion != projectionSchemaVersion || !report.Verified || report.Stream != o.stream || report.Generation != o.core.target.Generation || report.Mode != string(o.core.target.Mode) {
		return Route{}, errors.New("reconciliation report target mismatch")
	}
	if err := o.verifyControls(ctx); err != nil {
		return Route{}, err
	}
	if err := o.emptyQueue(ctx); err != nil {
		return Route{}, err
	}
	actual, err := o.snapshot(ctx)
	if err != nil {
		return Route{}, err
	}
	if mapDigest(actual) != report.TargetDigest || len(actual) != report.Documents {
		return Route{}, errors.New("target changed after reconciliation")
	}
	route := Route{ID: o.stream, Generation: report.Generation, Mode: report.Mode, ReportID: report.ID, Revision: expectedRevision + 1}
	err = o.core.withTransaction(ctx, func(ctx context.Context) error {
		var previous Route
		readErr := o.core.db.Collection("projection_routes").FindOne(ctx, bson.D{{Key: "_id", Value: o.stream}}).Decode(&previous)
		if readErr != nil && !errors.Is(readErr, mongo.ErrNoDocuments) {
			return readErr
		}
		if (errors.Is(readErr, mongo.ErrNoDocuments) && expectedRevision != 0) || (readErr == nil && (expectedRevision == 0 || previous.Revision != expectedRevision)) {
			return errors.New("read route changed concurrently")
		}
		if errors.Is(readErr, mongo.ErrNoDocuments) {
			if _, err := o.core.db.Collection("projection_routes").InsertOne(ctx, route); err != nil {
				return err
			}
		} else {
			result, err := o.core.db.Collection("projection_routes").ReplaceOne(ctx, bson.D{{Key: "_id", Value: o.stream}, {Key: "revision", Value: expectedRevision}}, route)
			if err != nil {
				return err
			}
			if result.MatchedCount != 1 {
				return errors.New("read route changed concurrently")
			}
		}
		_, err := o.core.db.Collection("projection_route_history").InsertOne(ctx, bson.D{{Key: "_id", Value: fmt.Sprintf("%s:%d", o.stream, route.Revision)}, {Key: "stream", Value: o.stream}, {Key: "previous", Value: previous}, {Key: "current", Value: route}, {Key: "activated_at", Value: time.Now().UTC()}})
		return err
	})
	if err != nil {
		return Route{}, err
	}
	return route, nil
}
