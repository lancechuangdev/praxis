package mongostore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	lifecycle "praxis/api/events/lifecycle/v1"
	"praxis/projectionservice/internal/journal"
	"praxis/projectionservice/internal/workflow"
)

var ErrQueueFull = errors.New("projection recovery queue is full")

const queueRecords = 1024
const queueBytes = 64 * 1024 * 1024

// Queue retains original Kafka coordinates and bytes before source commit.
// Pending and quarantined records share a transactional quota; completed
// records remain as immutable delivery receipts for ambiguous Kafka commits.
type Queue struct {
	core           *Store
	target         workflow.Target
	stream         string
	records, quota *mongo.Collection
}
type QueueRecord struct {
	PayloadDigest     string    `bson:"digest,omitempty" json:"digest,omitempty"`
	Provenance        string    `bson:"provenance,omitempty" json:"provenance,omitempty"`
	OperatorReason    string    `bson:"operator_reason,omitempty" json:"operator_reason,omitempty"`
	OperatorRetries   int64     `bson:"operator_retries,omitempty" json:"operator_retries,omitempty"`
	OperatorRetriedAt time.Time `bson:"operator_retried_at,omitempty" json:"operator_retried_at,omitempty"`
	ID                string    `bson:"_id" json:"id"`
	Topic             string    `bson:"topic" json:"topic"`
	Partition         int       `bson:"partition" json:"partition"`
	Offset            int64     `bson:"offset" json:"offset"`
	Key               []byte    `bson:"key" json:"key"`
	Payload           []byte    `bson:"payload" json:"payload"`
	State             string    `bson:"state" json:"state"`
	Reason            string    `bson:"reason" json:"reason"`
	Attempts          int64     `bson:"attempts" json:"attempts"`
	Next              time.Time `bson:"next_attempt_at" json:"next_attempt_at"`
}

func NewQueue(client *mongo.Client, database string, target workflow.Target, stream string, timeout time.Duration) (*Queue, error) {
	if stream != "workflow" && stream != "journal" {
		return nil, errors.New("unsupported queue stream")
	}
	core, err := New(client, database, target, timeout)
	if err != nil {
		return nil, err
	}
	suffix := stream + "_" + string(target.Mode) + "_" + target.Generation
	return &Queue{core: core, target: target, stream: stream, records: core.db.Collection("queue_" + suffix), quota: core.db.Collection("queue_quota_" + suffix)}, nil
}
func (q *Queue) Scope() (workflow.Target, string) { return q.target, q.stream }
func (q *Queue) Verify(ctx context.Context) error {
	if err := q.core.verifyCollections(ctx, q.records, q.quota); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, q.core.timeout)
	defer cancel()
	var quota struct {
		Records int64 `bson:"records"`
		Bytes   int64 `bson:"bytes"`
	}
	err := q.quota.FindOne(ctx, bson.D{{Key: "_id", Value: "active"}}).Decode(&quota)
	if err != nil || quota.Records < 0 || quota.Records > queueRecords || quota.Bytes < 0 || quota.Bytes > queueBytes {
		q.core.verified.Store(false)
		return errors.New("invalid recovery queue quota")
	}
	return nil
}
func (q *Queue) Prepare(ctx context.Context) error {
	q.core.verified.Store(false)
	ctx, cancel := context.WithTimeout(ctx, q.core.timeout)
	defer cancel()
	for _, collection := range []*mongo.Collection{q.records, q.quota} {
		err := q.core.db.CreateCollection(ctx, collection.Name())
		var command mongo.CommandError
		if err != nil && !(errors.As(err, &command) && command.Code == 48) {
			return err
		}
	}
	if _, err := q.quota.UpdateOne(ctx, bson.D{{Key: "_id", Value: "active"}}, bson.D{{Key: "$setOnInsert", Value: bson.D{{Key: "records", Value: int64(0)}, {Key: "bytes", Value: int64(0)}}}}, options.UpdateOne().SetUpsert(true)); err != nil {
		return err
	}
	if _, err := q.records.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "state", Value: 1}, {Key: "next_attempt_at", Value: 1}, {Key: "_id", Value: 1}}}); err != nil {
		return err
	}
	return q.Verify(ctx)
}
func (q *Queue) ProcessRecord(ctx context.Context, m kafka.Message) (workflow.Outcome, error) {
	return q.retain(ctx, m, "")
}
func (q *Queue) EnqueueSnapshot(ctx context.Context, artifact string, m kafka.Message) (workflow.Outcome, error) {
	raw, err := hex.DecodeString(artifact)
	if err != nil || len(raw) != 32 {
		return "", errors.New("snapshot artifact requires a SHA-256 digest")
	}
	return q.retain(ctx, m, "snapshot:"+artifact+":")
}
func (q *Queue) retain(ctx context.Context, m kafka.Message, provenance string) (workflow.Outcome, error) {
	if !q.core.verified.Load() {
		return "", ErrUnverified
	}
	topic := "order.events.v1"
	if q.stream == "journal" {
		topic = "ledger.journals.v1"
	}
	if m.Topic != topic || m.Partition < 0 || m.Offset < 0 || len(m.Key) > 1024 || len(m.Value) > 2*lifecycle.MaxEventBytes {
		return "", errors.New("invalid recovery source coordinates or size")
	}
	id := provenance + fmt.Sprintf("%s:%d:%d", m.Topic, m.Partition, m.Offset)
	state, reason := "pending", ""
	e, decodeErr := lifecycle.Decode(m.Value)
	if decodeErr != nil {
		state, reason = "quarantine", "invalid"
	} else {
		key, err := lifecycle.Key(e)
		if err != nil || key != string(m.Key) || (q.stream == "workflow" && e.GetOrderWorkflow() == nil) || (q.stream == "journal" && e.GetLedgerJournal() == nil) {
			state, reason = "quarantine", "invalid"
		}
	}
	outcome := workflow.Applied
	err := q.core.withTransaction(ctx, func(ctx context.Context) error {
		outcome = workflow.Applied
		var prior QueueRecord
		err := q.records.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&prior)
		if err == nil {
			if prior.Topic != m.Topic || prior.Partition != m.Partition || prior.Offset != m.Offset || !bytes.Equal(prior.Key, m.Key) || !bytes.Equal(prior.Payload, m.Value) {
				return errors.New("recovery source coordinate reused")
			}
			outcome = workflow.Duplicate
			return nil
		}
		if !errors.Is(err, mongo.ErrNoDocuments) {
			return err
		}
		size := int64(len(m.Key) + len(m.Value))
		result, err := q.quota.UpdateOne(ctx, bson.D{{Key: "_id", Value: "active"}, {Key: "records", Value: bson.D{{Key: "$gte", Value: 0}, {Key: "$lt", Value: queueRecords}}}, {Key: "bytes", Value: bson.D{{Key: "$gte", Value: 0}, {Key: "$lte", Value: queueBytes - size}}}}, bson.D{{Key: "$inc", Value: bson.D{{Key: "records", Value: 1}, {Key: "bytes", Value: size}}}})
		if err != nil {
			return err
		}
		if result.MatchedCount != 1 {
			return ErrQueueFull
		}
		_, err = q.records.InsertOne(ctx, QueueRecord{ID: id, PayloadDigest: payloadHash(m.Value), Provenance: provenance, Topic: m.Topic, Partition: m.Partition, Offset: m.Offset, Key: bytes.Clone(m.Key), Payload: bytes.Clone(m.Value), State: state, Reason: reason, Next: time.Now().UTC()})
		return err
	})
	if err != nil {
		return "", err
	}
	return outcome, nil
}

// Batch returns at most 32 due records; it never loads the entire generation.
func (q *Queue) Batch(ctx context.Context) ([]QueueRecord, error) {
	if !q.core.verified.Load() {
		return nil, ErrUnverified
	}
	ctx, cancel := context.WithTimeout(ctx, q.core.timeout)
	defer cancel()
	cursor, err := q.records.Find(ctx, bson.D{{Key: "state", Value: "pending"}, {Key: "next_attempt_at", Value: bson.D{{Key: "$lte", Value: time.Now().UTC()}}}}, options.Find().SetSort(bson.D{{Key: "next_attempt_at", Value: 1}, {Key: "_id", Value: 1}}).SetLimit(32))
	if err != nil {
		return nil, err
	}
	var records []QueueRecord
	err = cursor.All(ctx, &records)
	return records, err
}

// Finish is idempotent across appliers. Effects commit through the domain
// processor first; a crash before Finish simply replays its inbox receipt.
func (q *Queue) Finish(ctx context.Context, record QueueRecord, processErr error) error {
	if !q.core.verified.Load() {
		return ErrUnverified
	}
	return q.core.withTransaction(ctx, func(ctx context.Context) error {
		var prior QueueRecord
		if err := q.records.FindOne(ctx, bson.D{{Key: "_id", Value: record.ID}}).Decode(&prior); err != nil {
			return err
		}
		if !bytes.Equal(prior.Payload, record.Payload) || !bytes.Equal(prior.Key, record.Key) {
			return errors.New("retained recovery bytes changed")
		}
		if prior.State != "pending" {
			return nil
		}
		state, reason := "done", ""
		if processErr != nil {
			state, reason = "pending", "dependency"
			switch {
			case errors.Is(processErr, journal.ErrGap) || errors.Is(processErr, workflow.ErrGap):
				reason = "gap"
			case errors.Is(processErr, journal.ErrInvalid) || errors.Is(processErr, workflow.ErrInvalid):
				state, reason = "quarantine", "invalid"
			case errors.Is(processErr, journal.ErrStale) || errors.Is(processErr, workflow.ErrStale):
				state, reason = "quarantine", "stale"
			case errors.Is(processErr, journal.ErrConflict) || errors.Is(processErr, workflow.ErrConflict):
				state, reason = "quarantine", "conflict"
			}
		}
		// Bound the persisted delay even for long-lived unresolved gaps.
		attempt := prior.Attempts
		if attempt > 6 {
			attempt = 6
		}
		delay := time.Duration(1<<uint(attempt)) * time.Second
		if delay > 60*time.Second {
			delay = 60 * time.Second
		}
		_, err := q.records.UpdateOne(ctx, bson.D{{Key: "_id", Value: prior.ID}}, bson.D{{Key: "$set", Value: bson.D{{Key: "state", Value: state}, {Key: "reason", Value: reason}, {Key: "next_attempt_at", Value: time.Now().UTC().Add(delay)}}}, {Key: "$inc", Value: bson.D{{Key: "attempts", Value: 1}}}})
		if err != nil {
			return err
		}
		if state == "done" {
			result, updateErr := q.quota.UpdateOne(ctx, bson.D{{Key: "_id", Value: "active"}, {Key: "records", Value: bson.D{{Key: "$gte", Value: 1}}}, {Key: "bytes", Value: bson.D{{Key: "$gte", Value: int64(len(prior.Key) + len(prior.Payload))}}}}, bson.D{{Key: "$inc", Value: bson.D{{Key: "records", Value: -1}, {Key: "bytes", Value: -int64(len(prior.Key) + len(prior.Payload))}}}})
			err = updateErr
			if err == nil && result.MatchedCount != 1 {
				return errors.New("recovery quota is inconsistent")
			}
		}
		return err
	})
}
func (q *Queue) Inspect(ctx context.Context, id string) (QueueRecord, error) {
	if !q.core.verified.Load() {
		return QueueRecord{}, ErrUnverified
	}
	ctx, cancel := context.WithTimeout(ctx, q.core.timeout)
	defer cancel()
	var record QueueRecord
	err := q.records.FindOne(ctx, bson.D{{Key: "_id", Value: id}, {Key: "state", Value: "quarantine"}}).Decode(&record)
	return record, err
}
func (q *Queue) Retry(ctx context.Context, id, expectedDigest, reason string) error {
	if !q.core.verified.Load() {
		return ErrUnverified
	}
	if len(expectedDigest) != 64 || strings.TrimSpace(reason) == "" || len(reason) > 256 || strings.ContainsAny(reason, "\x00\r\n") {
		return errors.New("retry requires the inspected digest and a bounded operator reason")
	}
	return q.core.withTransaction(ctx, func(ctx context.Context) error {
		var record QueueRecord
		if err := q.records.FindOne(ctx, bson.D{{Key: "_id", Value: id}, {Key: "state", Value: "quarantine"}}).Decode(&record); err != nil {
			return err
		}
		if record.Digest() != expectedDigest {
			return errors.New("quarantined source digest changed")
		}
		_, err := q.records.UpdateOne(ctx, bson.D{{Key: "_id", Value: id}}, bson.D{{Key: "$set", Value: bson.D{{Key: "state", Value: "pending"}, {Key: "reason", Value: "operator_retry"}, {Key: "operator_reason", Value: reason}, {Key: "operator_retried_at", Value: time.Now().UTC()}, {Key: "next_attempt_at", Value: time.Now().UTC()}}}, {Key: "$inc", Value: bson.D{{Key: "operator_retries", Value: 1}}}})
		return err
	})
}

// Digest is an audit handle; it never replaces retained source bytes.
func (r QueueRecord) Digest() string {
	sum := sha256.Sum256(r.Payload)
	return hex.EncodeToString(sum[:])
}

type QueueSummary struct {
	Active      int64 `json:"active"`
	Bytes       int64 `json:"bytes"`
	Quarantined int64 `json:"quarantined"`
}

func (q *Queue) Summary(ctx context.Context) (QueueSummary, error) {
	if !q.core.verified.Load() {
		return QueueSummary{}, ErrUnverified
	}
	ctx, cancel := context.WithTimeout(ctx, q.core.timeout)
	defer cancel()
	var quota struct {
		Records int64 `bson:"records"`
		Bytes   int64 `bson:"bytes"`
	}
	if err := q.quota.FindOne(ctx, bson.D{{Key: "_id", Value: "active"}}).Decode(&quota); err != nil {
		return QueueSummary{}, err
	}
	count, err := q.records.CountDocuments(ctx, bson.D{{Key: "state", Value: "quarantine"}})
	return QueueSummary{Active: quota.Records, Bytes: quota.Bytes, Quarantined: count}, err
}

func (q *Queue) Prometheus(ctx context.Context) string {
	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	summary, err := q.Summary(ctx)
	if err != nil {
		return "projection_queue_observation_up 0\n"
	}
	return fmt.Sprintf("projection_queue_observation_up 1\nprojection_queue_active_records %d\nprojection_queue_active_bytes %d\nprojection_queue_quarantined_records %d\n", summary.Active, summary.Bytes, summary.Quarantined)
}

type QueueMetadata struct {
	ID        string `json:"id"`
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Reason    string `json:"reason"`
	Attempts  int64  `json:"attempts"`
	Digest    string `json:"digest,omitempty"`
}

func (q *Queue) QuarantinePage(ctx context.Context, after string) ([]QueueMetadata, string, error) {
	if !q.core.verified.Load() {
		return nil, "", ErrUnverified
	}
	if len(after) > 512 {
		return nil, "", errors.New("invalid inspection cursor")
	}
	ctx, cancel := context.WithTimeout(ctx, q.core.timeout)
	defer cancel()
	filter := bson.D{{Key: "state", Value: "quarantine"}}
	if after != "" {
		filter = append(filter, bson.E{Key: "_id", Value: bson.D{{Key: "$gt", Value: after}}})
	}
	rows, err := q.records.Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetLimit(51).SetProjection(bson.D{{Key: "payload", Value: 0}, {Key: "key", Value: 0}}))
	if err != nil {
		return nil, "", err
	}
	defer rows.Close(ctx)
	items := []QueueMetadata{}
	next := ""
	for rows.Next(ctx) {
		if len(items) == 50 {
			next = items[len(items)-1].ID
			break
		}
		var record QueueRecord
		if err := rows.Decode(&record); err != nil {
			return nil, "", err
		}
		items = append(items, QueueMetadata{ID: record.ID, Topic: record.Topic, Partition: record.Partition, Offset: record.Offset, Reason: record.Reason, Attempts: record.Attempts, Digest: record.PayloadDigest})
	}
	return items, next, rows.Err()
}
