package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"praxis/projectionservice/internal/config"
	"praxis/projectionservice/internal/mongostore"
	"praxis/projectionservice/internal/runtime"
)

type command struct {
	Mode, Input, ID, Report, Digest, Reason, Cursor string
	Revision                                        int64
}

func parseCommand(args []string) (command, error) {
	c := command{Mode: "serve"}
	if len(args) == 0 {
		return c, nil
	}
	c.Mode = args[0]
	switch c.Mode {
	case "prepare", "drain", "replay", "reconcile", "activate", "retry", "inspect":
	default:
		return c, errors.New("usage: projection-service [prepare|drain|replay|reconcile|activate|inspect|retry]")
	}
	flags := flag.NewFlagSet(c.Mode, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&c.Cursor, "cursor", "", "quarantine metadata cursor")
	flags.StringVar(&c.Input, "input", "", "JSONL source export")
	flags.StringVar(&c.ID, "id", "", "quarantine source-coordinate ID")
	flags.StringVar(&c.Digest, "expected-digest", "", "inspected source digest")
	flags.StringVar(&c.Reason, "reason", "", "operator repair reference")
	flags.StringVar(&c.Report, "report", "", "verified reconciliation report ID")
	flags.Int64Var(&c.Revision, "expected-revision", 0, "current read-route revision, zero for first activation")
	if err := flags.Parse(args[1:]); err != nil || flags.NArg() != 0 {
		return c, errors.New("invalid projection command arguments")
	}
	if (c.Mode == "replay" || c.Mode == "reconcile") && c.Input == "" {
		return c, errors.New("command requires --input")
	}
	if c.Mode == "retry" && c.ID == "" {
		return c, errors.New("retry requires --id")
	}
	if c.Mode == "activate" && (c.Report == "" || c.Revision < 0) {
		return c, errors.New("activate requires --report and nonnegative --expected-revision")
	}
	if c.Mode == "retry" && (len(c.Digest) != 64 || strings.TrimSpace(c.Reason) == "" || len(c.Reason) > 256) {
		return c, errors.New("retry requires --expected-digest and --reason")
	}
	return c, nil
}
func drain(ctx context.Context, q *runtime.Queued) error {
	for ctx.Err() == nil {
		summary, err := q.Queue.Summary(ctx)
		if err != nil {
			return err
		}
		if summary.Quarantined > 0 {
			return errors.New("quarantined records require operator repair before drain completes")
		}
		if summary.Active == 0 {
			return nil
		}
		records, err := q.Queue.Batch(ctx)
		if err != nil {
			return err
		}
		for _, r := range records {
			applyErr := q.Apply(ctx, r)
			if err := q.Queue.Finish(ctx, r, applyErr); err != nil {
				return err
			}
		}
		if len(records) == 0 {
			timer := time.NewTimer(100 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
	return ctx.Err()
}
func runAdmin(ctx context.Context, c command, cfg config.Config, client *mongo.Client, worker *runtime.Queued) error {
	ops, err := mongostore.NewOperations(client, cfg.Database, cfg.Target, cfg.Stream, cfg.TransactionTimeout)
	if err != nil {
		return err
	}
	switch c.Mode {
	case "drain":
		budget, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		return drain(budget, worker)
	case "retry":
		return worker.Queue.Retry(ctx, c.ID, c.Digest, c.Reason)
	case "inspect":
		if c.ID == "" {
			items, next, err := worker.Queue.QuarantinePage(ctx, c.Cursor)
			if err != nil {
				return err
			}
			return json.NewEncoder(os.Stdout).Encode(map[string]any{"items": items, "next_cursor": next})
		}
		record, err := worker.Queue.Inspect(ctx, c.ID)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(struct {
			Record mongostore.QueueRecord `json:"record"`
			Digest string                 `json:"digest"`
		}{record, record.Digest()})
	case "reconcile":
		file, err := os.Open(c.Input)
		if err != nil {
			return errors.New("source export cannot be opened")
		}
		defer file.Close()
		report, err := ops.Reconcile(ctx, file)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(report)
	case "activate":
		route, err := ops.Activate(ctx, c.Report, c.Revision)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(route)
	case "replay":
		if cfg.Target.Mode != "rebuild" {
			return errors.New("replay requires a separate rebuild generation")
		}
		file, err := os.Open(c.Input)
		if err != nil {
			return errors.New("source export cannot be opened")
		}
		defer file.Close()
		hash := sha256.New()
		size, err := io.Copy(hash, io.LimitReader(file, 128*1024*1024+1))
		if err != nil || size > 128*1024*1024 {
			return errors.New("invalid or oversized snapshot artifact")
		}
		artifact := hex.EncodeToString(hash.Sum(nil))
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return err
		}
		type bounds struct {
			Topic     string `bson:"topic" json:"topic"`
			Partition int    `bson:"partition" json:"partition"`
			Start     int64  `bson:"start" json:"start"`
			End       int64  `bson:"end" json:"end"`
		}
		positions := map[string]*bounds{}
		digest, err := mongostore.ScanExport(file, func(r mongostore.ExportRecord) error {
			id := fmt.Sprintf("%s:%d", r.Topic, r.Partition)
			p := positions[id]
			if p == nil {
				p = &bounds{Topic: r.Topic, Partition: r.Partition, Start: r.Offset, End: r.Offset}
				positions[id] = p
			}
			if r.Partition < 0 || r.Offset < 0 || r.Offset == math.MaxInt64 || r.Offset != p.End {
				return errors.New("replay source partition offsets are not contiguous")
			}
			p.End = r.Offset + 1
			if _, err := worker.Queue.EnqueueSnapshot(ctx, artifact, kafka.Message{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Key: r.Key, Value: r.Payload}); err != nil {
				return err
			}
			// Apply a bounded batch as input advances; unresolved gaps remain retained.
			records, err := worker.Queue.Batch(ctx)
			if err != nil {
				return err
			}
			for _, record := range records {
				if err := worker.Queue.Finish(ctx, record, worker.Apply(ctx, record)); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		if digest != artifact {
			return errors.New("snapshot artifact changed during replay")
		}
		budget, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		if err := drain(budget, worker); err != nil {
			return err
		}
		source := make([]bounds, 0, len(positions))
		for _, b := range positions {
			source = append(source, *b)
		}
		sort.Slice(source, func(i, j int) bool {
			if source[i].Topic == source[j].Topic {
				return source[i].Partition < source[j].Partition
			}
			return source[i].Topic < source[j].Topic
		})
		checkpoint := bson.D{{Key: "_id", Value: cfg.Stream + ":" + string(cfg.Target.Mode) + ":" + cfg.Target.Generation + ":" + digest}, {Key: "stream", Value: cfg.Stream}, {Key: "mode", Value: string(cfg.Target.Mode)}, {Key: "generation", Value: cfg.Target.Generation}, {Key: "artifact_digest", Value: digest}, {Key: "source_kind", Value: "authoritative_snapshot"}, {Key: "sources", Value: source}, {Key: "complete", Value: true}}
		_, err = client.Database(cfg.Database).Collection("projection_replays").InsertOne(ctx, checkpoint)
		if err != nil && !mongo.IsDuplicateKeyError(err) {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"artifact_digest": digest, "sources": source, "complete": true})
	}
	return errors.New("unsupported admin command")
}
