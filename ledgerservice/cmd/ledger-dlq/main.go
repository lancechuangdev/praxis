// ledger-dlq is a deliberately narrow operator tool for inspecting and
// replaying Ledger command dead letters. Access is additionally constrained by
// Kafka/database IAM and by an operator allowlist supplied by the runbook.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	eventsv1 "praxis/api/events/v1"
	"praxis/ledgerservice/internal/config"
	"praxis/ledgerservice/internal/dlq"
	"praxis/ledgerservice/migrations"
)

type options struct {
	action, reason, jobID, failureCode, eventID string
	partition                                   int
	offset                                      int64
	max                                         int
	rate                                        time.Duration
	approve                                     bool
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	var o options
	fs := flag.NewFlagSet("ledger-dlq", flag.ContinueOnError)
	fs.StringVar(&o.action, "action", "", "inspect or replay")
	fs.StringVar(&o.reason, "reason", "", "incident/change reference and reason")
	fs.StringVar(&o.jobID, "job-id", "", "unique operation job ID")
	fs.IntVar(&o.partition, "partition", -1, "DLQ partition")
	fs.Int64Var(&o.offset, "offset", -1, "first DLQ offset")
	fs.IntVar(&o.max, "max-records", 1, "maximum records to scan (1-100)")
	fs.DurationVar(&o.rate, "interval", time.Second, "minimum interval between replay publishes")
	fs.StringVar(&o.failureCode, "failure-code", "", "optional exact failure-code filter")
	fs.StringVar(&o.eventID, "event-id", "", "optional exact original event-ID filter")
	fs.BoolVar(&o.approve, "approve-replay", false, "confirm the underlying defect is resolved")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if o.action != "inspect" && o.action != "replay" {
		return errors.New("--action must be inspect or replay")
	}
	if strings.TrimSpace(o.reason) == "" || strings.TrimSpace(o.jobID) == "" {
		return errors.New("--reason and --job-id are required")
	}
	if o.partition < 0 || o.offset < 0 || o.max < 1 || o.max > 100 {
		return errors.New("partition/offset must be non-negative and max-records must be 1..100")
	}
	if o.action == "replay" && (!o.approve || o.rate < 100*time.Millisecond) {
		return errors.New("replay requires --approve-replay and --interval of at least 100ms")
	}
	operator := strings.TrimSpace(os.Getenv("PRAXIS_OPERATOR_ID"))
	if err := dlq.Authorize(operator, strings.Split(os.Getenv("PRAXIS_DLQ_ALLOWED_OPERATORS"), ",")); err != nil {
		return err
	}

	cfg, err := config.Load()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	db, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := migrations.Verify(ctx, db); err != nil {
		return fmt.Errorf("verify schema: %w", err)
	}
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: cfg.KafkaBrokers, Topic: cfg.CommandsDLQTopic, Partition: o.partition, MinBytes: 1, MaxBytes: 10e6, Dialer: cfg.KafkaAuth.Dialer()})
	defer reader.Close()
	if err := reader.SetOffset(o.offset); err != nil {
		return err
	}
	writer := &kafka.Writer{Addr: kafka.TCP(cfg.KafkaBrokers...), Transport: cfg.KafkaAuth.Transport(), RequiredAcks: kafka.RequireAll, Async: false}
	defer writer.Close()
	selection := dlq.Selection{FailureCode: o.failureCode, EventID: o.eventID}
	matched := 0
	for scanned := 0; scanned < o.max; scanned++ {
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			return err
		}
		var envelope eventsv1.DLQEnvelope
		if err = json.Unmarshal(message.Value, &envelope); err != nil {
			return auditFailure(ctx, db, o, operator, message, eventsv1.DLQEnvelope{}, fmt.Errorf("decode DLQ envelope: %w", err))
		}
		if err = envelope.Validate(); err != nil {
			return auditFailure(ctx, db, o, operator, message, envelope, err)
		}
		if envelope.SourceTopic != cfg.CommandsTopic {
			return auditFailure(ctx, db, o, operator, message, envelope, fmt.Errorf("source topic %q is not allowed", envelope.SourceTopic))
		}
		if !selection.Match(envelope) {
			continue
		}
		matched++
		if err = audit(ctx, db, o, operator, message, envelope, "selected", ""); err != nil {
			return err
		}
		if o.action == "inspect" {
			fmt.Printf("%s\n", inspectionJSON(message, envelope))
			if err = audit(ctx, db, o, operator, message, envelope, "succeeded", ""); err != nil {
				return err
			}
			continue
		}
		replayed, err := dlq.ReplayMessage(envelope, message, o.jobID, operator, time.Now())
		if err != nil {
			return auditFailure(ctx, db, o, operator, message, envelope, err)
		}
		if err = writer.WriteMessages(ctx, replayed); err != nil {
			return auditFailure(ctx, db, o, operator, message, envelope, err)
		}
		if err = audit(ctx, db, o, operator, message, envelope, "succeeded", ""); err != nil {
			return err
		}
		fmt.Printf("replayed dlq=%s/%d/%d source=%s event=%s job=%s\n", message.Topic, message.Partition, message.Offset, envelope.SourceTopic, sourceEventID(envelope), o.jobID)
		if scanned+1 < o.max {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(o.rate):
			}
		}
	}
	if matched == 0 {
		return errors.New("no DLQ records matched the selection")
	}
	return nil
}

func inspectionJSON(m kafka.Message, d eventsv1.DLQEnvelope) string {
	v := struct {
		Topic           string    `json:"dlq_topic"`
		Partition       int       `json:"dlq_partition"`
		Offset          int64     `json:"dlq_offset"`
		ID              string    `json:"id"`
		SourceTopic     string    `json:"source_topic"`
		SourcePartition int       `json:"source_partition"`
		SourceOffset    int64     `json:"source_offset"`
		EventID         string    `json:"event_id,omitempty"`
		FailureCode     string    `json:"failure_code"`
		FailedAt        time.Time `json:"failed_at"`
	}{m.Topic, m.Partition, m.Offset, d.ID, d.SourceTopic, d.SourcePartition, d.SourceOffset, sourceEventID(d), d.FailureCode, d.FailedAt}
	raw, _ := json.Marshal(v)
	return string(raw)
}

func sourceEventID(d eventsv1.DLQEnvelope) string {
	e, err := eventsv1.Decode(d.Original)
	if err != nil {
		return ""
	}
	return e.ID
}

func audit(ctx context.Context, db *pgxpool.Pool, o options, operator string, m kafka.Message, d eventsv1.DLQEnvelope, outcome, errorText string) error {
	_, err := db.Exec(ctx, `INSERT INTO dlq_operation_audit(job_id,action,operator_id,reason,dlq_topic,dlq_partition,dlq_offset,source_topic,source_event_id,failure_code,outcome,error_text) VALUES($1,$2,$3,$4,$5,$6,$7,$8,NULLIF($9,''),NULLIF($10,''),$11,NULLIF($12,''))`, o.jobID, o.action, operator, o.reason, m.Topic, m.Partition, m.Offset, d.SourceTopic, sourceEventID(d), d.FailureCode, outcome, errorText)
	return err
}

func auditFailure(ctx context.Context, db *pgxpool.Pool, o options, operator string, m kafka.Message, d eventsv1.DLQEnvelope, cause error) error {
	if err := audit(ctx, db, o, operator, m, d, "failed", cause.Error()); err != nil {
		return fmt.Errorf("operation failed (%v) and audit failed: %w", cause, err)
	}
	return cause
}
