package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	eventsv1 "praxis/api/events/v1"
	"praxis/ledgerservice/internal/kafkaauth"
	"praxis/ledgerservice/internal/ledger"
)

type tradeData struct {
	TradeID             string `json:"trade_id"`
	EngineID            string `json:"engine_id"`
	Symbol              string `json:"symbol"`
	SequenceNumber      int64  `json:"sequence_number"`
	BuyerOrderID        string `json:"buyer_order_id"`
	SellerOrderID       string `json:"seller_order_id"`
	BuyerUserID         string `json:"buyer_user_id"`
	SellerUserID        string `json:"seller_user_id"`
	BaseAssetID         string `json:"base_asset_id"`
	QuoteAssetID        string `json:"quote_asset_id"`
	BaseAmountAtomic    string `json:"base_amount_atomic"`
	QuoteAmountAtomic   string `json:"quote_amount_atomic"`
	BuyerFeeQuoteAtomic string `json:"buyer_fee_quote_atomic"`
	SellerFeeBaseAtomic string `json:"seller_fee_base_atomic"`
	CorrelationID       string `json:"correlation_id"`
	CausationID         string `json:"causation_id"`
}
type cancelData struct {
	OrderID        string `json:"order_id"`
	EngineID       string `json:"engine_id"`
	Symbol         string `json:"symbol"`
	SequenceNumber int64  `json:"sequence_number"`
	CorrelationID  string `json:"correlation_id"`
	CausationID    string `json:"causation_id"`
}
type depositData struct {
	DepositID         string `json:"deposit_id"`
	UserID            string `json:"user_id"`
	AssetID           string `json:"asset_id"`
	CustodyPositionID string `json:"custody_position_id"`
	AmountAtomic      string `json:"amount_atomic"`
	TargetBucket      string `json:"target_bucket"`
	SourceSystem      string `json:"source_system"`
	CorrelationID     string `json:"correlation_id"`
	CausationID       string `json:"causation_id"`
}

type Consumer struct {
	Reader   *kafka.Reader
	DB       *pgxpool.Pool
	Store    ledger.Store
	Name     string
	DLQTopic string
	Log      *slog.Logger
}

func NewConsumer(brokers []string, topic, dlqTopic, group string, auth kafkaauth.Config, db *pgxpool.Pool, s ledger.Store, log *slog.Logger) *Consumer {
	return &Consumer{Reader: kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: topic, GroupID: group, CommitInterval: 0, MinBytes: 1, MaxBytes: 10e6, Dialer: auth.Dialer()}), DB: db, Store: s, Name: group, DLQTopic: dlqTopic, Log: log}
}
func (c *Consumer) Close() error { return c.Reader.Close() }
func (c *Consumer) Run(ctx context.Context) error {
	for {
		m, err := c.Reader.FetchMessage(ctx)
		if err != nil {
			return err
		}
		if err = c.handle(ctx, m.Value); err != nil {
			c.Log.ErrorContext(ctx, "consume ledger command", "error", err, "topic", m.Topic, "partition", m.Partition, "offset", m.Offset)
			var poison deterministicError
			if !errors.As(err, &poison) {
				return err
			}
			if err = c.deadLetter(ctx, m, err); err != nil {
				return err
			}
		}
		if err = c.Reader.CommitMessages(ctx, m); err != nil {
			return err
		}
	}
}

type deterministicError struct{ error }

func (c *Consumer) deadLetter(ctx context.Context, message kafka.Message, cause error) error {
	id := fmt.Sprintf("dlq:%s:%d:%d", message.Topic, message.Partition, message.Offset)
	d := eventsv1.DLQEnvelope{Version: eventsv1.Version, ID: id, SourceTopic: message.Topic, SourcePartition: message.Partition, SourceOffset: message.Offset, SourceKey: string(message.Key), ConsumerGroup: c.Name, FailureClass: "deterministic", FailureReason: cause.Error(), FailedAt: time.Now().UTC(), Original: message.Value}
	if err := d.Validate(); err != nil {
		return err
	}
	payload, err := json.Marshal(d)
	if err != nil {
		return err
	}
	tx, err := c.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, `INSERT INTO inbox_events(consumer_name,event_id,event_type) VALUES($1,$2,'DeadLettered') ON CONFLICT DO NOTHING`, c.Name, id); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO outbox_events(id,topic,event_type,aggregate_id,message_key,payload,occurred_at) VALUES($1,$2,'DeadLettered',$1,$3,$4,now()) ON CONFLICT(id) DO NOTHING`, id, c.DLQTopic, string(message.Key), payload); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (c *Consumer) handle(ctx context.Context, raw []byte) error {
	e, err := eventsv1.Decode(raw)
	if err != nil {
		return deterministicError{err}
	}
	if e.Type != "PostDeposit" && e.Type != "TradeExecuted" && e.Type != "OrderCancelled" {
		return deterministicError{fmt.Errorf("unsupported event type %q", e.Type)}
	}
	var seen bool
	if err := c.DB.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM inbox_events WHERE consumer_name=$1 AND event_id=$2)`, c.Name, e.ID).Scan(&seen); err != nil {
		return err
	}
	if seen {
		return nil
	}
	commandCtx := ledger.WithInboxMetadata(ctx, ledger.InboxMetadata{ConsumerName: c.Name, EventID: e.ID, EventType: e.Type})
	switch e.Type {
	case "PostDeposit":
		var d depositData
		err = json.Unmarshal(e.Data, &d)
		if err != nil {
			return deterministicError{err}
		}
		_, err = c.Store.PostDeposit(commandCtx, ledger.PostDeposit{CommandID: e.ID, DepositID: d.DepositID, UserID: d.UserID, AssetID: d.AssetID, CustodyPositionID: d.CustodyPositionID, AmountAtomic: d.AmountAtomic, TargetBucket: d.TargetBucket, SourceSystem: d.SourceSystem, CorrelationID: d.CorrelationID, CausationID: d.CausationID, OccurredAt: e.OccurredAt})
	case "TradeExecuted":
		var d tradeData
		err = json.Unmarshal(e.Data, &d)
		if err != nil {
			return deterministicError{err}
		}
		_, err = c.Store.BookTrade(commandCtx, ledger.TradeExecuted{EventID: e.ID, TradeID: d.TradeID, EngineID: d.EngineID, Symbol: d.Symbol, SequenceNumber: d.SequenceNumber, BuyerOrderID: d.BuyerOrderID, SellerOrderID: d.SellerOrderID, BuyerUserID: d.BuyerUserID, SellerUserID: d.SellerUserID, BaseAssetID: d.BaseAssetID, QuoteAssetID: d.QuoteAssetID, BaseAmountAtomic: d.BaseAmountAtomic, QuoteAmountAtomic: d.QuoteAmountAtomic, BuyerFeeQuoteAtomic: d.BuyerFeeQuoteAtomic, SellerFeeBaseAtomic: d.SellerFeeBaseAtomic, CorrelationID: d.CorrelationID, CausationID: d.CausationID, OccurredAt: e.OccurredAt})
	case "OrderCancelled":
		var d cancelData
		err = json.Unmarshal(e.Data, &d)
		if err != nil {
			return deterministicError{err}
		}
		_, err = c.Store.CancelOrder(commandCtx, ledger.CancelOrder{EventID: e.ID, OrderID: d.OrderID, EngineID: d.EngineID, Symbol: d.Symbol, SequenceNumber: d.SequenceNumber, CorrelationID: d.CorrelationID, CausationID: d.CausationID, OccurredAt: e.OccurredAt})
	default:
		return deterministicError{fmt.Errorf("unsupported event type %q", e.Type)}
	}
	if err != nil {
		return err
	}
	return nil
}
