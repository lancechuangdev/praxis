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
	"praxis/ledgerservice/internal/kafkaauth"
	"praxis/ledgerservice/internal/ledger"
)

type envelope struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	AggregateID string          `json:"aggregate_id"`
	OccurredAt  time.Time       `json:"occurred_at"`
	Data        json.RawMessage `json:"data"`
}
type tradeData struct {
	TradeID             string `json:"trade_id"`
	EngineID            string `json:"engine_id"`
	EnginePartition     int32  `json:"engine_partition"`
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
	OrderID         string `json:"order_id"`
	EngineID        string `json:"engine_id"`
	EnginePartition int32  `json:"engine_partition"`
	SequenceNumber  int64  `json:"sequence_number"`
	CorrelationID   string `json:"correlation_id"`
	CausationID     string `json:"causation_id"`
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
	Reader *kafka.Reader
	DB     *pgxpool.Pool
	Store  ledger.Store
	Name   string
	Log    *slog.Logger
}

func NewConsumer(brokers []string, topic, group string, auth kafkaauth.Config, db *pgxpool.Pool, s ledger.Store, log *slog.Logger) *Consumer {
	return &Consumer{Reader: kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: topic, GroupID: group, CommitInterval: 0, MinBytes: 1, MaxBytes: 10e6, Dialer: auth.Dialer()}), DB: db, Store: s, Name: group, Log: log}
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
			return err
		}
		if err = c.Reader.CommitMessages(ctx, m); err != nil {
			return err
		}
	}
}
func (c *Consumer) handle(ctx context.Context, raw []byte) error {
	var e envelope
	if err := json.Unmarshal(raw, &e); err != nil {
		return fmt.Errorf("decode envelope: %w", err)
	}
	if e.ID == "" || e.Type == "" {
		return errors.New("event id and type are required")
	}
	var seen bool
	if err := c.DB.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM inbox_events WHERE consumer_name=$1 AND event_id=$2)`, c.Name, e.ID).Scan(&seen); err != nil {
		return err
	}
	if seen {
		return nil
	}
	var err error
	switch e.Type {
	case "PostDeposit":
		var d depositData
		err = json.Unmarshal(e.Data, &d)
		if err == nil {
			_, err = c.Store.PostDeposit(ctx, ledger.PostDeposit{CommandID: e.ID, DepositID: d.DepositID, UserID: d.UserID, AssetID: d.AssetID, CustodyPositionID: d.CustodyPositionID, AmountAtomic: d.AmountAtomic, TargetBucket: d.TargetBucket, SourceSystem: d.SourceSystem, CorrelationID: d.CorrelationID, CausationID: d.CausationID, OccurredAt: e.OccurredAt})
		}
	case "TradeExecuted":
		var d tradeData
		err = json.Unmarshal(e.Data, &d)
		if err == nil {
			_, err = c.Store.BookTrade(ctx, ledger.TradeExecuted{EventID: e.ID, TradeID: d.TradeID, EngineID: d.EngineID, EnginePartition: d.EnginePartition, SequenceNumber: d.SequenceNumber, BuyerOrderID: d.BuyerOrderID, SellerOrderID: d.SellerOrderID, BuyerUserID: d.BuyerUserID, SellerUserID: d.SellerUserID, BaseAssetID: d.BaseAssetID, QuoteAssetID: d.QuoteAssetID, BaseAmountAtomic: d.BaseAmountAtomic, QuoteAmountAtomic: d.QuoteAmountAtomic, BuyerFeeQuoteAtomic: d.BuyerFeeQuoteAtomic, SellerFeeBaseAtomic: d.SellerFeeBaseAtomic, CorrelationID: d.CorrelationID, CausationID: d.CausationID, OccurredAt: e.OccurredAt})
		}
	case "OrderCancelled":
		var d cancelData
		err = json.Unmarshal(e.Data, &d)
		if err == nil {
			_, err = c.Store.CancelOrder(ctx, ledger.CancelOrder{EventID: e.ID, OrderID: d.OrderID, EngineID: d.EngineID, EnginePartition: d.EnginePartition, SequenceNumber: d.SequenceNumber, CorrelationID: d.CorrelationID, CausationID: d.CausationID, OccurredAt: e.OccurredAt})
		}
	default:
		return fmt.Errorf("unsupported event type %q", e.Type)
	}
	if err != nil {
		return err
	}
	_, err = c.DB.Exec(ctx, `INSERT INTO inbox_events(consumer_name,event_id,event_type) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, c.Name, e.ID, e.Type)
	return err
}
