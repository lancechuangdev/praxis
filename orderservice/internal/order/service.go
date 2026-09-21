package order

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math/big"
	"strings"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

var ErrRiskRejected = errors.New("risk rejected order")
var tracer = otel.Tracer("praxis/order-service")

type Ledger interface {
	Reserve(context.Context, Request) (Reservation, error)
	Close() error
}

type MatchingEngine interface {
	Submit(context.Context, Request, Reservation) (MatchResult, error)
	Close() error
}

type Metrics struct {
	Requests, Accepted, RiskRejected, Failed atomic.Uint64
	RiskNS, ReserveNS, MatchingNS, TotalNS   atomic.Uint64
	RiskDuration, ReserveDuration            DurationHistogram
	MatchingDuration, TotalDuration          DurationHistogram
}

type Service struct {
	Ledger        Ledger
	Matching      MatchingEngine
	RiskLatency   time.Duration
	RiskRejectBPS int
	Metrics       *Metrics
}

func Validate(v Request) error {
	if v.RequestID == "" || v.OrderID == "" || v.UserID == "" || v.Symbol == "" || v.ReserveAssetID == "" {
		return errors.New("request_id, order_id, user_id, symbol, and reserve_asset_id are required")
	}
	if v.Side != "buy" && v.Side != "sell" {
		return errors.New("side must be buy or sell")
	}
	if v.OrderType != "limit" && v.OrderType != "market" {
		return errors.New("order_type must be limit or market")
	}
	for name, raw := range map[string]string{"quantity": v.Quantity, "reserve_amount_atomic": v.ReserveAmountAtomic} {
		n, ok := new(big.Int).SetString(raw, 10)
		if !ok || n.Sign() <= 0 {
			return fmt.Errorf("%s must be a positive base-10 integer", name)
		}
	}
	if v.OrderType == "limit" && strings.TrimSpace(v.Price) == "" {
		return errors.New("price is required for a limit order")
	}
	if v.EnginePartition < 0 {
		return errors.New("engine_partition cannot be negative")
	}
	return nil
}

func (s *Service) Admit(ctx context.Context, req Request) (Response, error) {
	started := time.Now()
	s.Metrics.Requests.Add(1)
	if err := Validate(req); err != nil {
		s.failed(started)
		return Response{}, err
	}

	riskStart := time.Now()
	riskCtx, riskSpan := tracer.Start(ctx, "risk.check")
	if err := wait(riskCtx, s.RiskLatency); err != nil {
		riskSpan.RecordError(err)
		riskSpan.SetStatus(codes.Error, err.Error())
		riskSpan.End()
		s.failed(started)
		return Response{}, err
	}
	riskDuration := time.Since(riskStart)
	s.Metrics.RiskNS.Add(uint64(riskDuration))
	s.Metrics.RiskDuration.Observe(riskDuration)
	if rejected(req.RequestID, s.RiskRejectBPS) {
		riskSpan.SetAttributes(attribute.Bool("risk.rejected", true))
		riskSpan.End()
		s.Metrics.RiskRejected.Add(1)
		total := time.Since(started)
		s.Metrics.TotalNS.Add(uint64(total))
		s.Metrics.TotalDuration.Observe(total)
		return Response{}, ErrRiskRejected
	}
	riskSpan.SetAttributes(attribute.Bool("risk.rejected", false))
	riskSpan.End()

	reserveStart := time.Now()
	reservation, err := s.Ledger.Reserve(ctx, req)
	reserveDuration := time.Since(reserveStart)
	s.Metrics.ReserveNS.Add(uint64(reserveDuration))
	s.Metrics.ReserveDuration.Observe(reserveDuration)
	if err != nil {
		s.failed(started)
		return Response{}, fmt.Errorf("reserve funds: %w", err)
	}

	matchingStart := time.Now()
	match, err := s.Matching.Submit(ctx, req, reservation)
	matchingDuration := time.Since(matchingStart)
	s.Metrics.MatchingNS.Add(uint64(matchingDuration))
	s.Metrics.MatchingDuration.Observe(matchingDuration)
	if err != nil {
		s.failed(started)
		return Response{}, fmt.Errorf("matching admission: %w", err)
	}

	total := time.Since(started)
	s.Metrics.Accepted.Add(1)
	s.Metrics.TotalNS.Add(uint64(total))
	s.Metrics.TotalDuration.Observe(total)
	return Response{OrderID: req.OrderID, Status: match.Status, Reservation: reservation, EngineSequence: match.EngineSequence, Timings: Timings{RiskMS: milliseconds(riskDuration), ReserveMS: milliseconds(reserveDuration), MatchingMS: milliseconds(matchingDuration), TotalMS: milliseconds(total)}}, nil
}

func (s *Service) failed(started time.Time) {
	s.Metrics.Failed.Add(1)
	total := time.Since(started)
	s.Metrics.TotalNS.Add(uint64(total))
	s.Metrics.TotalDuration.Observe(total)
}
func milliseconds(v time.Duration) float64 { return float64(v) / float64(time.Millisecond) }
func rejected(key string, bps int) bool {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32()%10000) < bps
}
func wait(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
