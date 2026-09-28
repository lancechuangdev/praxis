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
	grpcCodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ErrRiskRejected = errors.New("risk rejected order")
var tracer = otel.Tracer("praxis/order-service")

type Ledger interface {
	Reserve(context.Context, Request) (Reservation, error)
	Release(context.Context, ReleaseRequest) (Reservation, error)
	Close() error
}

type SagaStore interface {
	Start(context.Context, Request) error
	Reserved(context.Context, string, string) error
	Accepted(context.Context, string) error
	ReservationFailed(context.Context, string, error, bool) (bool, error)
	RequestRelease(context.Context, string, string) (Saga, error)
	Released(context.Context, string) error
	ReleaseFailed(context.Context, string, error) (bool, error)
	DueReleases(context.Context, int) ([]Saga, error)
	DueReservations(context.Context, int) ([]Saga, error)
}

type MatchingEngine interface {
	Submit(context.Context, Request, Reservation) (MatchResult, error)
	Close() error
}

type Metrics struct {
	Requests, Accepted, RiskRejected, Failed, DeadLettered atomic.Uint64
	RiskNS, ReserveNS, MatchingNS, TotalNS                 atomic.Uint64
	RiskDuration, ReserveDuration                          DurationHistogram
	MatchingDuration, TotalDuration                        DurationHistogram
}

type Service struct {
	Ledger        Ledger
	Matching      MatchingEngine
	RiskLatency   time.Duration
	RiskRejectBPS int
	Metrics       *Metrics
	Sagas         SagaStore
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
	if s.Sagas != nil {
		if err := s.Sagas.Start(ctx, req); err != nil {
			s.failed(started)
			return Response{}, fmt.Errorf("persist order saga: %w", err)
		}
	}

	reserveStart := time.Now()
	reservation, err := s.Ledger.Reserve(ctx, req)
	reserveDuration := time.Since(reserveStart)
	s.Metrics.ReserveNS.Add(uint64(reserveDuration))
	s.Metrics.ReserveDuration.Observe(reserveDuration)
	if err != nil {
		if s.Sagas != nil {
			deadLettered, deadLetterErr := s.Sagas.ReservationFailed(context.Background(), req.RequestID, err, retryableReservationError(err))
			if deadLetterErr != nil {
				err = errors.Join(err, fmt.Errorf("persist reservation failure: %w", deadLetterErr))
			} else if deadLettered {
				s.Metrics.DeadLettered.Add(1)
			}
		}
		s.failed(started)
		return Response{}, fmt.Errorf("reserve funds: %w", err)
	}
	if s.Sagas != nil {
		if err := s.Sagas.Reserved(ctx, req.RequestID, reservation.ID); err != nil {
			s.failed(started)
			return Response{}, fmt.Errorf("persist reservation step: %w", err)
		}
	}

	matchingStart := time.Now()
	match, err := s.Matching.Submit(ctx, req, reservation)
	matchingDuration := time.Since(matchingStart)
	s.Metrics.MatchingNS.Add(uint64(matchingDuration))
	s.Metrics.MatchingDuration.Observe(matchingDuration)
	if err != nil {
		if s.Sagas != nil {
			saga, persistErr := s.Sagas.RequestRelease(context.Background(), req.OrderID, "matching_failed")
			if persistErr == nil {
				_ = s.release(context.Background(), saga)
			}
		}
		s.failed(started)
		return Response{}, fmt.Errorf("matching admission: %w", err)
	}
	if s.Sagas != nil {
		if err := s.Sagas.Accepted(ctx, req.RequestID); err != nil {
			s.failed(started)
			return Response{}, fmt.Errorf("persist matching step: %w", err)
		}
	}

	total := time.Since(started)
	s.Metrics.Accepted.Add(1)
	s.Metrics.TotalNS.Add(uint64(total))
	s.Metrics.TotalDuration.Observe(total)
	return Response{OrderID: req.OrderID, Status: match.Status, Reservation: reservation, EngineSequence: match.EngineSequence, Timings: Timings{RiskMS: milliseconds(riskDuration), ReserveMS: milliseconds(reserveDuration), MatchingMS: milliseconds(matchingDuration), TotalMS: milliseconds(total)}}, nil
}

// CompleteExecution is called after all TradeExecuted events for the order have
// been booked by Ledger. Release is safe because Ledger returns only the unused remainder.
func (s *Service) CompleteExecution(ctx context.Context, orderID string) error {
	if s.Sagas == nil {
		return errors.New("persistent saga store is not configured")
	}
	saga, err := s.Sagas.RequestRelease(ctx, orderID, "execution_complete")
	if err != nil {
		return err
	}
	return s.release(ctx, saga)
}

func (s *Service) release(ctx context.Context, saga Saga) error {
	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := s.Ledger.Release(callCtx, ReleaseRequest{CommandID: saga.Request.RequestID + ":release:" + saga.ReleaseReason, OrderID: saga.Request.OrderID, Reason: saga.ReleaseReason, CorrelationID: saga.Request.CorrelationID, CausationID: saga.Request.CausationID})
	if err != nil {
		deadLettered, persistErr := s.Sagas.ReleaseFailed(context.Background(), saga.Request.RequestID, err)
		if deadLettered {
			s.Metrics.DeadLettered.Add(1)
		}
		if persistErr != nil {
			return errors.Join(err, fmt.Errorf("persist release failure: %w", persistErr))
		}
		return err
	}
	return s.Sagas.Released(context.Background(), saga.Request.RequestID)
}

func (s *Service) RunSagaRecovery(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			reservations, reservationErr := s.Sagas.DueReservations(ctx, 100)
			if reservationErr == nil {
				for _, saga := range reservations {
					_ = s.resumeReservation(ctx, saga)
				}
			}
			sagas, err := s.Sagas.DueReleases(ctx, 100)
			if err != nil {
				continue
			}
			for _, saga := range sagas {
				_ = s.release(ctx, saga)
			}
		}
	}
}

func (s *Service) resumeReservation(ctx context.Context, saga Saga) error {
	reservation, err := s.Ledger.Reserve(ctx, saga.Request)
	if err != nil {
		deadLettered, persistErr := s.Sagas.ReservationFailed(context.Background(), saga.Request.RequestID, err, retryableReservationError(err))
		if deadLettered {
			s.Metrics.DeadLettered.Add(1)
		}
		if persistErr != nil {
			return errors.Join(err, persistErr)
		}
		return err
	}
	if err = s.Sagas.Reserved(ctx, saga.Request.RequestID, reservation.ID); err != nil {
		return err
	}
	_, err = s.Matching.Submit(ctx, saga.Request, reservation)
	if err != nil {
		pending, persistErr := s.Sagas.RequestRelease(context.Background(), saga.Request.OrderID, "matching_failed")
		if persistErr != nil {
			return errors.Join(err, persistErr)
		}
		_ = s.release(context.Background(), pending)
		return err
	}
	return s.Sagas.Accepted(ctx, saga.Request.RequestID)
}

func retryableReservationError(err error) bool {
	switch status.Code(err) {
	case grpcCodes.InvalidArgument, grpcCodes.FailedPrecondition, grpcCodes.AlreadyExists, grpcCodes.NotFound, grpcCodes.PermissionDenied, grpcCodes.Unauthenticated:
		return false
	default:
		return true
	}
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
