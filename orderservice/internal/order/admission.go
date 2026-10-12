package order

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrRateLimited = errors.New("order rate limit exceeded")
	ErrOverloaded  = errors.New("order service admission queue is full")
)

type RateLimitError struct{ RetryAfter time.Duration }

func (e *RateLimitError) Error() string { return ErrRateLimited.Error() }
func (e *RateLimitError) Unwrap() error { return ErrRateLimited }

type AdmissionDecision struct {
	Allowed     bool
	WouldReject bool
	RetryAfter  time.Duration
}

type AdmissionController interface {
	Check(context.Context, Request) (AdmissionDecision, error)
}

// ConcurrencyGate reserves capacity for in-flight requests and bounds the
// number of callers waiting for that capacity.
type ConcurrencyGate struct {
	slots   chan struct{}
	maxWait int64
	waiting atomic.Int64
}

func NewConcurrencyGate(maxInFlight, maxWaiting int) *ConcurrencyGate {
	if maxInFlight < 1 {
		return nil
	}
	return &ConcurrencyGate{slots: make(chan struct{}, maxInFlight), maxWait: int64(maxWaiting)}
}

func (g *ConcurrencyGate) Acquire(ctx context.Context) (func(), error) {
	if g == nil {
		return func() {}, nil
	}
	select {
	case g.slots <- struct{}{}:
		return func() { <-g.slots }, nil
	default:
	}
	if g.maxWait <= 0 || g.waiting.Add(1) > g.maxWait {
		g.waiting.Add(-1)
		return nil, ErrOverloaded
	}
	defer g.waiting.Add(-1)
	select {
	case g.slots <- struct{}{}:
		return func() { <-g.slots }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type bucket struct {
	tokens float64
	at     time.Time
}

// LocalTokenBuckets is an emergency/observe-only limiter. It must not be used
// as an authoritative distributed quota because each replica has separate state.
type LocalTokenBuckets struct {
	mu                   sync.Mutex
	now                  func() time.Time
	capacity, refill     float64
	ObserveOnly          bool
	buckets              map[string]bucket
	maxBuckets           int
	Allowed, WouldReject atomic.Uint64
}

func NewLocalTokenBuckets(capacity, refillPerSecond float64, observeOnly bool) *LocalTokenBuckets {
	return &LocalTokenBuckets{now: time.Now, capacity: capacity, refill: refillPerSecond, ObserveOnly: observeOnly, buckets: make(map[string]bucket), maxBuckets: 10_000}
}

func (l *LocalTokenBuckets) Check(_ context.Context, req Request) (AdmissionDecision, error) {
	if l == nil || l.capacity <= 0 || l.refill <= 0 {
		return AdmissionDecision{Allowed: true}, nil
	}
	now := l.now()
	l.mu.Lock()
	keys := []string{"global"}
	for _, key := range []string{"account:" + req.UserID, "market:" + req.Symbol} {
		if _, exists := l.buckets[key]; exists || len(l.buckets) < l.maxBuckets {
			keys = append(keys, key)
		}
	}
	allowed, retryAfter := true, time.Duration(0)
	updated := make(map[string]bucket, len(keys))
	for _, key := range keys {
		b, ok := l.buckets[key]
		if !ok {
			b = bucket{tokens: l.capacity, at: now}
		}
		b.tokens += now.Sub(b.at).Seconds() * l.refill
		if b.tokens > l.capacity {
			b.tokens = l.capacity
		}
		b.at = now
		if b.tokens < 1 {
			allowed = false
			wait := time.Duration((1 - b.tokens) / l.refill * float64(time.Second))
			if wait > retryAfter {
				retryAfter = wait
			}
		}
		updated[key] = b
	}
	if allowed {
		for key, b := range updated {
			b.tokens--
			l.buckets[key] = b
		}
	} else {
		for key, b := range updated {
			l.buckets[key] = b
		}
	}
	l.mu.Unlock()
	if !allowed {
		l.WouldReject.Add(1)
		if !l.ObserveOnly {
			return AdmissionDecision{RetryAfter: retryAfter}, nil
		}
	}
	l.Allowed.Add(1)
	return AdmissionDecision{Allowed: true, WouldReject: !allowed, RetryAfter: retryAfter}, nil
}
