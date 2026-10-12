package order_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"praxis/orderservice/internal/order"
	"praxis/orderservice/migrations"
)

func sagaTestStore(t *testing.T) (*order.PostgresSagaStore, *pgxpool.Pool) {
	t.Helper()
	url := os.Getenv("ORDER_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("ORDER_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	db, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err = migrations.Apply(ctx, db); err != nil {
		t.Fatal(err)
	}
	return &order.PostgresSagaStore{DB: db}, db
}

func TestConcurrentWorkersClaimEachSagaOnce(t *testing.T) {
	store, db := sagaTestStore(t)
	ctx := context.Background()
	prefix := fmt.Sprintf("claim-it-%d", time.Now().UnixNano())
	const workers = 4
	const perWorker = 3
	requestIDs := make([]string, 0, workers*perWorker)
	for i := range workers * perWorker {
		requestID := fmt.Sprintf("%s-request-%d", prefix, i)
		requestIDs = append(requestIDs, requestID)
		req := order.Request{RequestID: requestID, OrderID: fmt.Sprintf("%s-order-%d", prefix, i), UserID: "integration-user", Symbol: "BTC-USDT", Side: "buy", OrderType: "market", Quantity: "1", ReserveAssetID: "asset_usdt", ReserveAmountAtomic: "1"}
		if err := store.Start(ctx, req); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		_, _ = db.Exec(context.Background(), `DELETE FROM order_sagas WHERE request_id=ANY($1::text[])`, requestIDs)
	})

	start := make(chan struct{})
	results := make(chan []order.Saga, workers)
	errs := make(chan error, workers)
	var group sync.WaitGroup
	for i := range workers {
		group.Add(1)
		go func(worker int) {
			defer group.Done()
			<-start
			claimed, err := store.DueReservations(ctx, perWorker, fmt.Sprintf("%s-worker-%d", prefix, worker), time.Minute)
			if err != nil {
				errs <- err
				return
			}
			results <- claimed
		}(i)
	}
	close(start)
	group.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	seen := make(map[string]string, workers*perWorker)
	for claimed := range results {
		for _, saga := range claimed {
			if owner, duplicate := seen[saga.Request.RequestID]; duplicate {
				t.Fatalf("saga %s claimed by both %s and %s", saga.Request.RequestID, owner, saga.ClaimOwner)
			}
			seen[saga.Request.RequestID] = saga.ClaimOwner
		}
	}
	if len(seen) != workers*perWorker {
		t.Fatalf("claimed %d sagas, want %d", len(seen), workers*perWorker)
	}
}

func TestExpiredLeaseCanBeReclaimedAndRejectsStaleOwner(t *testing.T) {
	store, db := sagaTestStore(t)
	ctx := context.Background()
	prefix := fmt.Sprintf("lease-it-%d", time.Now().UnixNano())
	req := order.Request{RequestID: prefix + "-request", OrderID: prefix + "-order", UserID: "integration-user", Symbol: "BTC-USDT", Side: "buy", OrderType: "market", Quantity: "1", ReserveAssetID: "asset_usdt", ReserveAmountAtomic: "1"}
	if err := store.Start(ctx, req); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(context.Background(), `DELETE FROM order_sagas WHERE request_id=$1`, req.RequestID)
	})

	first, err := store.DueReservations(ctx, 1, "worker-a", time.Minute)
	if err != nil || len(first) != 1 {
		t.Fatalf("first claim=%d error=%v", len(first), err)
	}
	if _, err = db.Exec(ctx, `UPDATE order_sagas SET claim_expires_at=now()-interval '1 second' WHERE request_id=$1`, req.RequestID); err != nil {
		t.Fatal(err)
	}
	second, err := store.DueReservations(ctx, 1, "worker-b", time.Minute)
	if err != nil || len(second) != 1 || !second[0].LeaseExpired {
		t.Fatalf("reclaim=%+v error=%v", second, err)
	}
	if err = store.Reserved(ctx, req.RequestID, "reservation-1", "worker-a"); err == nil {
		t.Fatal("stale worker updated a saga after its lease was reassigned")
	}
	if err = store.Reserved(ctx, req.RequestID, "reservation-1", "worker-b"); err != nil {
		t.Fatalf("current owner could not update saga: %v", err)
	}
}
