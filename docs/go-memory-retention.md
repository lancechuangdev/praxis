# Go memory retention and leak troubleshooting

This document describes two ways apparently small or empty Go slices can retain much larger objects in production. These patterns matter at long-lived ownership boundaries such as caches, retry queues, worker fields, and goroutines.

## Small Kafka subslice retains a large message

A Kafka message may contain a large value. Slicing out a small identifier does not copy its bytes: the new slice still refers to the message's backing array.

```go
type Cache struct {
	IDs map[string][]byte
}

func handleMessage(cache *Cache, message kafka.Message) {
	// message.Value may have a 10 MB backing array.
	id := message.Value[:32]

	// Bad: retaining id may retain all 10 MB.
	cache.IDs[string(id)] = id
}
```

The retained slice may have a length of only 32 while its capacity exposes how much of the original allocation remains behind it:

```go
fmt.Println(len(id)) // 32
fmt.Println(cap(id)) // potentially 10 MB
```

Copy data when it crosses into longer-lived ownership:

```go
func handleMessage(cache *Cache, message kafka.Message) {
	if len(message.Value) < 32 {
		return
	}

	id := bytes.Clone(message.Value[:32])
	cache.IDs[string(id)] = id
}
```

`append([]byte(nil), message.Value[:32]...)` is an equivalent copy on Go versions without `bytes.Clone`.

## Truncated batch retains referenced objects

Reducing a slice's length to zero does not erase elements in its backing array. When the slice is kept on a reusable worker, pointers in the unused capacity can keep entire object graphs reachable.

```go
type Event struct {
	Payload []byte
}

type Worker struct {
	batch []*Event
}

func (w *Worker) process() {
	for _, event := range w.batch {
		publish(event)
	}

	// Bad: the backing array can still contain pointers to all Events.
	w.batch = w.batch[:0]
}
```

Clear reference-bearing elements before retaining and reusing the allocation:

```go
func (w *Worker) process() {
	for _, event := range w.batch {
		publish(event)
	}

	clear(w.batch)
	w.batch = w.batch[:0]
}
```

The same rule applies to slices of structs containing strings, slices, maps, pointers, interfaces, or functions:

```go
type Event struct {
	ID      string
	Payload []byte
}

type Worker struct {
	batch []Event
}

func (w *Worker) reset() {
	// Bad: old strings and payload slices remain in the backing array.
	w.batch = w.batch[:0]
}
```

Use `clear(w.batch)` before truncating it. If an unusually large batch expanded the allocation and that capacity is no longer useful, release it instead:

```go
w.batch = nil
```

## Long-lived maps retain capacity and referenced data

Maps do not have the same subslice/backing-array relationship, but a long-lived map can retain substantial memory after a traffic spike. Deleting entries normally makes their keys and values unreachable, but it does not guarantee that the map's internal bucket storage shrinks.

```go
type Session struct {
	UserID  string
	Payload []byte
}

type Server struct {
	// Reused for the lifetime of the process.
	sessions map[string]*Session
}

func (s *Server) processTraffic() {
	// A traffic spike grows the map to millions of entries.
	for i := 0; i < 5_000_000; i++ {
		payload := make([]byte, 1<<20) // 1 MB
		s.sessions[makeSessionID(i)] = &Session{
			UserID:  makeUserID(i),
			Payload: payload[:32], // Also retains the entire 1 MB allocation.
		}
	}

	// Bad: deleting most entries does not guarantee that the map's
	// internal bucket storage shrinks. The remaining values also retain
	// their Session objects and complete Payload backing arrays.
	for id := range s.sessions {
		if shouldDelete(id) {
			delete(s.sessions, id)
		}
	}
}
```

Even an empty map may retain capacity acquired during a temporary spike:

```go
cache := make(map[string][]byte)
for i := 0; i < 5_000_000; i++ {
	cache[key(i)] = make([]byte, 128)
}

// Bad for a long-lived cache after exceptional growth: len(cache) becomes
// zero, but the map may retain its large internal bucket allocation.
for k := range cache {
	delete(cache, k)
}
```

If the capacity is no longer useful, replace the map so the old storage can be collected:

```go
cache = make(map[string][]byte)
```

When entries must survive, rebuild a right-sized map:

```go
replacement := make(map[string]*Session, len(s.sessions))
for id, session := range s.sessions {
	replacement[id] = session
}
s.sessions = replacement
```

Rebuilding should follow measured or exceptional growth rather than run frequently because it creates allocation and garbage-collection work. Copy small subslices before storing them as map values regardless of whether the map itself is rebuilt:

```go
smallPayload := bytes.Clone(payload[:32])
s.sessions[id] = &Session{Payload: smallPayload}
```

During map growth, the runtime may also temporarily hold old bucket storage while entries are migrated. Peak memory can therefore be higher than the map's steady-state footprint.

### Current Praxis exposure

The current Praxis application code has low exposure to this long-lived map pattern:

- `Relay.TopicMap` is populated once from `OUTBOX_KAFKA_TOPIC_MAP`, remains configuration-sized, and does not grow with production traffic.
- `lockReservations` creates a request-local map containing at most two reservations in its current call sites.
- `lockBalances` creates a request-local deduplication map containing at most four IDs in its current call sites.
- JSON payload and HTTP response maps are temporary and become unreachable when each operation finishes.
- Praxis currently has no application-level in-memory session map, order cache, idempotency map, or retry map that grows with traffic.

This risk would become material if Praxis adds an in-memory order book, cache, WebSocket connection registry, deduplication store, retry queue, or per-client rate limiter. Those components should have explicit entry and byte limits, an eviction policy, and a way to discard or rebuild storage after exceptional growth.

## Goroutine leak incident runbook with pprof

A goroutine leak occurs when code repeatedly starts goroutines that never exit. Each goroutine retains its stack and everything reachable from it, so a leak can appear as both a rising goroutine count and sustained heap growth. Common causes include blocked channel operations, calls without cancellation or deadlines, forgotten ticker cleanup, and workers whose exit condition can never occur.

Assume a recent merge introduced this request path:

```go
func dependencyReady(ctx context.Context, check func() error) error {
	result := make(chan error)
	go func() {
		// Bad: if the caller times out first, this send blocks forever because
		// result is unbuffered and no receiver remains.
		result <- check()
	}()

	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
```

Under dependency latency, every timed-out request can leave another goroutine blocked in `result <- check()`. The investigation should establish that goroutines grow with traffic, identify their shared stack, connect that stack to the merge, and verify that the count stabilizes after remediation.

### Prepare pprof safely

Praxis does not currently expose pprof endpoints. If they are added, serve them on a dedicated diagnostics listener bound to loopback or a private operations interface, not the public API listener:

```go
import (
	"log/slog"
	"net/http"
	_ "net/http/pprof"
)

func serveDiagnostics(log *slog.Logger) {
	server := &http.Server{
		Addr:              "127.0.0.1:6060",
		Handler:           http.DefaultServeMux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Error("diagnostics server stopped", "error", err)
	}
}
```

Restrict the listener with network policy, firewall rules, and operator authentication or tunneling. Profiles expose stack traces, function names, URLs, and potentially sensitive process details. Prefer a temporary port-forward or authenticated internal diagnostics path over public exposure.

Also export `runtime.NumGoroutine()` as a gauge. Alert on sustained growth relative to traffic and the service's established baseline rather than on a universal fixed number.

### Triage the incident

First determine whether the growth is persistent rather than a short traffic burst:

1. Compare the goroutine count with request rate, latency, downstream errors, open connections, and pod age.
2. Check whether the count returns toward baseline after traffic falls.
3. Compare affected instances with a canary or an instance running the previous revision.
4. Record the deployment, commit, configuration, and first observed increase before restarting anything.

Use an authorized tunnel to the diagnostics listener. For Kubernetes, an example is:

```text
kubectl -n praxis port-forward pod/<order-service-pod> 6060:6060
```

Capture both a summarized profile and a full goroutine dump:

```text
curl -fsS http://127.0.0.1:6060/debug/pprof/goroutine > /tmp/goroutine-before.pb.gz
curl -fsS 'http://127.0.0.1:6060/debug/pprof/goroutine?debug=2' > /tmp/goroutine-before.txt
go tool pprof -top /tmp/goroutine-before.pb.gz
```

`debug=2` is valuable during an incident because it shows individual goroutines, their complete stacks, and states such as `chan send`, `chan receive`, `select`, `IO wait`, or `semacquire`. The binary profile groups identical stacks and makes dominant leak signatures easier to rank.

Do not start with a CPU profile when the symptom is a growing goroutine count. Block and mutex profiles can help with contention, but the goroutine profile is the primary artifact for goroutines that never terminate.

### Capture growth, not only one snapshot

One snapshot can confuse legitimate long-lived server goroutines with leaks. Keep load and instance selection consistent, wait a representative interval, and collect another profile:

```text
curl -fsS http://127.0.0.1:6060/debug/pprof/goroutine > /tmp/goroutine-after.pb.gz
curl -fsS 'http://127.0.0.1:6060/debug/pprof/goroutine?debug=2' > /tmp/goroutine-after.txt

go tool pprof -top /tmp/goroutine-after.pb.gz
go tool pprof -base /tmp/goroutine-before.pb.gz -top /tmp/goroutine-after.pb.gz
go tool pprof -http=127.0.0.1:0 /tmp/goroutine-after.pb.gz
```

The base comparison highlights stack groups that grew between captures. Preserve both profiles with the incident record; a post-restart profile alone destroys the most useful evidence.

For the bad example, a growing stack group would resemble:

```text
goroutine 4821 [chan send]:
praxis/orderservice/...dependencyReady.func1()
	.../readiness.go:42
created by praxis/orderservice/...dependencyReady in goroutine 4817
	.../readiness.go:39
```

Hundreds or thousands of goroutines with the same application stack and wait reason strongly indicate a leak. A large number of `IO wait` goroutines is not enough by itself; HTTP and database clients legitimately wait on I/O. Confirm that the group grows without later falling and inspect whether its contexts, deadlines, and transports are bounded.

### Connect the stack to the regression

For each growing stack group:

- identify the first application-owned frame rather than stopping at runtime or standard-library frames;
- inspect the goroutine creation site shown by `created by`;
- use `git blame`, the deployment diff, and recent merge history to find the introducing change;
- determine what should end the goroutine and why that event does not occur;
- check whether the stack retains request bodies, Kafka messages, database rows, timers, or other large objects;
- reproduce with the same timeout, cancellation, or downstream-failure condition.

Correlate a goroutine profile with a heap profile when memory is also rising:

```text
curl -fsS http://127.0.0.1:6060/debug/pprof/heap > /tmp/heap.pb.gz
go tool pprof -top -inuse_space /tmp/heap.pb.gz
```

The heap profile identifies retained memory, while goroutine stacks explain why roots remain live. A goroutine count can be stable while leaked goroutines retain very large buffers, so investigate both dimensions when RSS or heap usage is abnormal.

### Mitigate safely

If the process is approaching its memory or goroutine limit, remove it from traffic and restart it only after capturing profiles. A restart restores capacity but does not fix the leak. Roll back or disable the offending path when the regression is tied to a recent release.

Do not kill arbitrary goroutines; Go provides no safe general mechanism for doing so. Cancellation must be designed into the work. Avoid adding `recover()` because a leak is not a panic and recovery will not unblock a goroutine.

The example can be fixed with a one-result buffered channel so the producer can finish even after the caller leaves:

```go
func dependencyReady(ctx context.Context, check func() error) error {
	result := make(chan error, 1)
	go func() {
		result <- check()
	}()

	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
```

Prefer passing `context.Context` into the dependency so the underlying work also stops:

```go
func dependencyReady(ctx context.Context, check func(context.Context) error) error {
	result := make(chan error, 1)
	go func() {
		result <- check(ctx)
	}()

	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
```

The buffer prevents a blocked result send; context propagation addresses work that otherwise continues after cancellation. Both can be necessary.

### Verify the fix

Add a regression test that repeatedly exercises the timeout path. A goroutine-count assertion should allow runtime noise and use eventual stabilization rather than require an exact count:

```go
func TestDependencyReadyDoesNotLeakAfterTimeout(t *testing.T) {
	baseline := runtime.NumGoroutine()

	for i := 0; i < 1_000; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
		_ = dependencyReady(ctx, func(context.Context) error {
			return nil
		})
		cancel()
	}

	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > baseline+10 && time.Now().Before(deadline) {
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
	}
	if got := runtime.NumGoroutine(); got > baseline+10 {
		t.Fatalf("goroutines did not return to baseline: before=%d after=%d", baseline, got)
	}
}
```

Treat this as a supporting regression test rather than mathematical proof because the Go runtime and test runner have background goroutines. For stronger ownership assertions in complex worker code, use explicit shutdown hooks, wait groups, and completion channels.

Deploy the fix to a canary under the failure mode that triggered the incident, then repeat the before/after profile capture. Close the incident only when:

- the suspect stack group no longer grows;
- total goroutines return to a stable range after load subsides;
- heap and RSS stabilize;
- request latency and downstream connection counts recover; and
- the regression test passes under the race detector.

## Review guidance

Subslice use is not inherently unsafe. Short-lived parsing slices are efficient and normally desirable. Copy or clear when data crosses into longer-lived state:

- Copy small values derived from large buffers before storing them in caches, queues, persistent worker fields, or goroutines.
- Clear reusable slices whose elements contain references before setting their length to zero.
- Drop a reusable slice entirely after an exceptional allocation when keeping its capacity would waste significant memory.
- Bound long-lived maps and consider replacing or rebuilding them after exceptional growth when their retained capacity is no longer useful.
- Remember that remaining map keys and values can retain complete object graphs, including the backing arrays of stored slices.
- Bound Kafka message sizes, HTTP bodies, batch sizes, and retry queues so retention has a predictable upper limit.
- During regression testing, check both `len` and `cap` of retained byte slices and inspect heap profiles under downstream failure.
- Give every started goroutine an explicit owner, cancellation path, and observable termination condition.
- Track goroutine count and retain pprof access through a secured diagnostics path so leak evidence can be captured before restarting a process.

The practical rule is to copy at ownership boundaries rather than copying every temporary slice.
