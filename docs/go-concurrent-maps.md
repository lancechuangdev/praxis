# Concurrent maps in Go

A normal Go map is safe for concurrent reads only when no goroutine modifies it. Concurrent writes, or reads concurrent with writes, require synchronization.

The default choice for application state is a typed map protected by `sync.Mutex` or `sync.RWMutex`. Use `sync.Map` only when its specialized access patterns match the workload and, when performance motivates the choice, benchmarks demonstrate a benefit.

## Map with a mutex

Use `map` with `sync.Mutex` when:

- reads and writes are both common;
- a multi-step read-modify-write operation must be atomic;
- multiple entries or indexes must change together;
- application invariants matter; or
- type safety and straightforward code are priorities.

A per-user limiter requires the lookup, possible creation, refill, and token consumption to behave as one operation:

```go
type Limiter struct {
	mu      sync.Mutex
	buckets map[string]*Bucket
}

func (l *Limiter) Allow(userID string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	bucket, ok := l.buckets[userID]
	if !ok {
		bucket = newBucket()
		l.buckets[userID] = bucket
	}

	return bucket.consumeToken()
}
```

Individual `sync.Map` calls would be safe, but they would not make this entire sequence atomic.

Use `sync.RWMutex` when reads greatly outnumber writes and concurrent readers provide a measured benefit:

```go
type ConfigStore struct {
	mu     sync.RWMutex
	values map[string]string
}

func (s *ConfigStore) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.values[key]
	return value, ok
}

func (s *ConfigStore) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[key] = value
}
```

`RWMutex` is not automatically faster than `Mutex`. Its extra coordination can cost more for tiny critical sections or frequent writes.

## `sync.Map`

`sync.Map` is a good candidate when entries are independent and either:

- each entry is written once and read many times; or
- many goroutines mostly operate on disjoint keys.

A future Praxis WebSocket gateway could have this access pattern for connections keyed by connection ID:

```go
type ConnectionRegistry struct {
	connections sync.Map // map[string]*Connection
}

func (r *ConnectionRegistry) Register(conn *Connection) bool {
	_, alreadyExists := r.connections.LoadOrStore(conn.ID, conn)
	return !alreadyExists
}

func (r *ConnectionRegistry) Find(id string) (*Connection, bool) {
	value, ok := r.connections.Load(id)
	if !ok {
		return nil, false
	}
	return value.(*Connection), true
}

func (r *ConnectionRegistry) Remove(id string) {
	r.connections.Delete(id)
}
```

Connections are independent and are normally inserted once, read repeatedly, and deleted once. The registry would still need explicit connection limits and prompt cleanup after disconnects.

A lazily populated immutable asset-metadata cache is another possible use:

```go
type AssetCache struct {
	assets sync.Map // map[string]*AssetMetadata
}

func (c *AssetCache) Get(
	id string,
	load func(string) (*AssetMetadata, error),
) (*AssetMetadata, error) {
	if value, ok := c.assets.Load(id); ok {
		return value.(*AssetMetadata), nil
	}

	asset, err := load(id)
	if err != nil {
		return nil, err
	}

	actual, _ := c.assets.LoadOrStore(id, asset)
	return actual.(*AssetMetadata), nil
}
```

Two goroutines can both execute `load` before one wins `LoadOrStore`. Use request coalescing such as `singleflight` if duplicate loads are expensive or unsafe. A cache requiring expiration, size-based eviction, or coordinated refresh should normally use a bounded cache implementation instead.

## When not to use `sync.Map`

Do not use `sync.Map` for a multi-step update and assume the complete operation is atomic:

```go
// Bad: concurrent goroutines can load the same value and lose an update.
value, _ := counters.Load("orders")
count := value.(int)
counters.Store("orders", count+1)
```

Use a mutex around a typed map, or store an atomic counter as the map value when entries are truly independent.

An in-memory matching order book is also a poor `sync.Map` candidate. Its order index, bid levels, ask levels, and price-time priority must remain consistent across a multi-step mutation:

```go
type OrderBook struct {
	mu     sync.Mutex
	orders map[string]*Order
	bids   PriceLevels
	asks   PriceLevels
}
```

One mutex, sharded books, or one owner goroutine per symbol would preserve these invariants more clearly than independent `sync.Map` operations.

Likewise, a Praxis rate limiter would require atomic bucket updates and probably cross-replica enforcement. A local typed map with locking or an external atomic store is more appropriate than `sync.Map` alone.

## Current Praxis assessment

The current implementation has no compelling `sync.Map` use:

- `Relay.TopicMap` is initialized at startup and then only read. An ordinary map is safe for concurrent reads when it is never mutated.
- Ledger reservation and balance maps are request-local and not shared by goroutines.
- Metrics use atomic counters, which are a better fit than a map.
- Durable order, saga, inbox, and outbox state resides in PostgreSQL.

The strongest potential use is the proposed WebSocket or SSE connection registry. An immutable reference-data cache is another possibility. Both should remain bounded, and `sync.Map` should be selected only after confirming that keys are independent and that no cross-entry invariant is required.

## Decision guide

```text
Multi-step or cross-entry atomicity required? -> map + Mutex
Ordinary application state?                  -> map + Mutex
Clearly read-heavy workload?                 -> consider map + RWMutex
Write once/read many, independent keys?      -> consider sync.Map
Choosing based on performance?               -> benchmark both
```
