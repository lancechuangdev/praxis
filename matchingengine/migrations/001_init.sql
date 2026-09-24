CREATE TABLE matching_books (symbol TEXT PRIMARY KEY, next_sequence BIGINT NOT NULL CHECK (next_sequence > 0));
CREATE TABLE matching_orders (
 request_id TEXT PRIMARY KEY, request_fingerprint TEXT NOT NULL, order_id TEXT NOT NULL UNIQUE,
 user_id TEXT NOT NULL, symbol TEXT NOT NULL, side TEXT NOT NULL, order_type TEXT NOT NULL,
 quantity NUMERIC NOT NULL CHECK (quantity > 0), price NUMERIC NULL CHECK (price IS NULL OR price > 0),
 reservation_id TEXT NOT NULL, status TEXT NOT NULL, engine_sequence BIGINT NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(), UNIQUE(symbol, engine_sequence)
);
CREATE INDEX matching_orders_book_idx ON matching_orders(symbol, side, price, engine_sequence) WHERE status IN ('accepted','open','partially_filled');
CREATE TABLE outbox_events (
 sequence_number BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, id TEXT NOT NULL UNIQUE,
 topic TEXT NOT NULL, event_type TEXT NOT NULL, message_key TEXT NOT NULL, payload JSONB NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(), published_at TIMESTAMPTZ NULL,
 claimed_by TEXT NULL, claimed_until TIMESTAMPTZ NULL, attempt_count INTEGER NOT NULL DEFAULT 0,
 next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(), last_error TEXT NULL
);
CREATE INDEX outbox_events_claim_idx ON outbox_events(next_attempt_at, sequence_number) WHERE published_at IS NULL;
