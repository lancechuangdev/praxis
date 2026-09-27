CREATE TABLE order_sagas (
    request_id TEXT PRIMARY KEY,
    request_fingerprint TEXT NOT NULL,
    order_id TEXT NOT NULL UNIQUE,
    correlation_id TEXT NOT NULL,
    causation_id TEXT NOT NULL,
    reservation_id TEXT,
    state TEXT NOT NULL CHECK (state IN ('started','reserved','accepted','release_pending','released')),
    release_reason TEXT CHECK (release_reason IN ('matching_failed','execution_complete')),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX order_sagas_release_due_idx ON order_sagas(next_attempt_at) WHERE state='release_pending';
