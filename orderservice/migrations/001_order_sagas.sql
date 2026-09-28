CREATE TABLE order_sagas (
    request_id TEXT PRIMARY KEY,
    request_fingerprint TEXT NOT NULL,
    request_payload JSONB NOT NULL,
    order_id TEXT NOT NULL UNIQUE,
    correlation_id TEXT NOT NULL,
    causation_id TEXT NOT NULL,
    reservation_id TEXT,
    state TEXT NOT NULL CHECK (state IN ('started','reservation_pending','reserved','accepted','release_pending','released','dead_letter')),
    release_reason TEXT CHECK (release_reason IN ('matching_failed','execution_complete')),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX order_sagas_release_due_idx ON order_sagas(next_attempt_at) WHERE state='release_pending';

CREATE INDEX order_sagas_reservation_due_idx
    ON order_sagas(next_attempt_at) WHERE state='reservation_pending';

CREATE TABLE order_saga_dead_letters (
    request_id TEXT PRIMARY KEY REFERENCES order_sagas(request_id),
    order_id TEXT NOT NULL,
    failure_stage TEXT NOT NULL CHECK (failure_stage IN ('reservation','release')),
    release_reason TEXT,
    attempt_count INTEGER NOT NULL CHECK (attempt_count >= 1),
    last_error TEXT NOT NULL,
    failed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at TIMESTAMPTZ
);

CREATE INDEX order_saga_dead_letters_unresolved_idx
    ON order_saga_dead_letters(failed_at) WHERE resolved_at IS NULL;
