CREATE TABLE dlq_operation_audit (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id TEXT NOT NULL,
    action TEXT NOT NULL CHECK (action IN ('inspect','replay')),
    operator_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    dlq_topic TEXT NOT NULL,
    dlq_partition INTEGER NOT NULL CHECK (dlq_partition >= 0),
    dlq_offset BIGINT NOT NULL CHECK (dlq_offset >= 0),
    source_topic TEXT,
    source_event_id TEXT,
    failure_code TEXT,
    outcome TEXT NOT NULL CHECK (outcome IN ('selected','succeeded','failed')),
    error_text TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX dlq_operation_audit_job_idx ON dlq_operation_audit(job_id,created_at,id);
CREATE UNIQUE INDEX dlq_operation_audit_replay_success_idx ON dlq_operation_audit(dlq_topic,dlq_partition,dlq_offset,job_id) WHERE action='replay' AND outcome='succeeded';
