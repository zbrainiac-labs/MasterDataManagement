-- =============================================================================
-- 003_audit_tables.sql -- Audit event logging (SEC-04)
-- =============================================================================

CREATE TABLE IF NOT EXISTS audit_events (
    event_id     UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    event_type   VARCHAR(20) NOT NULL,   -- INGEST, GOLDEN_CHANGE, BATCH_RESOLVE, READ, ADMIN
    actor        VARCHAR(100) NOT NULL DEFAULT 'mdm-engine',
    source_system VARCHAR(10),
    source_key   VARCHAR(50),
    cluster_id   BIGINT,
    action       VARCHAR(20),            -- INSERT, UPDATE, MERGE, NO_CHANGE, SKIPPED, TRUNCATE
    detail       JSONB,                  -- before_hash, after_hash, latency_ms, error, etc.
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_created ON audit_events (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_cluster ON audit_events (cluster_id) WHERE cluster_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_audit_source ON audit_events (source_system, source_key) WHERE source_system IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_audit_event_type ON audit_events (event_type);
