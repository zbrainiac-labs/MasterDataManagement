-- =============================================================================
-- 004_suppressions_table.sql -- Match suppressions for cluster split/unmatch (BIZ-15)
-- =============================================================================

CREATE TABLE IF NOT EXISTS match_suppressions (
    id BIGSERIAL PRIMARY KEY,
    source_system_a VARCHAR(5) NOT NULL,
    source_key_a VARCHAR(50) NOT NULL,
    source_system_b VARCHAR(5) NOT NULL,
    source_key_b VARCHAR(50) NOT NULL,
    reason TEXT NOT NULL,
    created_by VARCHAR(100) NOT NULL DEFAULT 'admin',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_system_a, source_key_a, source_system_b, source_key_b)
);

CREATE INDEX IF NOT EXISTS idx_suppression_a ON match_suppressions (source_system_a, source_key_a);
CREATE INDEX IF NOT EXISTS idx_suppression_b ON match_suppressions (source_system_b, source_key_b);
