-- Migration 001: data lineage and alerting.
--
-- Applied by src/database.py after create_tables.sql and before indexes.sql.
-- Every statement is idempotent, so it is safe on a fresh database and on
-- one that already holds data.


-- Row-level lineage: the record's position in its source file.
-- With source_filename and pipeline_run_id this pins every staged, rejected
-- and fact row to the exact input record it came from.

ALTER TABLE stg_healthcare_claims
    ADD COLUMN IF NOT EXISTS source_row_number INTEGER;

ALTER TABLE fact_claims
    ADD COLUMN IF NOT EXISTS source_row_number INTEGER;

ALTER TABLE rejected_claims
    ADD COLUMN IF NOT EXISTS source_row_number INTEGER;


-- Runs that find only already-loaded files finish as SKIPPED.

ALTER TABLE pipeline_audit
    DROP CONSTRAINT IF EXISTS chk_pipeline_audit_status;

ALTER TABLE pipeline_audit
    ADD CONSTRAINT chk_pipeline_audit_status
    CHECK (
        run_status IS NULL
        OR run_status IN ('RUNNING', 'SUCCESS', 'FAILED', 'PARTIAL', 'SKIPPED')
    );

ALTER TABLE pipeline_audit
    ADD COLUMN IF NOT EXISTS records_inserted INTEGER;

ALTER TABLE pipeline_audit
    ADD COLUMN IF NOT EXISTS records_updated INTEGER;

ALTER TABLE pipeline_audit
    ADD COLUMN IF NOT EXISTS records_unchanged INTEGER;

ALTER TABLE pipeline_audit
    ADD COLUMN IF NOT EXISTS files_received INTEGER;

ALTER TABLE pipeline_audit
    ADD COLUMN IF NOT EXISTS files_rejected INTEGER;

ALTER TABLE pipeline_audit
    ADD COLUMN IF NOT EXISTS files_skipped INTEGER;


-- File-level lineage and duplicate-file control.
--
-- One row per file per run. sha256 identifies content independent of the
-- filename, so the same file dropped twice (or renamed) is recognized.

CREATE TABLE IF NOT EXISTS source_files (
    source_file_id BIGSERIAL PRIMARY KEY,
    pipeline_run_id VARCHAR(50) NOT NULL,
    filename VARCHAR(255) NOT NULL,
    sha256 CHAR(64) NOT NULL,
    size_bytes BIGINT,
    row_count INTEGER,
    status VARCHAR(20) NOT NULL,
    error_message TEXT,
    archived_path TEXT,
    recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_source_files_run_file UNIQUE (pipeline_run_id, sha256, filename),
    CONSTRAINT chk_source_files_status
        CHECK (status IN ('ACCEPTED', 'REJECTED', 'SKIPPED', 'LOADED', 'ARCHIVED'))
);


-- Claim-level lineage: every version of a claim the pipeline has written.
--
-- fact_claims keeps only the latest state; this table answers "when did the
-- claim move from Pending to Paid, and which file said so?". Rows are only
-- added for inserts and real changes, never for unchanged reruns.

CREATE TABLE IF NOT EXISTS fact_claims_history (
    history_id BIGSERIAL PRIMARY KEY,
    claim_id VARCHAR(50) NOT NULL,
    change_type VARCHAR(10) NOT NULL,
    claim_status VARCHAR(20),
    approved_amount NUMERIC(12,2),
    paid_amount NUMERIC(12,2),
    denial_reason VARCHAR(255),
    source_filename VARCHAR(255),
    source_row_number INTEGER,
    pipeline_run_id VARCHAR(50),
    recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT chk_fact_claims_history_change_type
        CHECK (change_type IN ('INSERT', 'UPDATE'))
);


-- Alerts raised by the pipeline, kept for the dashboard and the API.

CREATE TABLE IF NOT EXISTS pipeline_alerts (
    alert_id BIGSERIAL PRIMARY KEY,
    pipeline_run_id VARCHAR(50),
    alert_code VARCHAR(50) NOT NULL,
    severity VARCHAR(10) NOT NULL,
    title VARCHAR(255) NOT NULL,
    message TEXT,
    details JSONB,
    channels_delivered TEXT[],
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT chk_pipeline_alerts_severity
        CHECK (severity IN ('INFO', 'WARNING', 'CRITICAL'))
);


CREATE INDEX IF NOT EXISTS idx_source_files_sha256_status
    ON source_files (sha256, status);

CREATE INDEX IF NOT EXISTS idx_source_files_run_id
    ON source_files (pipeline_run_id);

CREATE INDEX IF NOT EXISTS idx_fact_claims_history_claim_id
    ON fact_claims_history (claim_id, recorded_at);

CREATE INDEX IF NOT EXISTS idx_fact_claims_history_run_id
    ON fact_claims_history (pipeline_run_id);

CREATE INDEX IF NOT EXISTS idx_pipeline_alerts_created_at
    ON pipeline_alerts (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_alerts_run_id
    ON pipeline_alerts (pipeline_run_id);
