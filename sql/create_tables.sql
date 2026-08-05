# Schema for the staging table
CREATE TABLE IF NOT EXISTS stg_healthcare_claims (
    claim_id VARCHAR(50), 
    patient_id VARCHAR(50), 
    provider_id VARCHAR(50),
    service_date DATE, 
    submission_date DATE, 
    procedure_code VARCHAR(20),
    diagnosis_code VARCHAR(20), 
    claim_type VARCHAR(30),
    billed_amount NUMERIC(12,2), 
    approved_amount NUMERIC(12,2),
    paid_amount NUMERIC(12,2), 
    claim_status VARCHAR(20),
    denial_reason VARCHAR(255), 
    insurance_plan VARCHAR(50),
    source_filename VARCHAR(255), 
    pipeline_run_id VARCHAR(50),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

# Schema for the fact table
CREATE TABLE IF NOT EXISTS fact_claims (
    claim_key BIGSERIAL PRIMARY KEY,
    claim_id VARCHAR(50) UNIQUE NOT NULL,
    patient_id VARCHAR(50) NOT NULL, 
    provider_id VARCHAR(50) NOT NULL,
    service_date DATE NOT NULL, 
    submission_date DATE NOT NULL,
    procedure_code VARCHAR(20), 
    diagnosis_code VARCHAR(20),
    claim_type VARCHAR(30), 
    billed_amount NUMERIC(12,2) NOT NULL,
    approved_amount NUMERIC(12,2), 
    paid_amount NUMERIC(12,2),
    unpaid_amount NUMERIC(12,2), 
    claim_status VARCHAR(20) NOT NULL,
    denial_reason VARCHAR(255), 
    insurance_plan VARCHAR(50),
    processing_days INTEGER, 
    is_approved BOOLEAN, 
    is_denied BOOLEAN,
    high_value_flag BOOLEAN, 
    duplicate_flag BOOLEAN,
    source_filename VARCHAR(255), 
    pipeline_run_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT chk_fact_claims_submission_after_service
        CHECK (submission_date >= service_date),
    CONSTRAINT chk_fact_claims_billed_amount_positive
        CHECK (billed_amount > 0),
    CONSTRAINT chk_fact_claims_apporoved_non_negative
        CHECK (approved_amount IS NULL OR approved_amount >= 0),
    CONSTRAINT chk_fact_claims_paid_non_negative
        CHECK (paid_amount IS NULL OR paid_amount >= 0),
    CONSTRAINT chk_fact_claims_billed_exceeds_approved
        CHECK (approved_amount IS NULL OR billed_amount >= approved_amount),
    CONSTRAINT chk_fact_claims_approved_exceeds_paid
        CHECK (approved_amount IS NULL OR paid_amount IS NULL OR approved_amount >= paid_amount),
    CONSTRAINT chk_fact_claims_status
        CHECK (claim_status IN ('Submitted', 'Pending', 'Approved', 'Denied', 'Paid')),
);

# Schema for rejected claims table

CREATE TABLE IF NOT EXISTS rejected_claims (
    rejection_id BIGSERIAL PRIMARY KEY, 
    claim_id VARCHAR(50),
    rejection_reason TEXT NOT NULL, 
    raw_record JSONB,
    source_filename VARCHAR(255), 
    pipeline_run_id VARCHAR(50),
    rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

# Schema for audit table
CREATE TABLE IF NOT EXISTS pipeline_audit (
    pipeline_run_id VARCHAR(50) PRIMARY KEY, 
    pipeline_name VARCHAR(100),
    source_filename VARCHAR(255), 
    run_start_time TIMESTAMP,
    run_end_time TIMESTAMP, 
    records_received INTEGER,
    records_loaded INTEGER, 
    records_rejected INTEGER,
    records_flagged_duplicate INTEGER, 
    run_status VARCHAR(20),
    error_message TEXT,

    CONSTRAINT chk_pipeline_audit_status
        CHECK (run_status IS NULL OR run_status IN ('RUNNING', 'SUCCESS', 'FAILED', 'PARTIAL'))
);

# Indexes for fact table
CREATE INDEX IF NOT EXISTS idx_fact_claims_status
    ON fact_claims (claim_status);

CREATE INDEX IF NOT EXISTS idx_fact_claims_provider
    ON fact_claims (provider_id);

CREATE INDEX IF NOT EXISTS idx_fact_claims_service_date
    ON fact_claims (service_date);

CREATE INDEX IF NOT EXISTS idx_fact_claims_patient
    ON fact_claims (patient_id);

CREATE INDEX IF NOT EXISTS idx_fact_claims_run_id
    ON fact_claims (pipeline_run_id);

# Indexes for rejected claims table
CREATE INDEX IF NOT EXISTS idx_rejected_claims_run_id
    ON rejected_claims (pipeline_run_id);

CREATE INDEX IF NOT EXISTS idx_rejected_claims_claim_id
    ON rejected_claims (claim_id);

CREATE INDEX IF NOT EXISTS idx_rejected_claims_rejected_at
    ON rejected_claims (rejected_at);

# Indexes for pipeline audit table
CREATE INDEX IF NOT EXISTS idx_pipeline_audit_status
    ON pipeline_audit (run_status);

CREATE INDEX IF NOT EXISTS idx_pipeline_audit_start
    ON pipeline_audit (run_start_time);