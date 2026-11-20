-- Staging table
CREATE INDEX IF NOT EXISTS idx_stg_claims_run_id
    ON stg_healthcare_claims (pipeline_run_id);


-- Fact table: claim status filtering
CREATE INDEX IF NOT EXISTS idx_fact_claims_status
    ON fact_claims (claim_status);


-- Fact table: provider-level lookups
CREATE INDEX IF NOT EXISTS idx_fact_claims_provider
    ON fact_claims (provider_id);


-- Fact table: service-date range queries
CREATE INDEX IF NOT EXISTS idx_fact_claims_service_date
    ON fact_claims (service_date);


-- Fact table: pipeline reconciliation and auditing
CREATE INDEX IF NOT EXISTS idx_fact_claims_run_id
    ON fact_claims (pipeline_run_id);


-- Fact table: duplicate/business-key lookups
CREATE INDEX IF NOT EXISTS idx_fact_claims_business_key
    ON fact_claims (
        patient_id,
        provider_id,
        service_date,
        procedure_code,
        billed_amount
    );


-- Rejected claims
CREATE INDEX IF NOT EXISTS idx_rejected_claims_run_id
    ON rejected_claims (pipeline_run_id);

CREATE INDEX IF NOT EXISTS idx_rejected_claims_claim_id
    ON rejected_claims (claim_id);

CREATE INDEX IF NOT EXISTS idx_rejected_claims_rejected_at
    ON rejected_claims (rejected_at);


-- Pipeline audit
CREATE INDEX IF NOT EXISTS idx_pipeline_audit_status
    ON pipeline_audit (run_status);

CREATE INDEX IF NOT EXISTS idx_pipeline_audit_start
    ON pipeline_audit (run_start_time);