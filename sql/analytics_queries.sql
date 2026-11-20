-- Claims count and monetary totals by status

SELECT
    claim_status,
    COUNT(*) AS claim_count,
    SUM(billed_amount) AS total_billed,
    SUM(approved_amount) AS total_approved,
    SUM(paid_amount) AS total_paid,
    SUM(unpaid_amount) AS total_unpaid
FROM fact_claims
GROUP BY claim_status
ORDER BY claim_count DESC;

-- Overall approval and denial rates

SELECT
    COUNT(*) AS total_claims,

    COUNT(*) FILTER (
        WHERE is_approved = TRUE
    ) AS approved_claims,

    COUNT(*) FILTER (
        WHERE is_denied = TRUE
    ) AS denied_claims,

    ROUND(
        100.0 * COUNT(*) FILTER (
            WHERE is_approved = TRUE
        ) / NULLIF(COUNT(*), 0),
        2
    ) AS approval_rate_percent,

    ROUND(
        100.0 * COUNT(*) FILTER (
            WHERE is_denied = TRUE
        ) / NULLIF(COUNT(*), 0),
        2
    ) AS denial_rate_percent

FROM fact_claims;

-- Top denial reasons

SELECT
    denial_reason,
    COUNT(*) AS denial_count
FROM fact_claims
WHERE is_denied = TRUE
GROUP BY denial_reason
ORDER BY denial_count DESC
LIMIT 10;

-- Providers with the highest billed and paid amounts

SELECT
    provider_id,
    COUNT(*) AS claim_count,
    SUM(billed_amount) AS total_billed,
    SUM(paid_amount) AS total_paid
FROM fact_claims
GROUP BY provider_id
ORDER BY total_billed DESC, total_paid DESC
LIMIT 10;

-- Average processing time by claim type

SELECT
    claim_type,
    COUNT(*) AS claim_count,
    ROUND(AVG(processing_days), 2) AS avg_processing_days
FROM fact_claims
GROUP BY claim_type
ORDER BY avg_processing_days DESC;

-- Monthly claim volume and payment trends

SELECT
    DATE_TRUNC('month', service_date)::date AS service_month,
    COUNT(*) AS claim_count,
    SUM(billed_amount) AS total_billed,
    SUM(approved_amount) AS total_approved,
    SUM(paid_amount) AS total_paid
FROM fact_claims
GROUP BY service_month
ORDER BY service_month;

-- Potential duplicate claims

SELECT
    patient_id,
    provider_id,
    service_date,
    procedure_code,
    billed_amount,
    COUNT(*) AS claim_count
FROM fact_claims
GROUP BY
    patient_id,
    provider_id,
    service_date,
    procedure_code,
    billed_amount
HAVING COUNT(*) > 1
ORDER BY claim_count DESC;

-- High-value claims requiring review

SELECT
    claim_id,
    patient_id,
    provider_id,
    service_date,
    claim_status,
    billed_amount,
    approved_amount,
    paid_amount
FROM fact_claims
WHERE high_value_flag = TRUE
ORDER BY billed_amount DESC;