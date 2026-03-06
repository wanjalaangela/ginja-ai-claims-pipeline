-- EXECUTIVE SUMMARY QUERIES
-- High-level KPIs, trends, business metrics

-- Total Claims Overview
SELECT 
    COUNT(*) as total_claims,
    COUNT(DISTINCT member_id) as total_members,
    COUNT(DISTINCT provider_id) as total_providers,
    ROUND(AVG(claim_amount), 2) as avg_claim_amount,
    ROUND(MIN(claim_amount), 2) as min_claim_amount,
    ROUND(MAX(claim_amount), 2) as max_claim_amount
FROM claims;

-- Approval Rate
SELECT 
    status,
    COUNT(*) as count,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM claims), 2) as percentage
FROM claims
GROUP BY status
ORDER BY count DESC;

-- Claims by Insurer
SELECT 
    i.insurer_name,
    COUNT(c.claim_id) as total_claims,
    ROUND(AVG(c.claim_amount), 2) as avg_claim,
    ROUND(SUM(c.approved_amount), 2) as total_approved
FROM claims c
LEFT JOIN insurers i ON c.insurer_id = i.insurer_id
GROUP BY i.insurer_name
ORDER BY total_claims DESC;

-- Claims Trend Over Time
SELECT 
    DATE_TRUNC('month', date_of_service)::DATE as month,
    COUNT(*) as claims_count,
    ROUND(AVG(claim_amount), 2) as avg_amount,
    ROUND(SUM(approved_amount), 2) as total_approved
FROM claims
GROUP BY DATE_TRUNC('month', date_of_service)
ORDER BY month DESC;

-- Top 10 Providers by Volume
SELECT 
    p.provider_name,
    p.location,
    COUNT(c.claim_id) as total_claims,
    ROUND(AVG(c.claim_amount), 2) as avg_claim,
    ROUND(SUM(c.approved_amount), 2) as total_approved
FROM claims c
LEFT JOIN providers p ON c.provider_id = p.provider_id
GROUP BY p.provider_id, p.provider_name, p.location
ORDER BY total_claims DESC
LIMIT 10;

-- Claims by Location
SELECT 
    location,
    COUNT(*) as total_claims,
    ROUND(AVG(claim_amount), 2) as avg_claim,
    COUNT(DISTINCT provider_id) as provider_count
FROM claims
GROUP BY location
ORDER BY total_claims DESC;

-- Approval Rate by Provider
SELECT 
    p.provider_name,
    COUNT(c.claim_id) as total_claims,
    SUM(CASE WHEN c.status = 'Approved' THEN 1 ELSE 0 END) as approved_count,
    ROUND(100.0 * SUM(CASE WHEN c.status = 'Approved' THEN 1 ELSE 0 END) / COUNT(c.claim_id), 2) as approval_rate
FROM claims c
LEFT JOIN providers p ON c.provider_id = p.provider_id
GROUP BY p.provider_id, p.provider_name
ORDER BY approval_rate DESC
LIMIT 10;


-- OPERATIONS METRICS
-- Process efficiency, data quality, pipeline health

-- Claims Processing Time Analysis
SELECT 
    ROUND(AVG(processing_days), 2) as avg_processing_days,
    MIN(processing_days) as min_days,
    MAX(processing_days) as max_days,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY processing_days) as median_days,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY processing_days) as percentile_95_days
FROM claims;

-- Claims by Status
SELECT 
    status,
    COUNT(*) as count,
    ROUND(AVG(claim_amount), 2) as avg_amount,
    ROUND(AVG(processing_days), 2) as avg_processing_days
FROM claims
GROUP BY status
ORDER BY count DESC;

-- Processing Time by Status
SELECT 
    status,
    ROUND(AVG(processing_days), 2) as avg_processing_days,
    MIN(processing_days) as min_days,
    MAX(processing_days) as max_days
FROM claims
WHERE status IN ('Approved', 'Rejected', 'Pending')
GROUP BY status
ORDER BY avg_processing_days DESC;

-- Claims Requiring Manual Review
SELECT 
    claim_id,
    member_id,
    provider_id,
    claim_amount,
    approved_amount,
    ROUND((claim_amount - approved_amount), 2) as difference,
    ROUND(100.0 * (claim_amount - approved_amount) / claim_amount, 2) as percent_difference,
    status
FROM claims
WHERE claim_amount > approved_amount
    AND approved_amount IS NOT NULL
ORDER BY (claim_amount - approved_amount) DESC
LIMIT 20;

-- Data Quality Metrics
SELECT 
    COUNT(*) as total_records,
    SUM(CASE WHEN claim_amount IS NULL THEN 1 ELSE 0 END) as missing_amounts,
    SUM(CASE WHEN date_of_service IS NULL THEN 1 ELSE 0 END) as missing_dates,
    SUM(CASE WHEN provider_id IS NULL THEN 1 ELSE 0 END) as missing_providers,
    ROUND(100.0 * SUM(CASE WHEN claim_amount IS NOT NULL AND date_of_service IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as completeness_percent
FROM claims;

-- Claims by Insurance Plan
SELECT 
    insurance_plan_id,
    COUNT(*) as claim_count,
    ROUND(AVG(claim_amount), 2) as avg_claim,
    ROUND(SUM(approved_amount), 2) as total_approved
FROM claims
GROUP BY insurance_plan_id
ORDER BY claim_count DESC;

-- Rejection Reasons Analysis
SELECT 
    status,
    diagnosis_code,
    COUNT(*) as count,
    ROUND(AVG(claim_amount), 2) as avg_claim
FROM claims
WHERE status = 'Rejected'
GROUP BY status, diagnosis_code
ORDER BY count DESC
LIMIT 15;


-- PRODUCT & ML TEAM QUERIES
-- Features, model inputs, data distributions

-- Claim Amount Distribution
SELECT 
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY claim_amount) as q1,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY claim_amount) as median,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY claim_amount) as q3,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY claim_amount) as p95,
    ROUND(STDDEV(claim_amount), 2) as std_dev,
    ROUND(AVG(claim_amount), 2) as mean
FROM claims
WHERE claim_amount IS NOT NULL;

-- Diagnosis Code Frequency
SELECT 
    diagnosis_code,
    COUNT(*) as frequency,
    ROUND(AVG(claim_amount), 2) as avg_claim,
    ROUND(AVG(CASE WHEN status = 'Approved' THEN 1 ELSE 0 END), 2) as approval_rate,
    COUNT(DISTINCT member_id) as unique_members
FROM claims
GROUP BY diagnosis_code
ORDER BY frequency DESC;

-- Member Claim History
SELECT 
    member_id,
    COUNT(*) as total_claims,
    ROUND(AVG(claim_amount), 2) as avg_claim,
    ROUND(SUM(claim_amount), 2) as total_claims_value,
    MIN(date_of_service) as first_claim_date,
    MAX(date_of_service) as last_claim_date,
    ROUND((MAX(date_of_service) - MIN(date_of_service))::NUMERIC / 30, 1) as months_active
FROM claims
GROUP BY member_id
HAVING COUNT(*) >= 2
ORDER BY total_claims DESC
LIMIT 20;

-- Provider Characteristics
SELECT 
    p.provider_id,
    p.provider_name,
    p.location,
    COUNT(c.claim_id) as total_claims,
    ROUND(AVG(c.claim_amount), 2) as avg_claim,
    ROUND(100.0 * SUM(CASE WHEN c.status = 'Approved' THEN 1 ELSE 0 END) / COUNT(c.claim_id), 2) as approval_rate,
    ROUND(100.0 * SUM(CASE WHEN c.status = 'Rejected' THEN 1 ELSE 0 END) / COUNT(c.claim_id), 2) as rejection_rate,
    ROUND(AVG(c.processing_days), 2) as avg_processing_days
FROM claims c
JOIN providers p ON c.provider_id = p.provider_id
GROUP BY p.provider_id, p.provider_name, p.location
ORDER BY total_claims DESC;

-- Claims Approval Likelihood by Characteristics
SELECT 
    status,
    insurance_plan_id,
    COUNT(*) as claim_count,
    ROUND(AVG(claim_amount), 2) as avg_amount,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM claims WHERE insurance_plan_id IS NOT NULL), 2) as percent_of_total
FROM claims
WHERE insurance_plan_id IS NOT NULL
GROUP BY status, insurance_plan_id
ORDER BY claim_count DESC;

-- High-Risk Claims
SELECT 
    claim_id,
    member_id,
    provider_id,
    claim_amount,
    approved_amount,
    status,
    processing_days,
    CASE 
        WHEN claim_amount > (SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY claim_amount) FROM claims) THEN 'HIGH_AMOUNT'
        WHEN claim_amount < approved_amount THEN 'UNDER_APPROVED'
        WHEN processing_days > 10 THEN 'SLOW_PROCESSING'
        ELSE 'NORMAL'
    END as risk_flag
FROM claims
WHERE claim_amount > (SELECT PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY claim_amount) FROM claims)
   OR status = 'Rejected'
ORDER BY claim_amount DESC
LIMIT 30;

-- Temporal Patterns
SELECT 
    EXTRACT(MONTH FROM date_of_service) as month,
    COUNT(*) as claim_count,
    ROUND(AVG(claim_amount), 2) as avg_claim,
    ROUND(SUM(approved_amount), 2) as total_approved
FROM claims
GROUP BY EXTRACT(MONTH FROM date_of_service)
ORDER BY month;


-- EXPLORATORY ANALYSIS
-- Deep dives, data exploration, pattern discovery

-- Correlation between Claim and Approved Amount
SELECT 
    ROUND(AVG(claim_amount), 2) as avg_claimed,
    ROUND(AVG(approved_amount), 2) as avg_approved,
    ROUND(100.0 * AVG(approved_amount) / AVG(claim_amount), 2) as approval_percent
FROM claims
WHERE approved_amount IS NOT NULL AND claim_amount IS NOT NULL;

-- Members with Unusual Patterns
SELECT 
    member_id,
    COUNT(*) as total_claims,
    ROUND(AVG(claim_amount), 2) as avg_claim,
    ROUND(STDDEV(claim_amount), 2) as claim_std_dev,
    MAX(claim_amount) - MIN(claim_amount) as claim_range,
    COUNT(DISTINCT provider_id) as unique_providers
FROM claims
GROUP BY member_id
HAVING COUNT(*) >= 3
ORDER BY claim_range DESC
LIMIT 20;

-- Provider-Member Network
SELECT 
    c.provider_id,
    c.member_id,
    p.provider_name,
    COUNT(c.claim_id) as interaction_count,
    ROUND(AVG(c.claim_amount), 2) as avg_claim,
    ROUND(100.0 * SUM(CASE WHEN c.status = 'Approved' THEN 1 ELSE 0 END) / COUNT(c.claim_id), 2) as approval_rate
FROM claims c
JOIN providers p ON c.provider_id = p.provider_id
GROUP BY c.provider_id, c.member_id, p.provider_name
HAVING COUNT(c.claim_id) >= 2
ORDER BY interaction_count DESC
LIMIT 30;

-- Geographic Distribution Analysis
SELECT 
    location,
    COUNT(*) as total_claims,
    COUNT(DISTINCT member_id) as unique_members,
    COUNT(DISTINCT provider_id) as provider_count,
    ROUND(AVG(claim_amount), 2) as avg_claim,
    ROUND(AVG(processing_days), 2) as avg_processing_days
FROM claims
GROUP BY location
ORDER BY total_claims DESC;

-- Claims with Data Quality Issues
SELECT 
    claim_id,
    member_id,
    claim_amount,
    approved_amount,
    date_of_service,
    date_claimed,
    CASE 
        WHEN claim_amount IS NULL THEN 'MISSING_AMOUNT'
        WHEN date_claimed < date_of_service THEN 'DATE_ERROR'
        WHEN date_claimed > date_of_service + INTERVAL '30 days' THEN 'DELAYED_CLAIM'
        WHEN approved_amount IS NULL THEN 'MISSING_APPROVAL'
        ELSE 'OK'
    END as quality_issue
FROM claims
WHERE claim_amount IS NULL
   OR date_claimed < date_of_service
   OR date_claimed > date_of_service + INTERVAL '30 days'
   OR approved_amount IS NULL
LIMIT 50;