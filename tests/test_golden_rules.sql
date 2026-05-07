-- =============================================================================
-- test_golden_rules.sql
-- E2E validation of MDM golden records, DQ scoring, and SCD2 history
-- =============================================================================
-- All queries use materialized DTs (DT_CUSTOMER, DT_ADDRESSES, DT_*_HISTORY)
-- and RAW union views (VW_CUSTOMER_UNION, VW_ADDRESSES_UNION) to avoid
-- triggering Cortex AI calls through VW_CUSTOMER_ENRICHED.
-- =============================================================================

USE DATABASE MDM_DEV;
USE SCHEMA MDM_AGG_001;
USE WAREHOUSE MD_TEST_WH;

-- =============================================================================
-- TEST 1: PIPELINE METRICS — Record counts, merge rate, DQ averages
-- =============================================================================

SELECT
    'Source records' AS metric, (SELECT COUNT(*) FROM CRMA_AGG_VW_CUSTOMER_UNION)::VARCHAR AS value, 'INFO' AS status
UNION ALL SELECT 'Golden records', (SELECT COUNT(*) FROM CRMA_AGG_DT_CUSTOMER)::VARCHAR, 'INFO'
UNION ALL SELECT 'Golden addresses', (SELECT COUNT(*) FROM CRMA_AGG_DT_ADDRESSES)::VARCHAR, 'INFO'
UNION ALL SELECT 'Merged (2+ sources)', (SELECT COUNT(*) FROM CRMA_AGG_DT_CUSTOMER WHERE source_count >= 2)::VARCHAR, 'INFO'
UNION ALL SELECT 'Merge rate %', ROUND((1 - (SELECT COUNT(*) FROM CRMA_AGG_DT_CUSTOMER)::FLOAT / NULLIF((SELECT COUNT(*) FROM CRMA_AGG_VW_CUSTOMER_UNION), 0)) * 100, 1)::VARCHAR, 'INFO'
UNION ALL SELECT 'Avg DQ score', (SELECT AVG(dq_score)::NUMBER(5,1) FROM CRMA_AGG_DT_CUSTOMER)::VARCHAR, 'INFO'
UNION ALL SELECT 'DQ Excellent (>=90)', (SELECT COUNT(*) FROM CRMA_AGG_DT_CUSTOMER WHERE dq_score >= 90)::VARCHAR, 'INFO';

-- =============================================================================
-- TEST 2: DQ TIER DISTRIBUTION
-- =============================================================================

SELECT
    CASE WHEN dq_score >= 90 THEN 'Excellent (90-100)'
         WHEN dq_score >= 70 THEN 'Good (70-89)'
         WHEN dq_score >= 50 THEN 'Fair (50-69)'
         ELSE 'Poor (0-49)' END AS dq_tier,
    COUNT(*) AS record_count,
    MIN(dq_score) AS min_score,
    MAX(dq_score) AS max_score,
    AVG(dq_score)::NUMBER(5,1) AS avg_score
FROM CRMA_AGG_DT_CUSTOMER
GROUP BY 1 ORDER BY min_score DESC;

-- =============================================================================
-- TEST 3: DQ SCORE BOUNDS — No score outside 0-100
-- =============================================================================

SELECT 'DQ score out of bounds' AS check_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM CRMA_AGG_DT_CUSTOMER
WHERE dq_score < 0 OR dq_score > 100;

-- =============================================================================
-- TEST 4: DQ RULE SPOT CHECKS — Verify penalties on materialized golden records
-- =============================================================================

SELECT 'DQ-001: Invalid email' AS rule, COUNT(*) AS affected, AVG(dq_score)::NUMBER(5,1) AS avg_dq
FROM CRMA_AGG_DT_CUSTOMER
WHERE email IS NULL OR NOT RLIKE(email, '^[A-Za-z0-9._%+\\055]+@[A-Za-z0-9.\\055]+\\.[A-Za-z]{2,}$')
UNION ALL
SELECT 'DQ-003: Missing first_name', COUNT(*), AVG(dq_score)::NUMBER(5,1)
FROM CRMA_AGG_DT_CUSTOMER WHERE first_name IS NULL OR LENGTH(TRIM(first_name)) <= 1
UNION ALL
SELECT 'DQ-005: Missing last_name', COUNT(*), AVG(dq_score)::NUMBER(5,1)
FROM CRMA_AGG_DT_CUSTOMER WHERE last_name IS NULL OR LENGTH(TRIM(last_name)) <= 1
UNION ALL
SELECT 'DQ-008: Placeholder phone', COUNT(*), AVG(dq_score)::NUMBER(5,1)
FROM CRMA_AGG_DT_CUSTOMER WHERE REGEXP_REPLACE(phone, '[^0-9]', '') IN ('0000000000','1111111111','1234567890')
UNION ALL
SELECT 'DQ-C01: No contact method', COUNT(*), AVG(dq_score)::NUMBER(5,1)
FROM CRMA_AGG_DT_CUSTOMER
WHERE (email IS NULL OR NOT RLIKE(email, '^[A-Za-z0-9._%+\\055]+@[A-Za-z0-9.\\055]+\\.[A-Za-z]{2,}$'))
  AND (phone IS NULL OR LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '')) < 7);

-- =============================================================================
-- TEST 5: SOURCE COUNT DISTRIBUTION — Verify 1/2/3 source merge counts
-- =============================================================================

SELECT source_count, COUNT(*) AS customers,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
FROM CRMA_AGG_DT_CUSTOMER
GROUP BY source_count ORDER BY source_count;

-- =============================================================================
-- TEST 6: ADDRESS 1:1 CHECK — Every customer has exactly 1 golden address
-- =============================================================================

SELECT 'Customers without address' AS check_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM CRMA_AGG_DT_CUSTOMER c
WHERE NOT EXISTS (SELECT 1 FROM CRMA_AGG_DT_ADDRESSES a WHERE a.customer_id = c.customer_id)
UNION ALL
SELECT 'Customers with multiple addresses',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM (SELECT customer_id FROM CRMA_AGG_DT_ADDRESSES GROUP BY customer_id HAVING COUNT(*) > 1);

-- =============================================================================
-- TEST 7: SCD2 CUSTOMER HISTORY — Structure and row counts
-- =============================================================================

SELECT 'Total history rows' AS metric, COUNT(*)::VARCHAR AS value FROM CRMA_AGG_DT_CUSTOMER_HISTORY
UNION ALL SELECT 'Distinct customers', COUNT(DISTINCT customer_id)::VARCHAR FROM CRMA_AGG_DT_CUSTOMER_HISTORY
UNION ALL SELECT 'Current (IS_VALID=TRUE)', COUNT(*)::VARCHAR FROM CRMA_AGG_DT_CUSTOMER_HISTORY WHERE is_valid = TRUE
UNION ALL SELECT 'Historical (IS_VALID=FALSE)', COUNT(*)::VARCHAR FROM CRMA_AGG_DT_CUSTOMER_HISTORY WHERE is_valid = FALSE;

-- =============================================================================
-- TEST 8: SCD2 CUSTOMER INTEGRITY — 1 valid row per customer, no orphans
-- =============================================================================

SELECT 'Customers missing from history' AS check_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM CRMA_AGG_DT_CUSTOMER c
WHERE NOT EXISTS (SELECT 1 FROM CRMA_AGG_DT_CUSTOMER_HISTORY h WHERE h.customer_id = c.customer_id AND h.is_valid = TRUE)
UNION ALL
SELECT 'Customers with multiple valid rows',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM (SELECT customer_id FROM CRMA_AGG_DT_CUSTOMER_HISTORY WHERE is_valid = TRUE GROUP BY customer_id HAVING COUNT(*) > 1)
UNION ALL
SELECT 'Historical rows with VALID_TO = 9999-12-31',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM CRMA_AGG_DT_CUSTOMER_HISTORY
WHERE is_valid = FALSE AND valid_to = '9999-12-31'::TIMESTAMP_LTZ;

-- =============================================================================
-- TEST 9: SCD2 ADDRESS HISTORY — Structure and integrity
-- =============================================================================

SELECT 'Address history rows' AS metric, COUNT(*)::VARCHAR AS value FROM CRMA_AGG_DT_ADDRESSES_HISTORY
UNION ALL SELECT 'Current (IS_VALID=TRUE)', COUNT(*)::VARCHAR FROM CRMA_AGG_DT_ADDRESSES_HISTORY WHERE is_valid = TRUE
UNION ALL SELECT 'Historical (IS_VALID=FALSE)', COUNT(*)::VARCHAR FROM CRMA_AGG_DT_ADDRESSES_HISTORY WHERE is_valid = FALSE;

SELECT 'Addresses missing from history' AS check_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM CRMA_AGG_DT_ADDRESSES a
WHERE NOT EXISTS (SELECT 1 FROM CRMA_AGG_DT_ADDRESSES_HISTORY h WHERE h.customer_id = a.customer_id AND h.is_valid = TRUE)
UNION ALL
SELECT 'Addresses with multiple valid rows',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM (SELECT customer_id FROM CRMA_AGG_DT_ADDRESSES_HISTORY WHERE is_valid = TRUE GROUP BY customer_id HAVING COUNT(*) > 1);

-- =============================================================================
-- TEST 10: GOLDEN RECORD COMPLETENESS — NULL rates on key fields
-- =============================================================================

SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN first_name IS NULL THEN 1 ELSE 0 END) AS null_first_name,
    SUM(CASE WHEN last_name IS NULL THEN 1 ELSE 0 END) AS null_last_name,
    SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) AS null_email,
    SUM(CASE WHEN phone IS NULL THEN 1 ELSE 0 END) AS null_phone,
    ROUND(SUM(CASE WHEN first_name IS NOT NULL AND last_name IS NOT NULL AND email IS NOT NULL AND phone IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_complete
FROM CRMA_AGG_DT_CUSTOMER;

-- =============================================================================
-- TEST 11: SERVING LAYER — VW_CUSTOMER_360 row count matches DT_CUSTOMER
-- =============================================================================

SELECT
    'Serving view count matches golden' AS check_name,
    CASE WHEN c.cnt = s.cnt THEN 'PASS' ELSE 'FAIL: DT=' || c.cnt || ' SRV=' || s.cnt END AS result
FROM (SELECT COUNT(*) AS cnt FROM CRMA_AGG_DT_CUSTOMER) c,
     (SELECT COUNT(*) AS cnt FROM MDM_SRV_001.CRMS_AGG_VW_CUSTOMER_360) s;
