/*
 * test_golden_rules.sql
 * E2E validation of MDM golden records, DQ scoring, and SCD2 history
 * Tests both AI and FUZZY pipelines independently
 */

USE DATABASE MDM_DEV;
USE SCHEMA MDM_AGG_v001;
USE WAREHOUSE MD_TEST_WH;

-- =============================================================================
-- AI PIPELINE TESTS
-- =============================================================================

-- =============================================================================
-- TEST 1A: AI PIPELINE METRICS — Record counts, merge rate, DQ averages
-- =============================================================================

SELECT
    'Source records' AS metric, (SELECT COUNT(*) FROM CRMA_AGG_VW_CUSTOMER_UNION)::VARCHAR AS value, 'INFO' AS status
UNION ALL SELECT 'AI: Golden records', (SELECT COUNT(*) FROM CRMA_AGG_DT_CUSTOMER_AI)::VARCHAR, 'INFO'
UNION ALL SELECT 'AI: Golden addresses', (SELECT COUNT(*) FROM CRMA_AGG_DT_ADDRESSES_AI)::VARCHAR, 'INFO'
UNION ALL SELECT 'AI: Merged (2+ sources)', (SELECT COUNT(*) FROM CRMA_AGG_DT_CUSTOMER_AI WHERE source_count >= 2)::VARCHAR, 'INFO'
UNION ALL SELECT 'AI: Merge rate %', ROUND((1 - (SELECT COUNT(*) FROM CRMA_AGG_DT_CUSTOMER_AI)::FLOAT / NULLIF((SELECT COUNT(*) FROM CRMA_AGG_VW_CUSTOMER_UNION), 0)) * 100, 1)::VARCHAR, 'INFO'
UNION ALL SELECT 'AI: Avg DQ score', (SELECT AVG(dq_score)::NUMBER(5,1) FROM CRMA_AGG_DT_CUSTOMER_AI)::VARCHAR, 'INFO'
UNION ALL SELECT 'AI: DQ Excellent (>=90)', (SELECT COUNT(*) FROM CRMA_AGG_DT_CUSTOMER_AI WHERE dq_score >= 90)::VARCHAR, 'INFO';

-- =============================================================================
-- TEST 2A: AI DQ TIER DISTRIBUTION
-- =============================================================================

SELECT
    CASE
        WHEN DQ_SCORE >= 90 THEN 'Excellent (90-100)'
        WHEN DQ_SCORE >= 70 THEN 'Good (70-89)'
        WHEN DQ_SCORE >= 50 THEN 'Fair (50-69)'
        ELSE 'Poor (0-49)'
    END AS DQ_TIER,
    COUNT(*) AS RECORD_COUNT,
    MIN(DQ_SCORE) AS MIN_SCORE,
    MAX(DQ_SCORE) AS MAX_SCORE,
    AVG(DQ_SCORE)::NUMBER(5, 1) AS AVG_SCORE
FROM CRMA_AGG_DT_CUSTOMER_AI
GROUP BY DQ_TIER
ORDER BY MIN_SCORE DESC;

-- =============================================================================
-- TEST 3A: AI DQ SCORE BOUNDS — No score outside 0-100
-- =============================================================================

SELECT 'AI: DQ score out of bounds' AS check_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM CRMA_AGG_DT_CUSTOMER_AI
WHERE NOT (DQ_SCORE BETWEEN 0 AND 100);

-- =============================================================================
-- TEST 4A: AI DQ RULE SPOT CHECKS
-- =============================================================================

SELECT 'DQ-001: Invalid email' AS rule, COUNT(*) AS affected, AVG(dq_score)::NUMBER(5,1) AS avg_dq
FROM CRMA_AGG_DT_CUSTOMER_AI
WHERE email IS NULL OR NOT RLIKE(email, '^[A-Za-z0-9._%+\\055]+@[A-Za-z0-9.\\055]+\\.[A-Za-z]{2,}$')
UNION ALL
SELECT 'DQ-003: Missing first_name', COUNT(*), AVG(dq_score)::NUMBER(5,1)
FROM CRMA_AGG_DT_CUSTOMER_AI WHERE first_name IS NULL OR LENGTH(TRIM(first_name)) <= 1
UNION ALL
SELECT 'DQ-005: Missing last_name', COUNT(*), AVG(dq_score)::NUMBER(5,1)
FROM CRMA_AGG_DT_CUSTOMER_AI WHERE last_name IS NULL OR LENGTH(TRIM(last_name)) <= 1
UNION ALL
SELECT 'DQ-008: Placeholder phone', COUNT(*), AVG(dq_score)::NUMBER(5,1)
FROM CRMA_AGG_DT_CUSTOMER_AI WHERE REGEXP_REPLACE(phone, '[^0-9]', '') IN ('0000000000','1111111111','1234567890')
UNION ALL
SELECT 'DQ-C01: No contact method', COUNT(*), AVG(dq_score)::NUMBER(5,1)
FROM CRMA_AGG_DT_CUSTOMER_AI
WHERE (email IS NULL OR NOT RLIKE(email, '^[A-Za-z0-9._%+\\055]+@[A-Za-z0-9.\\055]+\\.[A-Za-z]{2,}$'))
  AND (phone IS NULL OR LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '')) < 7);

-- =============================================================================
-- TEST 5A: AI SOURCE COUNT DISTRIBUTION
-- =============================================================================

SELECT
    SOURCE_COUNT,
    COUNT(*) AS CUSTOMERS,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS PCT
FROM CRMA_AGG_DT_CUSTOMER_AI
GROUP BY SOURCE_COUNT
ORDER BY SOURCE_COUNT ASC;

-- =============================================================================
-- TEST 6A: AI ADDRESS 1:1 CHECK
-- =============================================================================

SELECT 'AI: Customers without address' AS check_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM CRMA_AGG_DT_CUSTOMER_AI c
WHERE NOT EXISTS (SELECT 1 FROM CRMA_AGG_DT_ADDRESSES_AI a WHERE a.customer_id = c.customer_id)
UNION ALL
SELECT 'AI: Customers with multiple addresses',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM (SELECT customer_id FROM CRMA_AGG_DT_ADDRESSES_AI GROUP BY customer_id HAVING COUNT(*) > 1);

-- =============================================================================
-- TEST 7A: AI SCD2 CUSTOMER HISTORY
-- =============================================================================

SELECT 'AI: Total history rows' AS metric, COUNT(*)::VARCHAR AS value FROM CRMA_AGG_DT_CUSTOMER_HISTORY_AI
UNION ALL SELECT 'AI: Distinct customers', COUNT(DISTINCT customer_id)::VARCHAR FROM CRMA_AGG_DT_CUSTOMER_HISTORY_AI
UNION ALL SELECT 'AI: Current (IS_VALID=TRUE)', COUNT(*)::VARCHAR FROM CRMA_AGG_DT_CUSTOMER_HISTORY_AI WHERE is_valid = TRUE
UNION ALL SELECT 'AI: Historical (IS_VALID=FALSE)', COUNT(*)::VARCHAR FROM CRMA_AGG_DT_CUSTOMER_HISTORY_AI WHERE is_valid = FALSE;

-- =============================================================================
-- TEST 8A: AI SCD2 CUSTOMER INTEGRITY
-- =============================================================================

SELECT 'AI: Customers missing from history' AS check_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM CRMA_AGG_DT_CUSTOMER_AI c
WHERE NOT EXISTS (SELECT 1 FROM CRMA_AGG_DT_CUSTOMER_HISTORY_AI h WHERE h.customer_id = c.customer_id AND h.is_valid = TRUE)
UNION ALL
SELECT 'AI: Customers with multiple valid rows',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM (SELECT customer_id FROM CRMA_AGG_DT_CUSTOMER_HISTORY_AI WHERE is_valid = TRUE GROUP BY customer_id HAVING COUNT(*) > 1)
UNION ALL
SELECT 'AI: Historical rows with VALID_TO = 9999-12-31',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM CRMA_AGG_DT_CUSTOMER_HISTORY_AI
WHERE is_valid = FALSE AND valid_to = '9999-12-31'::TIMESTAMP_LTZ;

-- =============================================================================
-- TEST 9A: AI SCD2 ADDRESS HISTORY
-- =============================================================================

SELECT 'AI: Address history rows' AS metric, COUNT(*)::VARCHAR AS value FROM CRMA_AGG_DT_ADDRESSES_HISTORY_AI
UNION ALL SELECT 'AI: Current (IS_VALID=TRUE)', COUNT(*)::VARCHAR FROM CRMA_AGG_DT_ADDRESSES_HISTORY_AI WHERE is_valid = TRUE
UNION ALL SELECT 'AI: Historical (IS_VALID=FALSE)', COUNT(*)::VARCHAR FROM CRMA_AGG_DT_ADDRESSES_HISTORY_AI WHERE is_valid = FALSE;

SELECT 'AI: Addresses missing from history' AS check_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM CRMA_AGG_DT_ADDRESSES_AI a
WHERE NOT EXISTS (SELECT 1 FROM CRMA_AGG_DT_ADDRESSES_HISTORY_AI h WHERE h.customer_id = a.customer_id AND h.is_valid = TRUE)
UNION ALL
SELECT 'AI: Addresses with multiple valid rows',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM (SELECT customer_id FROM CRMA_AGG_DT_ADDRESSES_HISTORY_AI WHERE is_valid = TRUE GROUP BY customer_id HAVING COUNT(*) > 1);

-- =============================================================================
-- TEST 10A: AI GOLDEN RECORD COMPLETENESS
-- =============================================================================

SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN first_name IS NULL THEN 1 ELSE 0 END) AS null_first_name,
    SUM(CASE WHEN last_name IS NULL THEN 1 ELSE 0 END) AS null_last_name,
    SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) AS null_email,
    SUM(CASE WHEN phone IS NULL THEN 1 ELSE 0 END) AS null_phone,
    ROUND(SUM(CASE WHEN first_name IS NOT NULL AND last_name IS NOT NULL AND email IS NOT NULL AND phone IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_complete
FROM CRMA_AGG_DT_CUSTOMER_AI;

-- =============================================================================
-- TEST 11A: AI SERVING LAYER
-- =============================================================================

WITH GOLDEN AS (SELECT COUNT(*) AS CNT FROM CRMA_AGG_DT_CUSTOMER_AI),

SERVING AS (SELECT COUNT(*) AS CNT FROM MDM_SRV_V001.CRMS_AGG_VW_CUSTOMER_360_AI)

SELECT
    'AI: Serving view count matches golden' AS CHECK_NAME,
    CASE WHEN GOLDEN.CNT = SERVING.CNT THEN 'PASS' ELSE 'FAIL: DT=' || GOLDEN.CNT || ' SRV=' || SERVING.CNT END AS CHECK_RESULT
FROM GOLDEN
CROSS JOIN SERVING;

-- =============================================================================
-- FUZZY PIPELINE TESTS
-- =============================================================================

-- =============================================================================
-- TEST 1B: FUZZY PIPELINE METRICS
-- =============================================================================

SELECT
    'FUZZY: Golden records' AS metric, (SELECT COUNT(*) FROM CRMA_AGG_DT_CUSTOMER_FUZZY)::VARCHAR AS value, 'INFO' AS status
UNION ALL SELECT 'FUZZY: Golden addresses', (SELECT COUNT(*) FROM CRMA_AGG_DT_ADDRESSES_FUZZY)::VARCHAR, 'INFO'
UNION ALL SELECT 'FUZZY: Merged (2+ sources)', (SELECT COUNT(*) FROM CRMA_AGG_DT_CUSTOMER_FUZZY WHERE source_count >= 2)::VARCHAR, 'INFO'
UNION ALL SELECT 'FUZZY: Merge rate %', ROUND((1 - (SELECT COUNT(*) FROM CRMA_AGG_DT_CUSTOMER_FUZZY)::FLOAT / NULLIF((SELECT COUNT(*) FROM CRMA_AGG_VW_CUSTOMER_UNION), 0)) * 100, 1)::VARCHAR, 'INFO'
UNION ALL SELECT 'FUZZY: Avg DQ score', (SELECT AVG(dq_score)::NUMBER(5,1) FROM CRMA_AGG_DT_CUSTOMER_FUZZY)::VARCHAR, 'INFO'
UNION ALL SELECT 'FUZZY: DQ Excellent (>=90)', (SELECT COUNT(*) FROM CRMA_AGG_DT_CUSTOMER_FUZZY WHERE dq_score >= 90)::VARCHAR, 'INFO';

-- =============================================================================
-- TEST 2B: FUZZY DQ TIER DISTRIBUTION
-- =============================================================================

SELECT
    CASE
        WHEN DQ_SCORE >= 90 THEN 'Excellent (90-100)'
        WHEN DQ_SCORE >= 70 THEN 'Good (70-89)'
        WHEN DQ_SCORE >= 50 THEN 'Fair (50-69)'
        ELSE 'Poor (0-49)'
    END AS DQ_TIER,
    COUNT(*) AS RECORD_COUNT,
    MIN(DQ_SCORE) AS MIN_SCORE,
    MAX(DQ_SCORE) AS MAX_SCORE,
    AVG(DQ_SCORE)::NUMBER(5, 1) AS AVG_SCORE
FROM CRMA_AGG_DT_CUSTOMER_FUZZY
GROUP BY DQ_TIER
ORDER BY MIN_SCORE DESC;

-- =============================================================================
-- TEST 3B: FUZZY DQ SCORE BOUNDS
-- =============================================================================

SELECT 'FUZZY: DQ score out of bounds' AS check_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM CRMA_AGG_DT_CUSTOMER_FUZZY
WHERE NOT (DQ_SCORE BETWEEN 0 AND 100);

-- =============================================================================
-- TEST 5B: FUZZY SOURCE COUNT DISTRIBUTION
-- =============================================================================

SELECT
    SOURCE_COUNT,
    COUNT(*) AS CUSTOMERS,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS PCT
FROM CRMA_AGG_DT_CUSTOMER_FUZZY
GROUP BY SOURCE_COUNT
ORDER BY SOURCE_COUNT ASC;

-- =============================================================================
-- TEST 6B: FUZZY ADDRESS 1:1 CHECK
-- =============================================================================

SELECT 'FUZZY: Customers without address' AS check_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM CRMA_AGG_DT_CUSTOMER_FUZZY c
WHERE NOT EXISTS (SELECT 1 FROM CRMA_AGG_DT_ADDRESSES_FUZZY a WHERE a.customer_id = c.customer_id)
UNION ALL
SELECT 'FUZZY: Customers with multiple addresses',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM (SELECT customer_id FROM CRMA_AGG_DT_ADDRESSES_FUZZY GROUP BY customer_id HAVING COUNT(*) > 1);

-- =============================================================================
-- TEST 7B: FUZZY SCD2 CUSTOMER HISTORY
-- =============================================================================

SELECT 'FUZZY: Total history rows' AS metric, COUNT(*)::VARCHAR AS value FROM CRMA_AGG_DT_CUSTOMER_HISTORY_FUZZY
UNION ALL SELECT 'FUZZY: Distinct customers', COUNT(DISTINCT customer_id)::VARCHAR FROM CRMA_AGG_DT_CUSTOMER_HISTORY_FUZZY
UNION ALL SELECT 'FUZZY: Current (IS_VALID=TRUE)', COUNT(*)::VARCHAR FROM CRMA_AGG_DT_CUSTOMER_HISTORY_FUZZY WHERE is_valid = TRUE
UNION ALL SELECT 'FUZZY: Historical (IS_VALID=FALSE)', COUNT(*)::VARCHAR FROM CRMA_AGG_DT_CUSTOMER_HISTORY_FUZZY WHERE is_valid = FALSE;

-- =============================================================================
-- TEST 8B: FUZZY SCD2 CUSTOMER INTEGRITY
-- =============================================================================

SELECT 'FUZZY: Customers missing from history' AS check_name,
    COUNT(*) AS violations,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM CRMA_AGG_DT_CUSTOMER_FUZZY c
WHERE NOT EXISTS (SELECT 1 FROM CRMA_AGG_DT_CUSTOMER_HISTORY_FUZZY h WHERE h.customer_id = c.customer_id AND h.is_valid = TRUE)
UNION ALL
SELECT 'FUZZY: Customers with multiple valid rows',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM (SELECT customer_id FROM CRMA_AGG_DT_CUSTOMER_HISTORY_FUZZY WHERE is_valid = TRUE GROUP BY customer_id HAVING COUNT(*) > 1)
UNION ALL
SELECT 'FUZZY: Historical rows with VALID_TO = 9999-12-31',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
FROM CRMA_AGG_DT_CUSTOMER_HISTORY_FUZZY
WHERE is_valid = FALSE AND valid_to = '9999-12-31'::TIMESTAMP_LTZ;

-- =============================================================================
-- TEST 9B: FUZZY SCD2 ADDRESS HISTORY
-- =============================================================================

SELECT 'FUZZY: Address history rows' AS metric, COUNT(*)::VARCHAR AS value FROM CRMA_AGG_DT_ADDRESSES_HISTORY_FUZZY
UNION ALL SELECT 'FUZZY: Current (IS_VALID=TRUE)', COUNT(*)::VARCHAR FROM CRMA_AGG_DT_ADDRESSES_HISTORY_FUZZY WHERE is_valid = TRUE
UNION ALL SELECT 'FUZZY: Historical (IS_VALID=FALSE)', COUNT(*)::VARCHAR FROM CRMA_AGG_DT_ADDRESSES_HISTORY_FUZZY WHERE is_valid = FALSE;

-- =============================================================================
-- TEST 10B: FUZZY GOLDEN RECORD COMPLETENESS
-- =============================================================================

SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN first_name IS NULL THEN 1 ELSE 0 END) AS null_first_name,
    SUM(CASE WHEN last_name IS NULL THEN 1 ELSE 0 END) AS null_last_name,
    SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) AS null_email,
    SUM(CASE WHEN phone IS NULL THEN 1 ELSE 0 END) AS null_phone,
    ROUND(SUM(CASE WHEN first_name IS NOT NULL AND last_name IS NOT NULL AND email IS NOT NULL AND phone IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_complete
FROM CRMA_AGG_DT_CUSTOMER_FUZZY;

-- =============================================================================
-- TEST 11B: FUZZY SERVING LAYER
-- =============================================================================

WITH GOLDEN AS (SELECT COUNT(*) AS CNT FROM CRMA_AGG_DT_CUSTOMER_FUZZY),

SERVING AS (SELECT COUNT(*) AS CNT FROM MDM_SRV_V001.CRMS_AGG_VW_CUSTOMER_360_FUZZY)

SELECT
    'FUZZY: Serving view count matches golden' AS CHECK_NAME,
    CASE WHEN GOLDEN.CNT = SERVING.CNT THEN 'PASS' ELSE 'FAIL: DT=' || GOLDEN.CNT || ' SRV=' || SERVING.CNT END AS CHECK_RESULT
FROM GOLDEN
CROSS JOIN SERVING;

-- =============================================================================
-- COMPARISON: AI vs FUZZY pipeline outcomes
-- =============================================================================

SELECT
    'AI' AS pipeline, COUNT(*) AS golden_records,
    SUM(CASE WHEN source_count >= 2 THEN 1 ELSE 0 END) AS merged,
    ROUND((1 - COUNT(*)::FLOAT / 1500) * 100, 1) AS merge_rate_pct,
    AVG(dq_score)::NUMBER(5,1) AS avg_dq
FROM CRMA_AGG_DT_CUSTOMER_AI
UNION ALL
SELECT
    'FUZZY', COUNT(*),
    SUM(CASE WHEN source_count >= 2 THEN 1 ELSE 0 END),
    ROUND((1 - COUNT(*)::FLOAT / 1500) * 100, 1),
    AVG(dq_score)::NUMBER(5,1)
FROM CRMA_AGG_DT_CUSTOMER_FUZZY;
