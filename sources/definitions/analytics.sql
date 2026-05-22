-- =============================================================================
-- analytics.sql — Full Dynamic Table chain for customer + address MDM pipeline
-- Two isolated implementations: AI (Cortex-powered) and FUZZY (classical matching)
-- =============================================================================

-- =============================================================================
-- AI PIPELINE — Uses Cortex AI for nickname resolution and fake name detection
-- =============================================================================

DEFINE DYNAMIC TABLE {{db}}.{{agg_schema}}.CRMA_AGG_DT_CUSTOMER_ENRICHED_AI
    WAREHOUSE = {{warehouse}}
    TARGET_LAG = '{{dt_lag}}'
    REFRESH_MODE = FULL
    COMMENT = 'Cortex AI enrichment (materialized). Cortex calls run once per refresh only.'
AS
SELECT
    source_system, source_key, first_name, last_name,
    CASE WHEN first_name IS NOT NULL AND LENGTH(TRIM(first_name)) > 1
        THEN INITCAP(TRIM(SNOWFLAKE.CORTEX.COMPLETE('mistral-large2',
            'Return ONLY the canonical/formal first name. Just the name, nothing else. Name: ' || first_name)))
        ELSE first_name
    END AS canonical_first_name,
    CASE WHEN first_name IS NOT NULL AND last_name IS NOT NULL
             AND LENGTH(TRIM(first_name)) > 0 AND LENGTH(TRIM(last_name)) > 0
        THEN CAST(SNOWFLAKE.CORTEX.AI_CLASSIFY(TRIM(first_name) || ' ' || TRIM(last_name),
            ['real_person_name', 'fake_or_test_name']) AS VARIANT):label::VARCHAR = 'fake_or_test_name'
        ELSE FALSE
    END AS is_fake_name,
    email, phone, file_date, row_timestamp
FROM {{db}}.{{agg_schema}}.CRMA_AGG_VW_CUSTOMER_UNION;

DEFINE DYNAMIC TABLE {{db}}.{{agg_schema}}.CRMA_AGG_DT_CUSTOMER_GROUPS_AI
    WAREHOUSE = {{warehouse}}
    TARGET_LAG = '{{dt_lag}}'
    REFRESH_MODE = FULL
    COMMENT = 'Entity resolution with blocking (BLOCK-01..04). Threshold >= 0.70. AI pipeline.'
AS
WITH base AS (
    SELECT DISTINCT
        e.source_system, e.source_key, e.first_name, e.last_name,
        e.canonical_first_name, e.is_fake_name, e.email, e.phone,
        a.street, a.city, a.postal_code,
        SOUNDEX(e.last_name) AS block_soundex,
        CASE WHEN e.email IS NOT NULL AND POSITION('@' IN e.email) > 0
             THEN SUBSTR(e.email, POSITION('@' IN e.email)) ELSE NULL END AS block_email_domain,
        CASE WHEN LENGTH(REGEXP_REPLACE(e.phone, '[^0-9]', '')) >= 4
             THEN RIGHT(REGEXP_REPLACE(e.phone, '[^0-9]', ''), 4) ELSE NULL END AS block_phone_suffix
    FROM {{db}}.{{agg_schema}}.CRMA_AGG_DT_CUSTOMER_ENRICHED_AI e
    LEFT JOIN {{db}}.{{agg_schema}}.CRMA_AGG_VW_ADDRESSES_UNION a
        ON e.source_system = a.source_system AND e.source_key = a.source_customer_key
),
blocked_pairs AS (
    SELECT DISTINCT
        a.source_system AS source_a, a.source_key AS key_a,
        b.source_system AS source_b, b.source_key AS key_b,
        a.canonical_first_name AS fn_a, a.last_name AS ln_a, a.email AS email_a, a.phone AS phone_a,
        a.street AS street_a, a.city AS city_a, a.postal_code AS postal_a,
        b.canonical_first_name AS fn_b, b.last_name AS ln_b, b.email AS email_b, b.phone AS phone_b,
        b.street AS street_b, b.city AS city_b, b.postal_code AS postal_b
    FROM base a JOIN base b
        ON (a.source_system < b.source_system OR (a.source_system = b.source_system AND a.source_key < b.source_key))
        AND ((a.block_soundex IS NOT NULL AND a.block_soundex = b.block_soundex)
            OR (a.block_email_domain IS NOT NULL AND a.block_email_domain = b.block_email_domain)
            OR (a.block_phone_suffix IS NOT NULL AND a.block_phone_suffix = b.block_phone_suffix))
),
match_pairs AS (
    SELECT source_a, key_a, source_b, key_b,
        CASE WHEN email_a IS NOT NULL AND email_a = email_b THEN 1.0 ELSE 0 END AS email_match,
        CASE WHEN LENGTH(phone_a) >= 10 AND LENGTH(phone_b) >= 10 AND RIGHT(phone_a, 10) = RIGHT(phone_b, 10) THEN 0.95 ELSE 0 END AS phone_match,
        CASE WHEN fn_a IS NOT NULL AND fn_b IS NOT NULL AND ln_a IS NOT NULL AND ln_b IS NOT NULL
             AND JAROWINKLER_SIMILARITY(CONCAT(fn_a, ' ', ln_a), CONCAT(fn_b, ' ', ln_b)) >= 85
             THEN JAROWINKLER_SIMILARITY(CONCAT(fn_a, ' ', ln_a), CONCAT(fn_b, ' ', ln_b)) / 100.0 * 0.30 ELSE 0 END AS name_similarity,
        CASE WHEN SOUNDEX(ln_a) = SOUNDEX(ln_b) THEN 0.20 ELSE 0 END AS soundex_match,
        CASE WHEN fn_a IS NOT NULL AND fn_b IS NOT NULL AND ln_a IS NOT NULL AND ln_b IS NOT NULL
             AND LOWER(TRIM(fn_a)) = LOWER(TRIM(fn_b)) AND LOWER(TRIM(ln_a)) = LOWER(TRIM(ln_b)) THEN 0.80 ELSE 0 END AS canonical_exact_match,
        CASE WHEN street_a IS NOT NULL AND street_b IS NOT NULL AND postal_a IS NOT NULL AND postal_b IS NOT NULL
             AND JAROWINKLER_SIMILARITY(street_a, street_b) >= 80 AND postal_a = postal_b THEN 0.25 ELSE 0 END AS address_similarity,
        CASE WHEN email_a IS NOT NULL AND email_b IS NOT NULL AND POSITION('@' IN email_a) > 0 AND POSITION('@' IN email_b) > 0
             AND SUBSTR(email_a, POSITION('@' IN email_a)) = SUBSTR(email_b, POSITION('@' IN email_b))
             AND fn_a IS NOT NULL AND fn_b IS NOT NULL AND JAROWINKLER_SIMILARITY(LOWER(TRIM(fn_a)), LOWER(TRIM(fn_b))) >= 90
             THEN 0.15 ELSE 0 END AS email_domain_name_match,
        CASE WHEN LENGTH(REGEXP_REPLACE(phone_a, '[^0-9]', '')) >= 7 AND LENGTH(REGEXP_REPLACE(phone_b, '[^0-9]', '')) >= 7
             AND RIGHT(REGEXP_REPLACE(phone_a, '[^0-9]', ''), 7) = RIGHT(REGEXP_REPLACE(phone_b, '[^0-9]', ''), 7)
             AND city_a IS NOT NULL AND city_b IS NOT NULL AND LOWER(TRIM(city_a)) = LOWER(TRIM(city_b))
             THEN 0.10 ELSE 0 END AS phone_partial_city_match
    FROM blocked_pairs
),
matches AS (
    SELECT source_a, key_a, source_b, key_b FROM match_pairs
    WHERE GREATEST(email_match, phone_match, canonical_exact_match) + name_similarity + soundex_match + address_similarity + email_domain_name_match + phone_partial_city_match >= 0.70
),
matched_clusters AS (
    SELECT b.source_system, b.source_key, COALESCE(MIN(m.source_a || '|' || m.key_a), b.source_system || '|' || b.source_key) AS cluster_id
    FROM base b LEFT JOIN matches m ON (b.source_system = m.source_a AND b.source_key = m.key_a) OR (b.source_system = m.source_b AND b.source_key = m.key_b)
    GROUP BY b.source_system, b.source_key
)
SELECT DENSE_RANK() OVER (ORDER BY cluster_id) AS customer_id, source_system, source_key, cluster_id
FROM matched_clusters;

DEFINE DYNAMIC TABLE {{db}}.{{agg_schema}}.CRMA_AGG_DT_CUSTOMER_GOLDEN_AI
    WAREHOUSE = {{warehouse}}
    TARGET_LAG = '{{dt_lag}}'
    REFRESH_MODE = FULL
    COMMENT = 'Golden customer records with survivorship + DQ scoring. AI pipeline.'
AS
WITH grouped AS (
    SELECT g.customer_id, g.source_system, g.source_key,
        u.first_name, u.last_name, u.email, u.phone, u.file_date, u.row_timestamp, u.is_fake_name,
        CASE g.source_system WHEN 'CRM_A' THEN 1 WHEN 'CRM_B' THEN 2 ELSE 3 END AS source_priority
    FROM {{db}}.{{agg_schema}}.CRMA_AGG_DT_CUSTOMER_GROUPS_AI g
    JOIN {{db}}.{{agg_schema}}.CRMA_AGG_DT_CUSTOMER_ENRICHED_AI u ON g.source_system = u.source_system AND g.source_key = u.source_key
),
survivorship AS (
    SELECT customer_id, file_date, row_timestamp,
        FIRST_VALUE(first_name) OVER (PARTITION BY customer_id, file_date ORDER BY CASE WHEN LENGTH(TRIM(COALESCE(first_name, ''))) > 1 THEN 0 ELSE 1 END, source_priority, row_timestamp DESC) AS first_name,
        FIRST_VALUE(last_name) OVER (PARTITION BY customer_id, file_date ORDER BY CASE WHEN LENGTH(TRIM(COALESCE(last_name, ''))) > 1 THEN 0 ELSE 1 END, source_priority, row_timestamp DESC) AS last_name,
        FIRST_VALUE(email) OVER (PARTITION BY customer_id, file_date ORDER BY CASE WHEN email LIKE '%@%' THEN 0 ELSE 1 END, source_priority, row_timestamp DESC) AS email,
        FIRST_VALUE(phone) OVER (PARTITION BY customer_id, file_date ORDER BY CASE WHEN LENGTH(phone) >= 7 THEN 0 ELSE 1 END, source_priority, row_timestamp DESC) AS phone,
        COUNT(DISTINCT source_system) OVER (PARTITION BY customer_id) AS source_count,
        MAX(CASE WHEN is_fake_name THEN 1 ELSE 0 END) OVER (PARTITION BY customer_id) = 1 AS is_fake_name
    FROM grouped
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id, file_date ORDER BY source_priority) = 1
),
dq_rules AS (
    SELECT customer_id, first_name, last_name, email, phone, file_date, row_timestamp, source_count, is_fake_name,
        100
        + CASE WHEN email IS NULL OR NOT RLIKE(email, '^[A-Za-z0-9._%+\055]+@[A-Za-z0-9.\055]+\.[A-Za-z]{2,}$') THEN -20 ELSE 0 END
        + CASE WHEN email IS NOT NULL AND (LOWER(email) LIKE '%@mailinator.com' OR LOWER(email) LIKE '%@tempmail.com' OR LOWER(email) LIKE '%@guerrillamail.com' OR LOWER(email) LIKE '%@10minutemail.com') THEN -5 ELSE 0 END
        + CASE WHEN first_name IS NULL OR LENGTH(TRIM(first_name)) <= 1 THEN -20 ELSE 0 END
        + CASE WHEN first_name IS NOT NULL AND LENGTH(TRIM(first_name)) > 1 AND NOT RLIKE(first_name, '^[A-Za-z \'\055]+$') THEN -5 ELSE 0 END
        + CASE WHEN last_name IS NULL OR LENGTH(TRIM(last_name)) <= 1 THEN -20 ELSE 0 END
        + CASE WHEN last_name IS NOT NULL AND LENGTH(TRIM(last_name)) > 1 AND NOT RLIKE(last_name, '^[A-Za-z \'\055]+$') THEN -5 ELSE 0 END
        + CASE WHEN phone IS NOT NULL AND NOT RLIKE(REGEXP_REPLACE(phone, '[^0-9+]', ''), '^\+?[0-9]{10,15}$') THEN -5 ELSE 0 END
        + CASE WHEN phone IS NOT NULL AND REGEXP_REPLACE(phone, '[^0-9]', '') IN ('0000000000','1111111111','1234567890') THEN -20 ELSE 0 END
        + CASE WHEN (email IS NULL OR NOT RLIKE(email, '^[A-Za-z0-9._%+\055]+@[A-Za-z0-9.\055]+\.[A-Za-z]{2,}$')) AND (phone IS NULL OR LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '')) < 7) THEN -20 ELSE 0 END
        + CASE WHEN (first_name IS NULL OR LENGTH(TRIM(first_name)) <= 1) AND (last_name IS NULL OR LENGTH(TRIM(last_name)) <= 1) THEN -20 ELSE 0 END
        + CASE WHEN first_name IS NOT NULL AND LENGTH(TRIM(first_name)) > 1 AND email IS NOT NULL AND POSITION(LOWER(TRIM(first_name)) IN LOWER(email)) > 0 THEN 5 ELSE 0 END
        + CASE WHEN is_fake_name THEN -20 ELSE 0 END
        AS raw_dq_score
    FROM survivorship
)
SELECT customer_id, first_name, last_name, email, phone, file_date, row_timestamp, source_count,
    GREATEST(0, LEAST(100, raw_dq_score)) AS dq_score
FROM dq_rules;

DEFINE DYNAMIC TABLE {{db}}.{{agg_schema}}.CRMA_AGG_DT_CUSTOMER_AI
    WAREHOUSE = {{warehouse}}
    TARGET_LAG = '{{dt_lag}}'
    REFRESH_MODE = FULL
    COMMENT = 'Current golden customer records (latest version only). AI pipeline.'
AS
SELECT customer_id, first_name, last_name, email, phone, dq_score, source_count, row_timestamp AS last_updated
FROM {{db}}.{{agg_schema}}.CRMA_AGG_DT_CUSTOMER_GOLDEN_AI
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY file_date DESC, row_timestamp DESC) = 1;

DEFINE DYNAMIC TABLE {{db}}.{{agg_schema}}.CRMA_AGG_DT_CUSTOMER_HISTORY_AI
    WAREHOUSE = {{warehouse}}
    TARGET_LAG = '{{dt_lag}}'
    REFRESH_MODE = FULL
    COMMENT = 'SCD Type 2 customer history. AI pipeline.'
AS
WITH versioned AS (
    SELECT customer_id, first_name, last_name, email, phone, dq_score, file_date, row_timestamp,
        SHA2(CONCAT(COALESCE(first_name,''),'|',COALESCE(last_name,''),'|',COALESCE(email,''),'|',COALESCE(phone,''),'|',COALESCE(dq_score::VARCHAR,''))) AS row_hash,
        LAG(SHA2(CONCAT(COALESCE(first_name,''),'|',COALESCE(last_name,''),'|',COALESCE(email,''),'|',COALESCE(phone,''),'|',COALESCE(dq_score::VARCHAR,'')))) OVER (PARTITION BY customer_id ORDER BY file_date, row_timestamp) AS prev_hash
    FROM {{db}}.{{agg_schema}}.CRMA_AGG_DT_CUSTOMER_GOLDEN_AI
),
changes AS (SELECT * FROM versioned WHERE prev_hash IS NULL OR row_hash != prev_hash),
scd2 AS (
    SELECT customer_id, first_name, last_name, email, phone, dq_score,
        row_timestamp AS valid_from,
        COALESCE(LEAD(row_timestamp) OVER (PARTITION BY customer_id ORDER BY file_date, row_timestamp), '9999-12-31'::TIMESTAMP_LTZ) AS valid_to,
        row_hash
    FROM changes
)
SELECT customer_id, first_name, last_name, email, phone, dq_score, valid_from, valid_to,
    CASE WHEN valid_to = '9999-12-31'::TIMESTAMP_LTZ THEN TRUE ELSE FALSE END AS is_valid, row_hash
FROM scd2;

DEFINE DYNAMIC TABLE {{db}}.{{agg_schema}}.CRMA_AGG_DT_ADDRESSES_GROUPS_AI
    WAREHOUSE = {{warehouse}}
    TARGET_LAG = '{{dt_lag}}'
    REFRESH_MODE = FULL
    COMMENT = 'Links source addresses to master customer records. AI pipeline.'
AS
WITH linked AS (
    SELECT DISTINCT a.source_system, a.source_key, a.source_customer_key, g.customer_id
    FROM {{db}}.{{agg_schema}}.CRMA_AGG_VW_ADDRESSES_UNION a
    JOIN {{db}}.{{agg_schema}}.CRMA_AGG_DT_CUSTOMER_GROUPS_AI g ON a.source_system = g.source_system AND a.source_customer_key = g.source_key
)
SELECT customer_id AS address_id, customer_id, source_system, source_key, customer_id::VARCHAR AS cluster_id
FROM linked;

DEFINE DYNAMIC TABLE {{db}}.{{agg_schema}}.CRMA_AGG_DT_ADDRESSES_GOLDEN_AI
    WAREHOUSE = {{warehouse}}
    TARGET_LAG = '{{dt_lag}}'
    REFRESH_MODE = FULL
    COMMENT = 'Golden address per customer with survivorship. AI pipeline.'
AS
WITH grouped AS (
    SELECT g.address_id, g.customer_id, g.source_system, g.source_key,
        u.street, u.city, u.postal_code, u.country, u.file_date, u.row_timestamp,
        CASE g.source_system WHEN 'CRM_A' THEN 1 WHEN 'CRM_B' THEN 2 ELSE 3 END AS source_priority
    FROM {{db}}.{{agg_schema}}.CRMA_AGG_DT_ADDRESSES_GROUPS_AI g
    JOIN {{db}}.{{agg_schema}}.CRMA_AGG_VW_ADDRESSES_UNION u ON g.source_system = u.source_system AND g.source_key = u.source_key
),
survivorship AS (
    SELECT customer_id AS address_id, customer_id, file_date, row_timestamp,
        FIRST_VALUE(street) OVER (PARTITION BY customer_id, file_date ORDER BY CASE WHEN LENGTH(TRIM(COALESCE(street, ''))) >= 5 THEN 0 ELSE 1 END, source_priority, row_timestamp DESC) AS street,
        FIRST_VALUE(city) OVER (PARTITION BY customer_id, file_date ORDER BY CASE WHEN city IS NOT NULL THEN 0 ELSE 1 END, source_priority, row_timestamp DESC) AS city,
        FIRST_VALUE(postal_code) OVER (PARTITION BY customer_id, file_date ORDER BY CASE WHEN postal_code IS NOT NULL THEN 0 ELSE 1 END, source_priority, row_timestamp DESC) AS postal_code,
        FIRST_VALUE(country) OVER (PARTITION BY customer_id, file_date ORDER BY CASE WHEN country IS NOT NULL THEN source_priority ELSE 99 END, row_timestamp DESC) AS country
    FROM grouped
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id, file_date ORDER BY source_priority) = 1
),
dq_rules AS (
    SELECT address_id, customer_id, street, city, postal_code, country, file_date, row_timestamp,
        100
        + CASE WHEN street IS NULL OR LENGTH(TRIM(street)) < 5 THEN -5 ELSE 0 END
        + CASE WHEN city IS NULL THEN -20 ELSE 0 END
        + CASE WHEN street IS NOT NULL AND LENGTH(TRIM(street)) >= 5 AND postal_code IS NOT NULL AND city IS NOT NULL THEN 10 ELSE 0 END
        AS raw_dq_score
    FROM survivorship
)
SELECT address_id, customer_id, 'PRIMARY' AS address_type, street, city, postal_code, country, TRUE AS is_primary, file_date, row_timestamp,
    GREATEST(0, LEAST(100, raw_dq_score)) AS dq_score
FROM dq_rules;

DEFINE DYNAMIC TABLE {{db}}.{{agg_schema}}.CRMA_AGG_DT_ADDRESSES_AI
    WAREHOUSE = {{warehouse}}
    TARGET_LAG = '{{dt_lag}}'
    REFRESH_MODE = FULL
    COMMENT = 'Current golden address per customer (latest version only). AI pipeline.'
AS
SELECT address_id, customer_id, address_type, street, city, postal_code, country, is_primary, dq_score
FROM {{db}}.{{agg_schema}}.CRMA_AGG_DT_ADDRESSES_GOLDEN_AI
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY file_date DESC, row_timestamp DESC) = 1;

DEFINE DYNAMIC TABLE {{db}}.{{agg_schema}}.CRMA_AGG_DT_ADDRESSES_HISTORY_AI
    WAREHOUSE = {{warehouse}}
    TARGET_LAG = '{{dt_lag}}'
    REFRESH_MODE = FULL
    COMMENT = 'SCD Type 2 address history. AI pipeline.'
AS
WITH versioned AS (
    SELECT address_id, customer_id, address_type, street, city, postal_code, country, is_primary, dq_score, file_date, row_timestamp,
        SHA2(CONCAT(COALESCE(address_type,''),'|',COALESCE(street,''),'|',COALESCE(city,''),'|',COALESCE(postal_code,''),'|',COALESCE(country,''),'|',COALESCE(is_primary::VARCHAR,''),'|',COALESCE(dq_score::VARCHAR,''))) AS row_hash,
        LAG(SHA2(CONCAT(COALESCE(address_type,''),'|',COALESCE(street,''),'|',COALESCE(city,''),'|',COALESCE(postal_code,''),'|',COALESCE(country,''),'|',COALESCE(is_primary::VARCHAR,''),'|',COALESCE(dq_score::VARCHAR,'')))) OVER (PARTITION BY customer_id ORDER BY file_date, row_timestamp) AS prev_hash
    FROM {{db}}.{{agg_schema}}.CRMA_AGG_DT_ADDRESSES_GOLDEN_AI
),
changes AS (SELECT * FROM versioned WHERE prev_hash IS NULL OR row_hash != prev_hash),
scd2 AS (
    SELECT address_id, customer_id, address_type, street, city, postal_code, country, is_primary, dq_score,
        row_timestamp AS valid_from,
        COALESCE(LEAD(row_timestamp) OVER (PARTITION BY customer_id ORDER BY file_date, row_timestamp), '9999-12-31'::TIMESTAMP_LTZ) AS valid_to,
        row_hash
    FROM changes
)
SELECT address_id, customer_id, address_type, street, city, postal_code, country, is_primary, dq_score, valid_from, valid_to,
    CASE WHEN valid_to = '9999-12-31'::TIMESTAMP_LTZ THEN TRUE ELSE FALSE END AS is_valid, row_hash
FROM scd2;

-- =============================================================================
-- FUZZY PIPELINE — Classical fuzzy matching only (no Cortex AI, zero AI cost)
-- =============================================================================

DEFINE DYNAMIC TABLE {{db}}.{{agg_schema}}.CRMA_AGG_DT_CUSTOMER_ENRICHED_FUZZY
    WAREHOUSE = {{warehouse}}
    TARGET_LAG = '{{dt_lag}}'
    REFRESH_MODE = FULL
    COMMENT = 'Classical fuzzy enrichment (no Cortex AI). Passes first_name as canonical.'
AS
SELECT
    source_system, source_key, first_name, last_name,
    INITCAP(TRIM(first_name)) AS canonical_first_name,
    FALSE AS is_fake_name,
    email, phone, file_date, row_timestamp
FROM {{db}}.{{agg_schema}}.CRMA_AGG_VW_CUSTOMER_UNION;

DEFINE DYNAMIC TABLE {{db}}.{{agg_schema}}.CRMA_AGG_DT_CUSTOMER_GROUPS_FUZZY
    WAREHOUSE = {{warehouse}}
    TARGET_LAG = '{{dt_lag}}'
    REFRESH_MODE = FULL
    COMMENT = 'Entity resolution with blocking (BLOCK-01..04). Threshold >= 0.70. Fuzzy pipeline.'
AS
WITH base AS (
    SELECT DISTINCT
        e.source_system, e.source_key, e.first_name, e.last_name,
        e.canonical_first_name, e.is_fake_name, e.email, e.phone,
        a.street, a.city, a.postal_code,
        SOUNDEX(e.last_name) AS block_soundex,
        CASE WHEN e.email IS NOT NULL AND POSITION('@' IN e.email) > 0
             THEN SUBSTR(e.email, POSITION('@' IN e.email)) ELSE NULL END AS block_email_domain,
        CASE WHEN LENGTH(REGEXP_REPLACE(e.phone, '[^0-9]', '')) >= 4
             THEN RIGHT(REGEXP_REPLACE(e.phone, '[^0-9]', ''), 4) ELSE NULL END AS block_phone_suffix
    FROM {{db}}.{{agg_schema}}.CRMA_AGG_DT_CUSTOMER_ENRICHED_FUZZY e
    LEFT JOIN {{db}}.{{agg_schema}}.CRMA_AGG_VW_ADDRESSES_UNION a
        ON e.source_system = a.source_system AND e.source_key = a.source_customer_key
),
blocked_pairs AS (
    SELECT DISTINCT
        a.source_system AS source_a, a.source_key AS key_a,
        b.source_system AS source_b, b.source_key AS key_b,
        a.canonical_first_name AS fn_a, a.last_name AS ln_a, a.email AS email_a, a.phone AS phone_a,
        a.street AS street_a, a.city AS city_a, a.postal_code AS postal_a,
        b.canonical_first_name AS fn_b, b.last_name AS ln_b, b.email AS email_b, b.phone AS phone_b,
        b.street AS street_b, b.city AS city_b, b.postal_code AS postal_b
    FROM base a JOIN base b
        ON (a.source_system < b.source_system OR (a.source_system = b.source_system AND a.source_key < b.source_key))
        AND ((a.block_soundex IS NOT NULL AND a.block_soundex = b.block_soundex)
            OR (a.block_email_domain IS NOT NULL AND a.block_email_domain = b.block_email_domain)
            OR (a.block_phone_suffix IS NOT NULL AND a.block_phone_suffix = b.block_phone_suffix))
),
match_pairs AS (
    SELECT source_a, key_a, source_b, key_b,
        CASE WHEN email_a IS NOT NULL AND email_a = email_b THEN 1.0 ELSE 0 END AS email_match,
        CASE WHEN LENGTH(phone_a) >= 10 AND LENGTH(phone_b) >= 10 AND RIGHT(phone_a, 10) = RIGHT(phone_b, 10) THEN 0.95 ELSE 0 END AS phone_match,
        CASE WHEN fn_a IS NOT NULL AND fn_b IS NOT NULL AND ln_a IS NOT NULL AND ln_b IS NOT NULL
             AND JAROWINKLER_SIMILARITY(CONCAT(fn_a, ' ', ln_a), CONCAT(fn_b, ' ', ln_b)) >= 85
             THEN JAROWINKLER_SIMILARITY(CONCAT(fn_a, ' ', ln_a), CONCAT(fn_b, ' ', ln_b)) / 100.0 * 0.30 ELSE 0 END AS name_similarity,
        CASE WHEN SOUNDEX(ln_a) = SOUNDEX(ln_b) THEN 0.20 ELSE 0 END AS soundex_match,
        CASE WHEN fn_a IS NOT NULL AND fn_b IS NOT NULL AND ln_a IS NOT NULL AND ln_b IS NOT NULL
             AND LOWER(TRIM(fn_a)) = LOWER(TRIM(fn_b)) AND LOWER(TRIM(ln_a)) = LOWER(TRIM(ln_b)) THEN 0.80 ELSE 0 END AS canonical_exact_match,
        CASE WHEN street_a IS NOT NULL AND street_b IS NOT NULL AND postal_a IS NOT NULL AND postal_b IS NOT NULL
             AND JAROWINKLER_SIMILARITY(street_a, street_b) >= 80 AND postal_a = postal_b THEN 0.25 ELSE 0 END AS address_similarity,
        CASE WHEN email_a IS NOT NULL AND email_b IS NOT NULL AND POSITION('@' IN email_a) > 0 AND POSITION('@' IN email_b) > 0
             AND SUBSTR(email_a, POSITION('@' IN email_a)) = SUBSTR(email_b, POSITION('@' IN email_b))
             AND fn_a IS NOT NULL AND fn_b IS NOT NULL AND JAROWINKLER_SIMILARITY(LOWER(TRIM(fn_a)), LOWER(TRIM(fn_b))) >= 90
             THEN 0.15 ELSE 0 END AS email_domain_name_match,
        CASE WHEN LENGTH(REGEXP_REPLACE(phone_a, '[^0-9]', '')) >= 7 AND LENGTH(REGEXP_REPLACE(phone_b, '[^0-9]', '')) >= 7
             AND RIGHT(REGEXP_REPLACE(phone_a, '[^0-9]', ''), 7) = RIGHT(REGEXP_REPLACE(phone_b, '[^0-9]', ''), 7)
             AND city_a IS NOT NULL AND city_b IS NOT NULL AND LOWER(TRIM(city_a)) = LOWER(TRIM(city_b))
             THEN 0.10 ELSE 0 END AS phone_partial_city_match
    FROM blocked_pairs
),
matches AS (
    SELECT source_a, key_a, source_b, key_b FROM match_pairs
    WHERE GREATEST(email_match, phone_match, canonical_exact_match) + name_similarity + soundex_match + address_similarity + email_domain_name_match + phone_partial_city_match >= 0.70
),
matched_clusters AS (
    SELECT b.source_system, b.source_key, COALESCE(MIN(m.source_a || '|' || m.key_a), b.source_system || '|' || b.source_key) AS cluster_id
    FROM base b LEFT JOIN matches m ON (b.source_system = m.source_a AND b.source_key = m.key_a) OR (b.source_system = m.source_b AND b.source_key = m.key_b)
    GROUP BY b.source_system, b.source_key
)
SELECT DENSE_RANK() OVER (ORDER BY cluster_id) AS customer_id, source_system, source_key, cluster_id
FROM matched_clusters;

DEFINE DYNAMIC TABLE {{db}}.{{agg_schema}}.CRMA_AGG_DT_CUSTOMER_GOLDEN_FUZZY
    WAREHOUSE = {{warehouse}}
    TARGET_LAG = '{{dt_lag}}'
    REFRESH_MODE = FULL
    COMMENT = 'Golden customer records with survivorship + DQ scoring. Fuzzy pipeline.'
AS
WITH grouped AS (
    SELECT g.customer_id, g.source_system, g.source_key,
        u.first_name, u.last_name, u.email, u.phone, u.file_date, u.row_timestamp, u.is_fake_name,
        CASE g.source_system WHEN 'CRM_A' THEN 1 WHEN 'CRM_B' THEN 2 ELSE 3 END AS source_priority
    FROM {{db}}.{{agg_schema}}.CRMA_AGG_DT_CUSTOMER_GROUPS_FUZZY g
    JOIN {{db}}.{{agg_schema}}.CRMA_AGG_DT_CUSTOMER_ENRICHED_FUZZY u ON g.source_system = u.source_system AND g.source_key = u.source_key
),
survivorship AS (
    SELECT customer_id, file_date, row_timestamp,
        FIRST_VALUE(first_name) OVER (PARTITION BY customer_id, file_date ORDER BY CASE WHEN LENGTH(TRIM(COALESCE(first_name, ''))) > 1 THEN 0 ELSE 1 END, source_priority, row_timestamp DESC) AS first_name,
        FIRST_VALUE(last_name) OVER (PARTITION BY customer_id, file_date ORDER BY CASE WHEN LENGTH(TRIM(COALESCE(last_name, ''))) > 1 THEN 0 ELSE 1 END, source_priority, row_timestamp DESC) AS last_name,
        FIRST_VALUE(email) OVER (PARTITION BY customer_id, file_date ORDER BY CASE WHEN email LIKE '%@%' THEN 0 ELSE 1 END, source_priority, row_timestamp DESC) AS email,
        FIRST_VALUE(phone) OVER (PARTITION BY customer_id, file_date ORDER BY CASE WHEN LENGTH(phone) >= 7 THEN 0 ELSE 1 END, source_priority, row_timestamp DESC) AS phone,
        COUNT(DISTINCT source_system) OVER (PARTITION BY customer_id) AS source_count,
        MAX(CASE WHEN is_fake_name THEN 1 ELSE 0 END) OVER (PARTITION BY customer_id) = 1 AS is_fake_name
    FROM grouped
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id, file_date ORDER BY source_priority) = 1
),
dq_rules AS (
    SELECT customer_id, first_name, last_name, email, phone, file_date, row_timestamp, source_count, is_fake_name,
        100
        + CASE WHEN email IS NULL OR NOT RLIKE(email, '^[A-Za-z0-9._%+\055]+@[A-Za-z0-9.\055]+\.[A-Za-z]{2,}$') THEN -20 ELSE 0 END
        + CASE WHEN email IS NOT NULL AND (LOWER(email) LIKE '%@mailinator.com' OR LOWER(email) LIKE '%@tempmail.com' OR LOWER(email) LIKE '%@guerrillamail.com' OR LOWER(email) LIKE '%@10minutemail.com') THEN -5 ELSE 0 END
        + CASE WHEN first_name IS NULL OR LENGTH(TRIM(first_name)) <= 1 THEN -20 ELSE 0 END
        + CASE WHEN first_name IS NOT NULL AND LENGTH(TRIM(first_name)) > 1 AND NOT RLIKE(first_name, '^[A-Za-z \'\055]+$') THEN -5 ELSE 0 END
        + CASE WHEN last_name IS NULL OR LENGTH(TRIM(last_name)) <= 1 THEN -20 ELSE 0 END
        + CASE WHEN last_name IS NOT NULL AND LENGTH(TRIM(last_name)) > 1 AND NOT RLIKE(last_name, '^[A-Za-z \'\055]+$') THEN -5 ELSE 0 END
        + CASE WHEN phone IS NOT NULL AND NOT RLIKE(REGEXP_REPLACE(phone, '[^0-9+]', ''), '^\+?[0-9]{10,15}$') THEN -5 ELSE 0 END
        + CASE WHEN phone IS NOT NULL AND REGEXP_REPLACE(phone, '[^0-9]', '') IN ('0000000000','1111111111','1234567890') THEN -20 ELSE 0 END
        + CASE WHEN (email IS NULL OR NOT RLIKE(email, '^[A-Za-z0-9._%+\055]+@[A-Za-z0-9.\055]+\.[A-Za-z]{2,}$')) AND (phone IS NULL OR LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '')) < 7) THEN -20 ELSE 0 END
        + CASE WHEN (first_name IS NULL OR LENGTH(TRIM(first_name)) <= 1) AND (last_name IS NULL OR LENGTH(TRIM(last_name)) <= 1) THEN -20 ELSE 0 END
        + CASE WHEN first_name IS NOT NULL AND LENGTH(TRIM(first_name)) > 1 AND email IS NOT NULL AND POSITION(LOWER(TRIM(first_name)) IN LOWER(email)) > 0 THEN 5 ELSE 0 END
        + CASE WHEN is_fake_name THEN -20 ELSE 0 END
        AS raw_dq_score
    FROM survivorship
)
SELECT customer_id, first_name, last_name, email, phone, file_date, row_timestamp, source_count,
    GREATEST(0, LEAST(100, raw_dq_score)) AS dq_score
FROM dq_rules;

DEFINE DYNAMIC TABLE {{db}}.{{agg_schema}}.CRMA_AGG_DT_CUSTOMER_FUZZY
    WAREHOUSE = {{warehouse}}
    TARGET_LAG = '{{dt_lag}}'
    REFRESH_MODE = FULL
    COMMENT = 'Current golden customer records (latest version only). Fuzzy pipeline.'
AS
SELECT customer_id, first_name, last_name, email, phone, dq_score, source_count, row_timestamp AS last_updated
FROM {{db}}.{{agg_schema}}.CRMA_AGG_DT_CUSTOMER_GOLDEN_FUZZY
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY file_date DESC, row_timestamp DESC) = 1;

DEFINE DYNAMIC TABLE {{db}}.{{agg_schema}}.CRMA_AGG_DT_CUSTOMER_HISTORY_FUZZY
    WAREHOUSE = {{warehouse}}
    TARGET_LAG = '{{dt_lag}}'
    REFRESH_MODE = FULL
    COMMENT = 'SCD Type 2 customer history. Fuzzy pipeline.'
AS
WITH versioned AS (
    SELECT customer_id, first_name, last_name, email, phone, dq_score, file_date, row_timestamp,
        SHA2(CONCAT(COALESCE(first_name,''),'|',COALESCE(last_name,''),'|',COALESCE(email,''),'|',COALESCE(phone,''),'|',COALESCE(dq_score::VARCHAR,''))) AS row_hash,
        LAG(SHA2(CONCAT(COALESCE(first_name,''),'|',COALESCE(last_name,''),'|',COALESCE(email,''),'|',COALESCE(phone,''),'|',COALESCE(dq_score::VARCHAR,'')))) OVER (PARTITION BY customer_id ORDER BY file_date, row_timestamp) AS prev_hash
    FROM {{db}}.{{agg_schema}}.CRMA_AGG_DT_CUSTOMER_GOLDEN_FUZZY
),
changes AS (SELECT * FROM versioned WHERE prev_hash IS NULL OR row_hash != prev_hash),
scd2 AS (
    SELECT customer_id, first_name, last_name, email, phone, dq_score,
        row_timestamp AS valid_from,
        COALESCE(LEAD(row_timestamp) OVER (PARTITION BY customer_id ORDER BY file_date, row_timestamp), '9999-12-31'::TIMESTAMP_LTZ) AS valid_to,
        row_hash
    FROM changes
)
SELECT customer_id, first_name, last_name, email, phone, dq_score, valid_from, valid_to,
    CASE WHEN valid_to = '9999-12-31'::TIMESTAMP_LTZ THEN TRUE ELSE FALSE END AS is_valid, row_hash
FROM scd2;

DEFINE DYNAMIC TABLE {{db}}.{{agg_schema}}.CRMA_AGG_DT_ADDRESSES_GROUPS_FUZZY
    WAREHOUSE = {{warehouse}}
    TARGET_LAG = '{{dt_lag}}'
    REFRESH_MODE = FULL
    COMMENT = 'Links source addresses to master customer records. Fuzzy pipeline.'
AS
WITH linked AS (
    SELECT DISTINCT a.source_system, a.source_key, a.source_customer_key, g.customer_id
    FROM {{db}}.{{agg_schema}}.CRMA_AGG_VW_ADDRESSES_UNION a
    JOIN {{db}}.{{agg_schema}}.CRMA_AGG_DT_CUSTOMER_GROUPS_FUZZY g ON a.source_system = g.source_system AND a.source_customer_key = g.source_key
)
SELECT customer_id AS address_id, customer_id, source_system, source_key, customer_id::VARCHAR AS cluster_id
FROM linked;

DEFINE DYNAMIC TABLE {{db}}.{{agg_schema}}.CRMA_AGG_DT_ADDRESSES_GOLDEN_FUZZY
    WAREHOUSE = {{warehouse}}
    TARGET_LAG = '{{dt_lag}}'
    REFRESH_MODE = FULL
    COMMENT = 'Golden address per customer with survivorship. Fuzzy pipeline.'
AS
WITH grouped AS (
    SELECT g.address_id, g.customer_id, g.source_system, g.source_key,
        u.street, u.city, u.postal_code, u.country, u.file_date, u.row_timestamp,
        CASE g.source_system WHEN 'CRM_A' THEN 1 WHEN 'CRM_B' THEN 2 ELSE 3 END AS source_priority
    FROM {{db}}.{{agg_schema}}.CRMA_AGG_DT_ADDRESSES_GROUPS_FUZZY g
    JOIN {{db}}.{{agg_schema}}.CRMA_AGG_VW_ADDRESSES_UNION u ON g.source_system = u.source_system AND g.source_key = u.source_key
),
survivorship AS (
    SELECT customer_id AS address_id, customer_id, file_date, row_timestamp,
        FIRST_VALUE(street) OVER (PARTITION BY customer_id, file_date ORDER BY CASE WHEN LENGTH(TRIM(COALESCE(street, ''))) >= 5 THEN 0 ELSE 1 END, source_priority, row_timestamp DESC) AS street,
        FIRST_VALUE(city) OVER (PARTITION BY customer_id, file_date ORDER BY CASE WHEN city IS NOT NULL THEN 0 ELSE 1 END, source_priority, row_timestamp DESC) AS city,
        FIRST_VALUE(postal_code) OVER (PARTITION BY customer_id, file_date ORDER BY CASE WHEN postal_code IS NOT NULL THEN 0 ELSE 1 END, source_priority, row_timestamp DESC) AS postal_code,
        FIRST_VALUE(country) OVER (PARTITION BY customer_id, file_date ORDER BY CASE WHEN country IS NOT NULL THEN source_priority ELSE 99 END, row_timestamp DESC) AS country
    FROM grouped
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id, file_date ORDER BY source_priority) = 1
),
dq_rules AS (
    SELECT address_id, customer_id, street, city, postal_code, country, file_date, row_timestamp,
        100
        + CASE WHEN street IS NULL OR LENGTH(TRIM(street)) < 5 THEN -5 ELSE 0 END
        + CASE WHEN city IS NULL THEN -20 ELSE 0 END
        + CASE WHEN street IS NOT NULL AND LENGTH(TRIM(street)) >= 5 AND postal_code IS NOT NULL AND city IS NOT NULL THEN 10 ELSE 0 END
        AS raw_dq_score
    FROM survivorship
)
SELECT address_id, customer_id, 'PRIMARY' AS address_type, street, city, postal_code, country, TRUE AS is_primary, file_date, row_timestamp,
    GREATEST(0, LEAST(100, raw_dq_score)) AS dq_score
FROM dq_rules;

DEFINE DYNAMIC TABLE {{db}}.{{agg_schema}}.CRMA_AGG_DT_ADDRESSES_FUZZY
    WAREHOUSE = {{warehouse}}
    TARGET_LAG = '{{dt_lag}}'
    REFRESH_MODE = FULL
    COMMENT = 'Current golden address per customer (latest version only). Fuzzy pipeline.'
AS
SELECT address_id, customer_id, address_type, street, city, postal_code, country, is_primary, dq_score
FROM {{db}}.{{agg_schema}}.CRMA_AGG_DT_ADDRESSES_GOLDEN_FUZZY
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY file_date DESC, row_timestamp DESC) = 1;

DEFINE DYNAMIC TABLE {{db}}.{{agg_schema}}.CRMA_AGG_DT_ADDRESSES_HISTORY_FUZZY
    WAREHOUSE = {{warehouse}}
    TARGET_LAG = '{{dt_lag}}'
    REFRESH_MODE = FULL
    COMMENT = 'SCD Type 2 address history. Fuzzy pipeline.'
AS
WITH versioned AS (
    SELECT address_id, customer_id, address_type, street, city, postal_code, country, is_primary, dq_score, file_date, row_timestamp,
        SHA2(CONCAT(COALESCE(address_type,''),'|',COALESCE(street,''),'|',COALESCE(city,''),'|',COALESCE(postal_code,''),'|',COALESCE(country,''),'|',COALESCE(is_primary::VARCHAR,''),'|',COALESCE(dq_score::VARCHAR,''))) AS row_hash,
        LAG(SHA2(CONCAT(COALESCE(address_type,''),'|',COALESCE(street,''),'|',COALESCE(city,''),'|',COALESCE(postal_code,''),'|',COALESCE(country,''),'|',COALESCE(is_primary::VARCHAR,''),'|',COALESCE(dq_score::VARCHAR,'')))) OVER (PARTITION BY customer_id ORDER BY file_date, row_timestamp) AS prev_hash
    FROM {{db}}.{{agg_schema}}.CRMA_AGG_DT_ADDRESSES_GOLDEN_FUZZY
),
changes AS (SELECT * FROM versioned WHERE prev_hash IS NULL OR row_hash != prev_hash),
scd2 AS (
    SELECT address_id, customer_id, address_type, street, city, postal_code, country, is_primary, dq_score,
        row_timestamp AS valid_from,
        COALESCE(LEAD(row_timestamp) OVER (PARTITION BY customer_id ORDER BY file_date, row_timestamp), '9999-12-31'::TIMESTAMP_LTZ) AS valid_to,
        row_hash
    FROM changes
)
SELECT address_id, customer_id, address_type, street, city, postal_code, country, is_primary, dq_score, valid_from, valid_to,
    CASE WHEN valid_to = '9999-12-31'::TIMESTAMP_LTZ THEN TRUE ELSE FALSE END AS is_valid, row_hash
FROM scd2;
