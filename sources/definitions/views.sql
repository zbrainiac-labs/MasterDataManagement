-- =============================================================================
-- views.sql — Union views and XREF views
-- =============================================================================

DEFINE VIEW {{db}}.{{agg_schema}}.CRMA_AGG_VW_CUSTOMER_UNION
    COMMENT = 'Harmonizes customer data from 3 CRM systems into unified schema.'
AS
WITH crm_a AS (
    SELECT 'CRM_A' AS source_system, src_customer_id AS source_key,
        INITCAP(first_name) AS first_name, INITCAP(last_name) AS last_name,
        LOWER(TRIM(email)) AS email, REGEXP_REPLACE(phone, '[^0-9+]', '') AS phone,
        TRY_TO_DATE(SPLIT_PART(_SOURCE_FILE, '_crm_', 1), 'YYYY-MM-DD') AS file_date,
        CURRENT_TIMESTAMP()::TIMESTAMP_TZ AS row_timestamp
    FROM {{db}}.{{raw_schema}}.CRMI_RAW_TB_CUSTOMER_A
),
crm_b AS (
    SELECT 'CRM_B' AS source_system, customer_key AS source_key,
        INITCAP(SPLIT_PART(name, ' ', 1)) AS first_name,
        INITCAP(CASE WHEN ARRAY_SIZE(SPLIT(name, ' ')) > 1 THEN TRIM(SUBSTR(name, POSITION(' ' IN name) + 1)) ELSE NULL END) AS last_name,
        LOWER(TRIM(email_address)) AS email, REGEXP_REPLACE(mobile, '[^0-9+]', '') AS phone,
        TRY_TO_DATE(SPLIT_PART(_SOURCE_FILE, '_crm_', 1), 'YYYY-MM-DD') AS file_date,
        CURRENT_TIMESTAMP()::TIMESTAMP_TZ AS row_timestamp
    FROM {{db}}.{{raw_schema}}.CRMI_RAW_TB_CUSTOMER_B
),
crm_c AS (
    SELECT 'CRM_C' AS source_system, ticket_customer_id AS source_key,
        INITCAP(SPLIT_PART(caller_name, ' ', 1)) AS first_name,
        INITCAP(CASE WHEN ARRAY_SIZE(SPLIT(caller_name, ' ')) > 1 THEN TRIM(SUBSTR(caller_name, POSITION(' ' IN caller_name) + 1)) ELSE NULL END) AS last_name,
        LOWER(TRIM(callback_email)) AS email, REGEXP_REPLACE(callback_phone, '[^0-9+]', '') AS phone,
        TRY_TO_DATE(SPLIT_PART(_SOURCE_FILE, '_crm_', 1), 'YYYY-MM-DD') AS file_date,
        CURRENT_TIMESTAMP()::TIMESTAMP_TZ AS row_timestamp
    FROM {{db}}.{{raw_schema}}.CRMI_RAW_TB_CUSTOMER_C
)
SELECT * FROM crm_a UNION ALL SELECT * FROM crm_b UNION ALL SELECT * FROM crm_c;

DEFINE VIEW {{db}}.{{agg_schema}}.CRMA_AGG_VW_ADDRESSES_UNION
    COMMENT = 'Harmonizes address data from 3 CRM systems into unified schema.'
AS
WITH crm_a AS (
    SELECT 'CRM_A' AS source_system, src_address_id AS source_key, src_customer_id AS source_customer_key,
        INITCAP(TRIM(street)) AS street, INITCAP(TRIM(city)) AS city,
        UPPER(TRIM(postal_code)) AS postal_code, UPPER(TRIM(country)) AS country,
        TRY_TO_DATE(SPLIT_PART(_SOURCE_FILE, '_crm_', 1), 'YYYY-MM-DD') AS file_date,
        CURRENT_TIMESTAMP()::TIMESTAMP_TZ AS row_timestamp
    FROM {{db}}.{{raw_schema}}.CRMI_RAW_TB_ADDRESSES_A
),
crm_b AS (
    SELECT 'CRM_B' AS source_system, addr_id AS source_key, customer_key AS source_customer_key,
        INITCAP(TRIM(address_line)) AS street, INITCAP(TRIM(city)) AS city,
        UPPER(TRIM(zip)) AS postal_code, UPPER(TRIM(country_code)) AS country,
        TRY_TO_DATE(SPLIT_PART(_SOURCE_FILE, '_crm_', 1), 'YYYY-MM-DD') AS file_date,
        CURRENT_TIMESTAMP()::TIMESTAMP_TZ AS row_timestamp
    FROM {{db}}.{{raw_schema}}.CRMI_RAW_TB_ADDRESSES_B
),
crm_c AS (
    SELECT 'CRM_C' AS source_system, addr_ref AS source_key, ticket_customer_id AS source_customer_key,
        INITCAP(TRIM(location)) AS street, INITCAP(TRIM(town)) AS city,
        UPPER(TRIM(postcode)) AS postal_code, UPPER(TRIM(country)) AS country,
        TRY_TO_DATE(SPLIT_PART(_SOURCE_FILE, '_crm_', 1), 'YYYY-MM-DD') AS file_date,
        CURRENT_TIMESTAMP()::TIMESTAMP_TZ AS row_timestamp
    FROM {{db}}.{{raw_schema}}.CRMI_RAW_TB_ADDRESSES_C
)
SELECT * FROM crm_a UNION ALL SELECT * FROM crm_b UNION ALL SELECT * FROM crm_c;

DEFINE VIEW {{db}}.{{agg_schema}}.CRMA_AGG_VW_CUSTOMER_XREF
    COMMENT = 'Live cross-reference mapping from source keys to master customer IDs.'
AS
SELECT ROW_NUMBER() OVER (ORDER BY customer_id, source_system, source_key) AS xref_id,
    customer_id, source_system, source_key, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS created_at
FROM {{db}}.{{agg_schema}}.CRMA_AGG_DT_CUSTOMER_GROUPS;

DEFINE VIEW {{db}}.{{agg_schema}}.CRMA_AGG_VW_ADDRESSES_XREF
    COMMENT = 'Live cross-reference mapping from source keys to master address IDs.'
AS
SELECT ROW_NUMBER() OVER (ORDER BY address_id, source_system, source_key) AS xref_id,
    address_id, source_system, source_key, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS created_at
FROM {{db}}.{{agg_schema}}.CRMA_AGG_DT_ADDRESSES_GROUPS;
