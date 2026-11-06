/*
 * ingestion.sql — File formats, directory-table streams and serverless load
 * tasks for the six RAW landing stages (formerly post_deploy.sql)
 */
{% set na_null = "('', 'NULL', 'null', 'N/A')" %}
{% set std_null = "('', 'NULL', 'null')" %}
{% set sources = [
    {'name': 'CUSTOMER_A', 'null_if': std_null,
     'cols': 'SRC_CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE',
     'casts': '$1::VARCHAR(50), $2::VARCHAR(100), $3::VARCHAR(100), $4::VARCHAR(255), $5::VARCHAR(50)'},
    {'name': 'CUSTOMER_B', 'null_if': std_null,
     'cols': 'CUSTOMER_KEY, NAME, EMAIL_ADDRESS, MOBILE',
     'casts': '$1::VARCHAR(50), $2::VARCHAR(200), $3::VARCHAR(255), $4::VARCHAR(50)'},
    {'name': 'CUSTOMER_C', 'null_if': na_null,
     'cols': 'TICKET_CUSTOMER_ID, CALLER_NAME, CALLBACK_EMAIL, CALLBACK_PHONE',
     'casts': '$1::VARCHAR(50), $2::VARCHAR(200), $3::VARCHAR(255), $4::VARCHAR(50)'},
    {'name': 'ADDRESSES_A', 'null_if': std_null,
     'cols': 'SRC_ADDRESS_ID, SRC_CUSTOMER_ID, STREET, CITY, POSTAL_CODE, COUNTRY',
     'casts': '$1::VARCHAR(50), $2::VARCHAR(50), $3::VARCHAR(255), $4::VARCHAR(100), $5::VARCHAR(20), $6::VARCHAR(10)'},
    {'name': 'ADDRESSES_B', 'null_if': std_null,
     'cols': 'ADDR_ID, CUSTOMER_KEY, ADDRESS_LINE, CITY, ZIP, COUNTRY_CODE',
     'casts': '$1::VARCHAR(50), $2::VARCHAR(50), $3::VARCHAR(255), $4::VARCHAR(100), $5::VARCHAR(20), $6::VARCHAR(10)'},
    {'name': 'ADDRESSES_C', 'null_if': na_null,
     'cols': 'ADDR_REF, TICKET_CUSTOMER_ID, LOCATION, TOWN, POSTCODE, COUNTRY',
     'casts': '$1::VARCHAR(50), $2::VARCHAR(50), $3::VARCHAR(255), $4::VARCHAR(100), $5::VARCHAR(20), $6::VARCHAR(10)'},
] %}

{% for s in sources %}
DEFINE FILE FORMAT {{db}}.{{raw_schema}}.CRMI_RAW_FF_{{s.name}}
    TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = {{s.null_if}} EMPTY_FIELD_AS_NULL = TRUE ENCODING = 'UTF8';

DEFINE STREAM {{db}}.{{raw_schema}}.CRMI_RAW_SM_{{s.name}}
    ON STAGE {{db}}.{{raw_schema}}.CRMI_RAW_ST_{{s.name}};

DEFINE TASK {{db}}.{{raw_schema}}.CRMI_RAW_TS_LOAD_{{s.name}}
    SCHEDULE = '60 MINUTE'
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    WHEN SYSTEM$STREAM_HAS_DATA('{{db}}.{{raw_schema}}.CRMI_RAW_SM_{{s.name}}')
    STARTED
AS
    COPY INTO {{db}}.{{raw_schema}}.CRMI_RAW_TB_{{s.name}} ({{s.cols}}, _SOURCE_FILE)
    FROM (SELECT {{s.casts}}, METADATA$FILENAME FROM @{{db}}.{{raw_schema}}.CRMI_RAW_ST_{{s.name}})
    FILE_FORMAT = (FORMAT_NAME = '{{db}}.{{raw_schema}}.CRMI_RAW_FF_{{s.name}}')
    ON_ERROR = 'CONTINUE';
{% endfor %}
