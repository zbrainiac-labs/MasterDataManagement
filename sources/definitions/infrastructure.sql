-- =============================================================================
-- infrastructure.sql — Internal stages
-- =============================================================================
-- NOTE: Warehouse MD_TEST_WH is managed by DATAOPS.IOT_RAW_V001.MOTHER_OF_ALL_PROJECTS

DEFINE STAGE {{db}}.{{raw_schema}}.CRMI_RAW_ST_CUSTOMER_A
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' NULL_IF = ('', 'NULL', 'null') EMPTY_FIELD_AS_NULL = TRUE ENCODING = 'UTF8')
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Landing zone for CRM A customer files.';

DEFINE STAGE {{db}}.{{raw_schema}}.CRMI_RAW_ST_CUSTOMER_B
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' NULL_IF = ('', 'NULL', 'null') EMPTY_FIELD_AS_NULL = TRUE ENCODING = 'UTF8')
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Landing zone for CRM B customer files.';

DEFINE STAGE {{db}}.{{raw_schema}}.CRMI_RAW_ST_CUSTOMER_C
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' NULL_IF = ('', 'NULL', 'null', 'N/A') EMPTY_FIELD_AS_NULL = TRUE ENCODING = 'UTF8')
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Landing zone for CRM C customer files.';

DEFINE STAGE {{db}}.{{raw_schema}}.CRMI_RAW_ST_ADDRESSES_A
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' NULL_IF = ('', 'NULL', 'null') EMPTY_FIELD_AS_NULL = TRUE ENCODING = 'UTF8')
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Landing zone for CRM A address files.';

DEFINE STAGE {{db}}.{{raw_schema}}.CRMI_RAW_ST_ADDRESSES_B
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' NULL_IF = ('', 'NULL', 'null') EMPTY_FIELD_AS_NULL = TRUE ENCODING = 'UTF8')
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Landing zone for CRM B address files.';

DEFINE STAGE {{db}}.{{raw_schema}}.CRMI_RAW_ST_ADDRESSES_C
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' NULL_IF = ('', 'NULL', 'null', 'N/A') EMPTY_FIELD_AS_NULL = TRUE ENCODING = 'UTF8')
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Landing zone for CRM C address files.';
