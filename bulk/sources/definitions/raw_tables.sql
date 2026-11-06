/*
 * raw_tables.sql — Append-only RAW tables for customer and address ingestion
 */

DEFINE TABLE {{db}}.{{raw_schema}}.CRMI_RAW_TB_CUSTOMER_A (
    SRC_CUSTOMER_ID VARCHAR(50) NOT NULL COMMENT 'Original customer identifier from CRM A system',
    FIRST_NAME VARCHAR(100) COMMENT 'Customer given name as recorded in CRM A',
    LAST_NAME VARCHAR(100) COMMENT 'Customer family name as recorded in CRM A',
    EMAIL VARCHAR(255) COMMENT 'Customer email address',
    PHONE VARCHAR(50) COMMENT 'Customer phone number in original format',
    _SOURCE_FILE VARCHAR(500) COMMENT 'Source filename for data lineage'
)
COMMENT = 'Append-only raw data from CRM A (legacy system).';

DEFINE TABLE {{db}}.{{raw_schema}}.CRMI_RAW_TB_CUSTOMER_B (
    CUSTOMER_KEY VARCHAR(50) NOT NULL COMMENT 'Original customer identifier from CRM B system',
    NAME VARCHAR(200) COMMENT 'Full customer name (first + last combined)',
    EMAIL_ADDRESS VARCHAR(255) COMMENT 'Customer email address',
    MOBILE VARCHAR(50) COMMENT 'Customer mobile phone number',
    _SOURCE_FILE VARCHAR(500) COMMENT 'Source filename for data lineage'
)
COMMENT = 'Append-only raw data from CRM B (acquired company).';

DEFINE TABLE {{db}}.{{raw_schema}}.CRMI_RAW_TB_CUSTOMER_C (
    TICKET_CUSTOMER_ID VARCHAR(50) NOT NULL COMMENT 'Original customer identifier from CRM C call center',
    CALLER_NAME VARCHAR(200) COMMENT 'Full caller name as recorded by call center agent',
    CALLBACK_EMAIL VARCHAR(255) COMMENT 'Callback email address',
    CALLBACK_PHONE VARCHAR(50) COMMENT 'Callback phone number',
    _SOURCE_FILE VARCHAR(500) COMMENT 'Source filename for data lineage'
)
COMMENT = 'Append-only raw data from CRM C (call center system).';

DEFINE TABLE {{db}}.{{raw_schema}}.CRMI_RAW_TB_ADDRESSES_A (
    SRC_ADDRESS_ID VARCHAR(50) NOT NULL COMMENT 'Original address identifier from CRM A',
    SRC_CUSTOMER_ID VARCHAR(50) NOT NULL COMMENT 'Customer identifier linking address to owner',
    STREET VARCHAR(255) COMMENT 'Street address including house number',
    CITY VARCHAR(100) COMMENT 'City or municipality name',
    POSTAL_CODE VARCHAR(20) COMMENT 'Postal or ZIP code',
    COUNTRY VARCHAR(10) COMMENT 'Country code (ISO 3166-1 alpha-2)',
    _SOURCE_FILE VARCHAR(500) COMMENT 'Source filename for data lineage'
)
COMMENT = 'Append-only raw address data from CRM A (legacy system).';

DEFINE TABLE {{db}}.{{raw_schema}}.CRMI_RAW_TB_ADDRESSES_B (
    ADDR_ID VARCHAR(50) NOT NULL COMMENT 'Original address identifier from CRM B',
    CUSTOMER_KEY VARCHAR(50) NOT NULL COMMENT 'Customer identifier linking address to owner',
    ADDRESS_LINE VARCHAR(255) COMMENT 'Full street address as single line',
    CITY VARCHAR(100) COMMENT 'City or municipality name',
    ZIP VARCHAR(20) COMMENT 'ZIP or postal code',
    COUNTRY_CODE VARCHAR(10) COMMENT 'Country code as stored in CRM B',
    _SOURCE_FILE VARCHAR(500) COMMENT 'Source filename for data lineage'
)
COMMENT = 'Append-only raw address data from CRM B (acquired company).';

DEFINE TABLE {{db}}.{{raw_schema}}.CRMI_RAW_TB_ADDRESSES_C (
    ADDR_REF VARCHAR(50) NOT NULL COMMENT 'Original address reference from CRM C',
    TICKET_CUSTOMER_ID VARCHAR(50) NOT NULL COMMENT 'Customer identifier linking address to owner',
    LOCATION VARCHAR(255) COMMENT 'Street address as recorded by call center agent',
    TOWN VARCHAR(100) COMMENT 'Town or city name',
    POSTCODE VARCHAR(20) COMMENT 'Postal code',
    COUNTRY VARCHAR(10) COMMENT 'Country code',
    _SOURCE_FILE VARCHAR(500) COMMENT 'Source filename for data lineage'
)
COMMENT = 'Append-only raw address data from CRM C (call center system).';
