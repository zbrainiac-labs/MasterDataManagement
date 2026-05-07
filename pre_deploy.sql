CREATE DATABASE IF NOT EXISTS {{ db }}
    COMMENT = 'Central repository for unified customer and address master data';

CREATE SCHEMA IF NOT EXISTS {{ db }}.MDM_DCM;
CREATE SCHEMA IF NOT EXISTS {{ db }}.{{ raw_schema }}
    COMMENT = 'Landing zone for raw customer and address data from source systems.';
CREATE SCHEMA IF NOT EXISTS {{ db }}.{{ agg_schema }}
    COMMENT = 'Entity resolution, survivorship, golden records, and SCD Type 2 history.';
CREATE SCHEMA IF NOT EXISTS {{ db }}.{{ srv_schema }}
    COMMENT = 'Consumer-ready Customer 360 views for BI tools, APIs, and applications.';

CREATE DCM PROJECT IF NOT EXISTS {{ db }}.MDM_DCM.MDM_PROJECT;
