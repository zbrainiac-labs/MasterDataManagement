-- =============================================================================
-- pre_deploy.sql — Database and schema setup (must exist before DCM plan)
-- =============================================================================

CREATE DATABASE IF NOT EXISTS MDM_DEV
    COMMENT = 'Central repository for unified customer and address master data';

USE DATABASE MDM_DEV;

CREATE SCHEMA IF NOT EXISTS MDM_RAW_001
    COMMENT = 'Landing zone for raw customer and address data from source systems.';

CREATE SCHEMA IF NOT EXISTS MDM_AGG_001
    COMMENT = 'Entity resolution, survivorship, golden records, and SCD Type 2 history.';

CREATE SCHEMA IF NOT EXISTS MDM_SRV_001
    COMMENT = 'Consumer-ready Customer 360 views for BI tools, APIs, and applications.';

CREATE DCM PROJECT IF NOT EXISTS MDM_DEV.MDM_AGG_001.MDM_PROJECT;
