# Snowflake-Native Master Data Management
## Why

Organizations running multiple systems that keep similar types of data -- such as CRM, ERP, or billing -- face a common problem: the same entity (customer, product, account) exists as separate, conflicting records across systems. There is no single source of truth, data quality is unknown, and historical changes are invisible. Commercial MDM platforms solve this but at significant complexity and cost.

This project proves that **Snowflake alone** delivers core MDM capabilities -- entity resolution, survivorship, data quality scoring, SCD Type 2 history, and a 360 view -- using only native features. The showcase uses **CRM customer data** as the example domain.

## What

The Showcase is a  fully functional MDM pipeline that merges **1,500 customer records and their addresses from 3 CRM systems** into **1,115 golden customer records with 1:1 linked golden addresses**, achieving a 24.4% merge rate, weighted DQ scoring, and full SCD2 change history for both entities.

### Glossary

| Term                  | Definition                                                                                     |
|-----------------------|------------------------------------------------------------------------------------------------|
| **DCDF**              | Data Cloud Deployment Framework -- RAW, INTEGRATION, PRESENTATION, SHARE layers                |
| **Entity Resolution** | Identifying and linking records referring to the same real-world entity across sources          |
| **Golden Record**     | The single authoritative version of a master data entity after survivorship rules are applied   |
| **Survivorship**      | Rules determining which attribute values from source records are selected for the golden record |
| **DQ**                | Data Quality -- metrics and rules to measure data accuracy and completeness                    |

### Source Systems

| Source | Description       | Trust | Records | Entities          |
|--------|-------------------|-------|---------|-------------------|
| CRM_A  | Legacy system     | 1     | 600     | Customer, Address |
| CRM_B  | Acquired company  | 2     | 400     | Customer, Address |
| CRM_C  | Call center       | 3     | 500     | Customer, Address |

### Key Metrics (E2E Tested)

| Metric                | Value                    |
|-----------------------|--------------------------|
| Source records        | 1,500 (600 + 400 + 500) |
| Golden records        | 1,115                    |
| Merged (2+ sources)   | 272 customers            |
| Three-source merges   | 40 customers             |
| Merge rate            | 24.4%                    |
| Avg DQ score          | 95.0                     |
| DQ Excellent (90-100) | 973 records              |

### Scope

**In Scope:** Customer master data, Address master data, entity resolution, survivorship, DQ scoring, Customer 360 view, Streamlit dashboard.

**Out of Scope:**

| ID | Category                  | Excluded                                           | Why                                    |
|----|---------------------------|----------------------------------------------------|-----------------------------------------|
| EX-01 | Additional MDM domains    | Product, Account, Organization, Household          | CRM Customer + Address only             |
| EX-02 | Stewardship UI            | Manual match/merge/unmerge, approval queues         | All logic is batch SQL                  |
| EX-03 | Integration & APIs        | REST/GraphQL APIs, webhooks, SaaS connectors       | CSV-based ingestion via stages          |
| EX-04 | Advanced governance       | GDPR/CCPA consent, retention policies               | Tags/masking planned (OP-05)            |
| EX-05 | Multi-address             | N:M relationships, org hierarchies                  | 1:1 model; N:M planned (OP-07)         |
| EX-06 | Production observability  | Health dashboards, error queues, SLAs               | DMF monitoring planned (OP-06)          |

### Capabilities & Snowflake Features

| ID | Capability                     | Snowflake Feature                                              | Status   |
|----|--------------------------------|----------------------------------------------------------------|----------|
| CAP-01 | Event-driven ingestion         | Stages + Directory Table Streams + Serverless Tasks            | **Done** |
| CAP-02 | Column harmonization           | Views with INITCAP, LOWER, REGEXP_REPLACE                     | **Done** |
| CAP-03 | AI-enhanced entity resolution  | CORTEX.COMPLETE (nickname), AI_CLASSIFY (fake detection)       | **Done** |
| CAP-04 | Deterministic matching         | Email exact, phone normalized (last 10 digits)                 | **Done** |
| CAP-05 | Probabilistic matching         | JAROWINKLER_SIMILARITY, SOUNDEX, blocking strategy             | **Done** |
| CAP-06 | Survivorship rules             | FIRST_VALUE() ordered by completeness, trust, recency          | **Done** |
| CAP-07 | Data quality scoring           | 13 weighted rules (base 100, error -20, warning -5, bonus +5) | **Done** |
| CAP-08 | Current golden records         | Dynamic Tables (TARGET_LAG = 1 hour, auto-refresh)             | **Done** |
| CAP-09 | SCD Type 2 history             | Dynamic Tables (declarative SCD2 via SHA2 + LAG)               | **Done** |
| CAP-10 | Cross-reference lineage        | Live XREF views (source key <-> master ID)                     | **Done** |
| CAP-11 | Customer 360 presentation      | Serving views (nested JSON + flat for BI)                      | **Done** |
| CAP-12 | Interactive dashboard          | Streamlit 5-tab app                                            | **Done** |

---

## How

### Naming Convention

```
[DOMAIN][LAYER]_[TYPE]_[OBJECT]_[OBJECT_NAME]
```

| Position | Code | Meaning |
|----------|------|---------|
| 1-3      | CRM  | Domain |
| 4        | I / A / S | Layer: Ingest (RAW), Analytical (Integration), Serving |
| 6+       | RAW, AGG | RAW = raw data, AGG = aggregated/mastered |
| next     | TB, DT, VW, ST, FF, TS, SM | Object type |
| last     | name | Descriptive name + source suffix |

**Examples:** `CRMI_RAW_TB_CUSTOMER_A`, `CRMA_AGG_DT_CUSTOMER`, `CRMS_AGG_VW_CUSTOMER_360`

### Architecture

```
  CRM_A (CSV)            CRM_B (CSV)            CRM_C (CSV)
  Trust=1                Trust=2                Trust=3
      |                      |                      |
      v                      v                      v
+------------------------------------------------------------------------+
|  RAW (CRM_RAW_001)                                                     |
|  Stages -> Directory Table Streams -> Serverless Tasks -> COPY INTO    |
|  6 append-only tables (3 customer + 3 address), ROW_TIMESTAMP=TRUE     |
+------------------------------------------------------------------------+
      |
      v
+------------------------------------------------------------------------+
|  INTEGRATION (CRM_AGG_001)                                             |
|                                                                        |
|  View Pipeline:                                                        |
|    VW_CUSTOMER_UNION       Harmonize 3 sources into common schema      |
|         |                                                              |
|    DT_CUSTOMER_ENRICHED     Cortex AI (materialized Dynamic Table)      |
|         |                  Nicknames + fake detection, cached results  |
|         |                                                              |
|    DT_CUSTOMER_GROUPS       Entity resolution with blocking strategy    |
|         |                  Deterministic + Probabilistic matching       |
|         |                                                              |
|    DT_CUSTOMER_GOLDEN       Survivorship + DQ scoring (0-100)           |
|         |                                                              |
|         +---> DT_CUSTOMER           Current golden (Dynamic Table)     |
|         |       |                                                      |
|         |       +---> DT_CUSTOMER_HISTORY   SCD2 (Dynamic Table)        |
|         |                                                              |
|    DT_ADDRESSES_*          Same pipeline for addresses (all DTs)        |
|         +---> DT_ADDRESSES          Current golden (Dynamic Table)     |
|               +---> DT_ADDRESSES_HISTORY   SCD2 (Dynamic Table)        |
+------------------------------------------------------------------------+
      |
      v
+------------------------------------------------------------------------+
|  SERVING (CRM_SRV_001)                                                 |
|  VW_CUSTOMER_360       Nested JSON (APIs, modern apps)                 |
|  VW_CUSTOMER_360_FLAT  Flat rows (Tableau, Power BI)                   |
+------------------------------------------------------------------------+
      |
      v
  Streamlit Dashboard (5 tabs: Overview, Search, DQ, ER, SCD History)
```

### Entity Resolution

Two-pass approach: **deterministic** rules for high-confidence matches, then **probabilistic** rules for fuzzy matches. Records are blocked first to reduce pair comparisons.

#### Blocking Strategy

| Block ID | Key                                            | Status       |
|----------|------------------------------------------------|--------------|
| BLOCK-01 | `SOUNDEX(last_name)`                           | **Done**     |
| BLOCK-02 | `LEFT(postal_code, 3)`                         | Deferred     |
| BLOCK-03 | `SUBSTR(email, POSITION('@' IN email))`        | **Done**     |
| BLOCK-04 | `RIGHT(REGEXP_REPLACE(phone, '[^0-9]', ''), 4)`| **Done**     |

#### Deterministic Rules

| Rule ID   | Rule             | Logic                                                          | Confidence |
|-----------|------------------|----------------------------------------------------------------|------------|
| MATCH-D01 | Email Exact      | `LOWER(TRIM(email_a)) = LOWER(TRIM(email_b))`                 | 100%       |
| MATCH-D02 | Phone Normalized | Last 10 digits match, length >= 10                             | 95%        |
| MATCH-D03 | Name + DOB       | `LOWER(last_name)` match AND `dob` match                      | Planned    |

#### Probabilistic Rules

| Rule ID   | Rule               | Logic                                      | Weight |
|-----------|--------------------|---------------------------------------------|--------|
| MATCH-P01 | Name Similarity    | `JAROWINKLER_SIMILARITY >= 85`             | 0.30   |
| MATCH-P02 | Address Similarity | Street JW >= 80 AND postal_code match       | 0.25   |
| MATCH-P03 | Name Sound         | `SOUNDEX(last_name)` match                                    | 0.20   |
| MATCH-P04 | Email Domain+Name  | Same domain AND first_name similarity >= 90% | 0.15   |
| MATCH-P05 | Phone Partial      | Last 7 digits match AND same city            | 0.10   |

#### Cortex AI Enrichment

| Rule ID   | Technique             | Function                                        | Purpose                                       |
|-----------|-----------------------|-------------------------------------------------|-----------------------------------------------|
| ENRICH-01 | Nickname resolution   | `CORTEX.COMPLETE('mistral-large2')`             | Bill->William, Bob->Robert, etc.              |
| ENRICH-02 | Fake name detection   | `AI_CLASSIFY(['real_person_name', 'fake_or_test_name'])` | Flags test/placeholder names (DQ -20) |
| MATCH-C01 | Canonical exact match | `canonical_first_name_a = canonical_first_name_b` | Score 0.80 using Cortex-resolved names      |

**Threshold:** Combined score >= 0.70 to merge.

```
match_score = GREATEST(D01, D02, C01) + P01 + P02 + P03 + P04 + P05

>= 0.70: Merge    < 0.70: No match
```

### Building the Golden Record

Each golden record is assembled in four steps from raw source records:

```
Step 1 — Union & Enrich     All 1,500 source records are unified into a common schema
                             (VW_CUSTOMER_UNION). Cortex AI resolves nicknames and flags
                             fake names (DT_CUSTOMER_ENRICHED, materialized).

Step 2 — Group              Entity resolution (blocking → deterministic → probabilistic)
                             assigns every record a GROUP_ID. Records sharing a GROUP_ID
                             represent the same real-world entity (DT_CUSTOMER_GROUPS).
                             Result: 1,115 groups from 1,500 records (24.4% merge rate).

Step 3 — Survive            Within each GROUP_ID, survivorship selects the best value for
                             every attribute using FIRST_VALUE() ordered by completeness →
                             source trust → recency. One row per GROUP_ID emerges as the
                             golden record (DT_CUSTOMER_GOLDEN → DT_CUSTOMER).

Step 4 — Score              Data quality rules run AFTER survivorship on the golden record.
                             The DQ score (0-100) measures output quality; it does not
                             influence which source value wins.
```

**Concrete example — 3 records, 1 golden:**

| Field        | CRM A (trust 1)       | CRM B (trust 2)       | CRM C (trust 3) | Golden Record winner          |
|--------------|-----------------------|-----------------------|------------------|-------------------------------|
| `first_name` | `Bill`                | `William`             | *(null)*         | `William` — most recent + len > 1 |
| `last_name`  | `Smith`               | `Smith`               | `Smth`           | `Smith` — CRM A trust wins tie   |
| `email`      | `bill@acme.com`       | *(null)*              | `b@test.xyz`     | `bill@acme.com` — CRM A + valid  |
| `phone`      | `+11043321819`        | `+110433218`          | *(null)*         | `+11043321819` — longest E.164   |
| `dq_score`   |                       |                       |                  | 95 — scored on the golden row     |

### Survivorship Rules

For each group of matched records, survivorship determines which source value wins for each attribute. Uses `FIRST_VALUE()` window functions ordered by: (1) completeness (non-null, non-empty), (2) source trust priority, (3) recency (`METADATA$ROW_LAST_COMMIT_TIME`).

| Priority | Source | Trust Level | Description      |
|----------|--------|-------------|------------------|
| 1        | CRM A  | High        | Legacy system    |
| 2        | CRM B  | Medium      | Acquired company |
| 3        | CRM C  | Low         | Call center      |

| Attribute    | Winning Strategy                                                              | Fallback           |
|--------------|-------------------------------------------------------------------------------|--------------------|
| `first_name` | Non-empty (`LENGTH > 1`) → source priority → most recent `row_timestamp`       | Next source        |
| `last_name`  | Non-empty (`LENGTH > 1`) → source priority → most recent `row_timestamp`       | Next source        |
| `email`      | Valid format (`LIKE '%@%'`) → source priority → most recent                    | Next source        |
| `phone`      | Valid length (`>= 7`) → source priority → most recent                          | Next source        |

Address attributes follow the same pattern: street (length >= 5), city (non-null), postal_code (non-null), country (CRM_A priority).

### Data Quality Rules

Base score 100. Error: -20, Warning: -5, Bonus: +5/+10. Clamped 0-100.

| Rule ID | Field       | Rule                     | Severity | Points | Status |
|---------|-------------|--------------------------|----------|--------|--------|
| DQ-001  | email       | Valid email format       | Error    | -20    | Done |
| DQ-002  | email       | Not disposable domain    | Warning  | -5     | Done |
| DQ-003  | first_name  | Not null, length > 1     | Error    | -20    | Done |
| DQ-004  | first_name  | No special characters    | Warning  | -5     | Done |
| DQ-005  | last_name   | Not null, length > 1     | Error    | -20    | Done |
| DQ-006  | last_name   | No special characters    | Warning  | -5     | Done |
| DQ-007  | phone       | Valid phone format       | Warning  | -5     | Done |
| DQ-008  | phone       | Not placeholder          | Error    | -20    | Done |
| DQ-009  | postal_code | Valid format for country | Warning  | -5     | Planned |
| DQ-010  | country     | Valid ISO 3166-1 code    | Error    | -20    | Planned |
| DQ-011  | street      | Minimum length >= 5      | Warning  | -5     | Planned |
| DQ-012  | city        | Not null for addresses   | Error    | -20    | Planned |
| DQ-X03  | first_name, email | Name appears in email | Bonus  | +5    | Done |
| DQ-X04  | street, postal_code, city | Complete address | Bonus | +10 | Planned |
| DQ-C01  | Customer    | Has contact method       | Error    | -20    | Done |
| DQ-C02  | Customer    | Has complete name        | Error    | -20    | Done |
| DQ-AI01 | first_name  | Cortex AI fake name flag | Error    | -20    | Done |

**Tiers:** 90-100 Excellent | 70-89 Good | 50-69 Fair | 0-49 Poor

### SCD Type 2 Design Decision

**All Dynamic Tables.** Since RAW tables are append-only, the full history of golden record versions is always derivable from the source data. SCD2 is computed declaratively:

1. `VW_*_GOLDEN` returns **all versions** per `file_date` (survivorship applied per snapshot)
2. `DT_*_HISTORY` uses `SHA2` row-hash + `LAG()` to detect changes between consecutive versions
3. `VALID_FROM` / `VALID_TO` / `IS_VALID` are computed from version boundaries via `LEAD()`

No stored procedures, no tasks, no imperative DML — just Dynamic Tables with `TARGET_LAG = '1 hour'`.

### Object Inventory (50 objects)

| # | Schema | Object | Type | Definition File |
|---|--------|--------|------|-----------------|
| 1-5 | -- | Database, Warehouse, 3 Schemas | Infra | `pre_deploy.sql` + `infrastructure.sql` |
| 6-11 | `CRM_RAW_001` | 6 Internal Stages (ST_CUSTOMER_A/B/C, ST_ADDRESSES_A/B/C) | Stage | `infrastructure.sql` |
| 12-17 | `CRM_RAW_001` | 6 RAW Tables (TB_CUSTOMER_A/B/C, TB_ADDRESSES_A/B/C) | Table | `raw_tables.sql` |
| 18-21 | `CRM_AGG_001` | VW_CUSTOMER_UNION, VW_ADDRESSES_UNION, VW_CUSTOMER_XREF, VW_ADDRESSES_XREF | View | `views.sql` |
| 22-30 | `CRM_AGG_001` | 9 DTs: ENRICHED, GROUPS, GOLDEN, CUSTOMER, HISTORY (x2 entities) | DT | `analytics.sql` |
| 31-32 | `CRM_SRV_001` | VW_CUSTOMER_360, VW_CUSTOMER_360_FLAT | View | `serve.sql` |
| 33-50 | `CRM_RAW_001` | 6 FF + 6 SM + 6 TS (file formats, streams, tasks) | Post-deploy | `post_deploy.sql` |

### Repository Structure

```
MasterDataManagement/
  manifest.yml                   DCM project manifest (v2, Jinja templated)
  pre_deploy.sql                 Database + schemas (run before DCM plan)
  post_deploy.sql                File formats, streams, tasks (run after DCM deploy)
  sources/
    definitions/
      infrastructure.sql         Warehouse + internal stages (DEFINE)
      raw_tables.sql             6 RAW tables (DEFINE TABLE)
      views.sql                  Union views + XREF views (DEFINE VIEW)
      analytics.sql              9 Dynamic Tables — full pipeline (DEFINE DYNAMIC TABLE)
      serve.sql                  Customer 360 views (DEFINE VIEW)
  sqlunit/
    tests.sqltest                48 automated tests (positive + negative)
  app/
    streamlit_app.py             5-tab Streamlit dashboard
  scripts/
    generate_test_data.py        Synthetic test data generator (3 CRM sources)
  tests/
    test_golden_rules.sql        Survivorship + DQ + SCD2 validation queries
    mdm_showcase.ipynb           E2E test notebook
  .github/
    workflows/
      update-local-repo.yml      CI/CD: analyze → plan → deploy → test
  github-workflow-verification_v1.sh   SHA256 integrity check for workflow
```

### Deployment (DCM)

```bash
# 1. Pre-deploy: database + schemas
snow sql -f pre_deploy.sql -c <connection>

# 2. DCM analyze + plan + deploy
snow dcm raw-analyze MASTER_DATA_MANAGEMENT.CRM_AGG_001.MDM_PROJECT -c <connection> --target DEV
snow dcm plan MASTER_DATA_MANAGEMENT.CRM_AGG_001.MDM_PROJECT -c <connection> --target DEV
snow dcm deploy MASTER_DATA_MANAGEMENT.CRM_AGG_001.MDM_PROJECT -c <connection> --target DEV

# 3. Post-deploy: file formats, streams, tasks
snow sql -f post_deploy.sql -c <connection>

# 4. Upload test data
./upload_data.sh --CONNECTION_NAME=<connection>

# 5. Dashboard
cd app && streamlit run streamlit_app.py
```

---

## Open Points

| ID    | Title                           | Priority | Description                                                                |
|-------|---------------------------------|----------|----------------------------------------------------------------------------|
| OP-05 | Data Governance                 | P3       | PII tags + masking policies. Run `SYSTEM$CLASSIFY` on RAW tables.          |
| OP-06 | DMF Monitoring                  | P3       | Row counts, NULL rates, DQ distribution on golden record DTs.              |
| OP-07 | Multiple Addresses per Customer | P4       | Current 1:1 model. Real MDM needs N addresses per customer.                |
