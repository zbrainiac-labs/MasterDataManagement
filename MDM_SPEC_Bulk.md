# MDM Spec - Bulk: Snowflake native


## Marcel Däppen | Principal Solutions Engineer | Snowflake | EMEA Growth Markets

## **Executive Summary**

Through M\&A and other historical changes, organizations often end up running multiple systems in parallel (CRM, ERP, billing, support, etc.) that all store overlapping information about the same real‑world entities. The result is fragmented and conflicting master data: duplicates, inconsistent attributes, missing history, and no single, trusted view across channels and lines of business.

This showcase explains why we need Master Data Management (MDM), what we aim to deliver, and how we implement it using CRM (customer relationship data) as a concrete example. The centerpiece is a **golden customer record** per real-world customer -- including a linked primary address -- assembled from three CRM systems into a single, trusted view with transparent logic for matching, survivorship, and data quality scoring.

**Why MDM and Target Business Outcomes:** See [MDM_SPEC_Shared.md](MDM_SPEC_Shared.md#why-master-data-management) for business problems, target outcomes, and key MDM concepts that apply across both pipelines.

**Scope, Key Concepts, and Glossary:** See [MDM_SPEC_Shared.md](MDM_SPEC_Shared.md#scope) for what is in/out of scope, key MDM concepts, and term definitions.

## **CRM Showcase: From Fragmented CRM Data to Golden Customer Records**

### Baseline Situation

In the showcase, we ingest 1,500 customer + address records from three CRM systems (see [MDM_SPEC_Shared.md](MDM_SPEC_Shared.md#source-systems-and-trust-hierarchy) for source definitions and trust hierarchy):

- CRM_A -- 600 customers
- CRM_B -- 400 customers
- CRM_C -- 500 customers

These are merged into **1,115 golden customer records** with 1:1 linked primary addresses, yielding a **24.4% merge rate**, a total of **272 merged customers**, and an average **DQ score of 95**, with **973 records** in the “Excellent” DQ tier.

This demonstrates that even relatively “clean” CRM landscapes can hide a significant amount of duplication and inconsistency.

**End-to-End MDM Process:** See [MDM_SPEC_Shared.md](MDM_SPEC_Shared.md#end-to-end-mdm-process) for the four-step pipeline (Union & harmonize -> Enrich & screen -> Group -> Survive & score) used by both Bulk and NRT pipelines. The Bulk pipeline executes this via Snowflake Dynamic Tables; the NRT pipeline executes it in Python within SPCS containers.

## **How the Golden Customer Record Is Built**

This section describes how we decide which values end up in the golden customer record and why.

### Business Rules Reference

All matching, survivorship, and DQ scoring rules are defined in [MDM_SPEC_Shared.md](MDM_SPEC_Shared.md). This section covers only the Bulk-specific implementation.

**Matching and grouping:** Entity resolution uses the shared matching rule catalog. Bulk implementation: SQL CTEs with `DENSE_RANK()` for cluster assignment in `VW_CUSTOMER_GROUPS`. Cortex AI provides `canonical_first_name` (nickname normalization) and `is_fake_name` (fake detection) upstream in `DT_CUSTOMER_ENRICHED`.

> See [MDM_SPEC_Shared.md: Matching Rule Catalog](MDM_SPEC_Shared.md#matching-rule-catalog)

**Survivorship:** Bulk implementation uses `FIRST_VALUE()` with `PARTITION BY customer_id ORDER BY completeness_rank, trust_level, file_date DESC` in `DT_CUSTOMER_GOLDEN`.

> See [MDM_SPEC_Shared.md: Survivorship Rules](MDM_SPEC_Shared.md#survivorship-rules)

**DQ scoring:** Bulk applies all shared DQ rules plus AI-based rules (DQ-AI01 fake name detection via Cortex `AI_CLASSIFY`). In the CRM showcase, 973 of 1,115 golden records land in the "Excellent" tier.

> See [MDM_SPEC_Shared.md: DQ Rule Catalog](MDM_SPEC_Shared.md#dq-rule-catalog) and [Intentional Differences](MDM_SPEC_Shared.md#intentional-differences-between-pipelines)

**History and Lineage:** See [MDM_SPEC_Shared.md](MDM_SPEC_Shared.md#history-and-lineage) for the SCD2 history and auditability concepts shared across both pipelines.

## **The Implementation (Technical View)**

From a technical perspective, the CRM MDM showcase is implemented entirely with native Snowflake capabilities.

### Dataflow

At a high level, the dataflow is:

- RAW tables → UNION ALL → Cortex enrichment → Match \+ group → Survivorship → Dynamic Tables \+ SCD2 history Dynamic Tables.

\-- DATAFLOW:

\--   RAW Tables → Union ALL → Cortex Enrich → Match+Group → Survive → DT \+ SCD2 DT

\--

\--   ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐

\--   │ **TB\_CUSTOMER\_A**    │  │ **TB\_CUSTOMER\_B**    │  │ **TB\_CUSTOMER\_C**    │  

\--   │ (CRM\_RAW\_001)    │  │ (CRM\_RAW\_001)    │  │ (CRM\_RAW\_001)    │

\--   └────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘

\--            │                     │                     │

\--            └─────────┬───────────┼─────────────────────┘

\--                      ▼

\--            ┌──────────────────────────┐

\--            │ **VW\_CUSTOMER\_UNION**        │  ← ALL records (no ROW\_NUMBER filter)

\--            │ \- Standardize columns    │     \+ file\_date from \_SOURCE\_FILE

\--            │ \- CRM\_A: first, last     │     email: LOWER(TRIM())

\--            │ \- CRM\_B: SPLIT name      │     phone: REGEXP\_REPLACE

\--            └────────────┬─────────────┘

\--                         ▼

\--            ┌──────────────────────────┐

\--            │ **DT\_CUSTOMER\_ENRICHED**     │  ← Cortex AI Enrichment (DT, materialized)

\--            │ \- canonical\_first\_name   │     CORTEX.COMPLETE nickname→formal

\--            │ \- is\_fake\_name           │     AI\_CLASSIFY real vs fake

\--            └────────────┬─────────────┘

\--                         ▼

\--            ┌──────────────────────────┐

\--            │ **VW\_CUSTOMER\_GROUPS**       │  ← Entity Resolution \+ Clustering

\--            │ Matching (CTE):          │     Uses canonical\_first\_name

\--            │ \- Email, Phone, Name     │     MATCH-D01: Email (1.0)

\--            │ \- SOUNDEX, Jaro-Winkler  │     MATCH-D02: Phone (0.95)

\--            │ Grouping:                │     MATCH-P: Probabilistic (0.70)

\--            │ \- Assign customer\_id     │     DENSE\_RANK() over cluster

\--            └────────────┬─────────────┘

\--                         ▼

\--            ┌──────────────────────────┐

\--            │ **DT\_CUSTOMER\_GOLDEN**       │  ← Survivorship per file\_date (DT)

\--            │ Survivorship:            │     FIRST\_VALUE() partitioned by

\--            │ \- first\_name: non-empty  │     (customer\_id, file\_date)

\--            │ \- email: valid \+ CRM\_A   │     Returns ALL versions

\--            │ \- phone: longest         │     DQ Score: weighted 0-100

\--            └────────────┬─────────────┘

\--                         ▼

\--       ┌─────────────────┴─────────────────┐

\--       ▼                                   ▼

\-- ┌──────────────────┐            ┌──────────────────────────────┐

\-- │ **DT\_CUSTOMER**      │            │ **DT\_CUSTOMER\_HISTORY** (DT)     │

\-- │ (Current State)  │            │ SHA2 row-hash \+ LAG()        │

\-- │ QUALIFY latest   │            │ Declarative SCD2             │

\-- │ TARGET\_LAG=1hr   │            │ valid\_from, valid\_to         │

\-- └──────────────────┘            └──────────────────────────────┘

Key components include:

- **RAW tables:** TB\_CUSTOMER\_A, TB\_CUSTOMER\_B, TB\_CUSTOMER\_C (per CRM system).  
- **Unified view:** VW\_CUSTOMER\_UNION (standardized columns, harmonized formats, file\_date from \_SOURCE\_FILE).  
- **Enrichment Dynamic Table:** CRMA\_AGG\_DT\_CUSTOMER\_ENRICHED (canonical first name via CORTEX.COMPLETE, fake‑name detection via AI\_CLASSIFY).  
- **Matching & grouping view:** CRMA\_AGG\_VW\_CUSTOMER\_GROUPS (deterministic and probabilistic matching with Jaro‑Winkler, SOUNDEX, email/phone rules; cluster assignment into customer groups).  
- **Survivorship \+ DQ Dynamic Table:** CRMA\_AGG\_DT\_CUSTOMER\_GOLDEN (attribute‑level survivorship logic and weighted DQ scoring rules).  
- **Golden customer Dynamic Tables:**  
  - CRMA\_AGG\_DT\_CUSTOMER – current golden customer records (latest only).  
  - CRMA\_AGG\_DT\_CUSTOMER\_HISTORY – SCD Type 2 history using SHA2 row‑hash to detect changes and maintain VALID\_FROM / VALID\_TO ranges.

The result is a repeatable, explainable MDM pipeline implemented with SQL, Dynamic Tables, and Cortex AI functions, without requiring a separate MDM application.

## Summary

From a business point of view, MDM is not an IT project; it is a strategic capability to:

- Establish one trusted customer across systems.  
- Improve customer experience and sales effectiveness.  
- Reduce operational and regulatory risk.  
- Unlock AI‑driven and analytics‑driven value from CRM and beyond.

The CRM showcase on Snowflake proves that we can deliver all core MDM capabilities — entity resolution, survivorship, golden customer records, data quality scoring, and full history — using native platform features, with concrete, explainable logic for how each golden customer record is constructed and governed over time.