# MDM Shared Business Rules

## Marcel Daeppen | Principal Solutions Engineer | Snowflake | EMEA Growth Markets

This document defines the canonical business rules shared by both the Bulk (Snowflake-native) and NRT (Near-Real-Time) MDM pipelines. Both pipeline specs reference this file as the single source of truth for matching, survivorship, DQ scoring, and source definitions.

**Referenced by:**
- [MDM_SPEC_Bulk.md](MDM_SPEC_Bulk.md) — Batch/Snowflake-native pipeline
- [MDM_SPEC_Near-Real-Time.md](MDM_SPEC_Near-Real-Time.md) — NRT/SPCS pipeline

---

## Why Master Data Management

### Business Problems Without MDM

Running multiple CRMs (e.g., legacy, acquired company, call center) without MDM typically leads to:

- **No single source of truth** -- The same customer exists multiple times across systems with different names, addresses, emails, or statuses. Nobody can answer confidently "How many customers do we have?"
- **Poor customer experience and sales effectiveness** -- Sales and service agents cannot see full interaction history. Marketing sends duplicate or inconsistent messages.
- **Operational inefficiency and higher cost-to-serve** -- Manual de-duplication, spreadsheet reconciliations, and one-off extracts. Every project spends time "fixing the same data again."
- **Risk and compliance exposure** -- Difficulty proving which customer data was used for decisions at a given point in time. Hard to implement GDPR/CCPA rights or AML/KYC controls consistently when customer identity is ambiguous.
- **Limited value from AI and analytics** -- Models trained on fragmented or low-quality customer data underperform. Customer 360, churn, cross-sell, and risk models all depend on clean, unified master data.

### Target Business Outcomes

By introducing MDM for CRM customer data, we aim to achieve:

- **Customer 360 for sales, service, and marketing** -- One golden customer record with interaction history and a data quality score. Better targeting, fewer duplicates, higher conversion.
- **Trustworthy reporting and regulatory submissions** -- Consistent customer identifiers across CRM, billing, risk, and finance. Transparent change history (SCD Type 2) to reconstruct "what we knew when."
- **More efficient operations** -- Automated match-merge and survivorship reduce manual data cleansing. Standardized data quality scores shift effort from firefighting to continuous improvement.
- **Stronger foundation for AI and analytics** -- A robust, governed customer master data layer becomes the backbone for AI use cases (personalization, credit risk, fraud detection, marketing optimization).

---

## Scope

### In Scope (CRM Showcase)

- Customer master data -- core identity and contact attributes.
- Address master data -- a single primary address per customer (Bulk: active; NRT: Phase 3 / BIZ-13).
- Entity resolution -- identifying which CRM records belong to the same real-world customer.
- Survivorship -- deciding which attribute values win when multiple sources disagree.
- Data quality (DQ) scoring -- standardized, rule-based 0-100 score per golden customer record.
- Customer 360 view -- serving layer providing analytics-ready and API-ready views.

### Out of Scope (first iteration)

- Additional domains (Product, Account, Household, Organization).
- Stewardship UI and manual workflows (Phase 2+).
- API integration layer and advanced governance (consent, retention) beyond basic tagging/masking.
- Multiple addresses and complex hierarchies (N:M) -- the current model is 1:1 customer-address.

---

## Key MDM Concepts

See also the [Glossary](#glossary) below for term definitions.

- **Entity resolution** — Identifying and grouping records that refer to the same real-world customer across sources.
- **Golden customer record** — The single authoritative master record for a customer after survivorship rules are applied across all matched source records.
- **Survivorship** — The ordered set of rules that determine which source value wins per attribute (e.g., completeness -> source trust -> recency).
- **Data quality (DQ) score** — A 0-100 score per golden customer record based on field-level and cross-field validation rules (errors, warnings, bonuses) that quantifies how trustworthy each record is.
- **Blocking** — Pre-filtering step that limits candidate pairs for matching, avoiding O(n^2) full comparison.
- **Match suppression** — A pair of source records explicitly marked as "do not merge," created by admin unmatch operations.

### End-to-End MDM Process

The MDM process follows four logical steps (both pipelines use the same sequence):

1. **Union and harmonize** — Standardize schemas and formats across CRM_A/B/C into a common customer schema. Normalize names, casing, phone formats, and email structures.
2. **Enrich and screen** — Apply enrichment rules: normalize nicknames to canonical names, flag fake or test names. (Bulk uses Cortex AI; NRT uses static rules or skips — see [Intentional Differences](#intentional-differences-between-pipelines).)
3. **Group (entity resolution)** — Group records that represent the same real-world customer into a common cluster ID using blocking, deterministic matching (email, phone), and probabilistic matching (name similarity, address). Each group corresponds to one golden customer record.
4. **Survive and score** — For each attribute within a group, apply survivorship rules to choose the best value and build the golden customer record. Run DQ rules to compute a DQ score and tier.

The key idea: many noisy CRM records go in; one trusted, scored golden customer record comes out, with a full change history.

---

## History and Lineage

To support analytics, regulatory, and audit needs, both pipelines maintain:

- **Current golden customer records** -- the latest version of each consolidated entity.
- **SCD Type 2 history** -- every change to a golden record is preserved with `valid_from`, `valid_to`, and `is_current` columns. Row-hash change detection (SHA256) ensures only actual changes create new history rows.

This gives the business:
- Full auditability of master data changes over time.
- The ability to reconstruct regulatory reports or decisions exactly as they were at any historical point.

**Implementation differs by pipeline:**
- **Bulk:** Declarative SCD2 via Dynamic Tables using SHA2 row-hash + LAG() with validity ranges.
- **NRT:** Procedural SCD2 in Postgres transactions: close previous row (UPDATE valid_to, is_current=FALSE), insert new row.

---

## Glossary

| Term | Definition |
|------|-----------|
| Source key | The primary key in the source CRM system (e.g., `A001`, `B-2047`). Immutable after first ingest. |
| Cluster ID | An internally assigned group identifier representing "these source records are the same real-world person." |
| Customer ID | Alias for cluster_id in outbound-facing APIs and Snowflake target tables. |
| Golden record | The single best-of-breed record computed from all source records in a cluster via survivorship rules. |
| Current row | The golden record row with `is_current = TRUE`. There is exactly one current row per cluster_id at any time. |
| History row | A closed golden record row (`is_current = FALSE`, `valid_to < '9999-12-31'`). SCD2 pattern preserves the full change history. |
| Blocking | Pre-filtering step that limits candidate pairs for matching. Avoids O(n^2) full comparison. |
| Match score | Composite score (0.0 to 1.0+) from deterministic and probabilistic rules. Scores above the merge threshold trigger cluster merge. |
| Survivorship | Attribute-level decision logic: for each field in a golden record, pick the "best" value from all source records in the cluster. |
| DQ score | Data quality score (0-100) computed on the golden record after survivorship. Higher is better. |
| XREF | Cross-reference table mapping every (source_system, source_key) to a customer_id (cluster_id). |
| Match suppression | A pair of source records explicitly marked as "do not merge" (created by admin unmatch). |

### Temporal and Pipeline Terms

| Term | Definition |
|------|-----------|
| `event_timestamp` | When the source system created or changed the record. NRT: extracted from Kafka message timestamp (`msg.timestamp()`). Bulk: derived from `file_date` in `_SOURCE_FILE` metadata. Used as the recency tiebreaker in survivorship. |
| `ingested_at` | When the MDM engine received and processed the record. NRT: Postgres `NOW()` at UPSERT time. Bulk: Dynamic Table refresh timestamp. |
| `file_date` | Bulk pipeline: the date extracted from the source file path/metadata. Equivalent to `event_timestamp` for batch ingestion. |

---

## Source CRM Definitions

### Source Systems and Trust Hierarchy

| Source | System Name | Trust Level | Description |
|--------|------------|-------------|-------------|
| CRM_A | Legacy CRM | 1 (highest) | Primary customer system of record |
| CRM_B | Acquired CRM | 2 | Data from business acquisition |
| CRM_C | Call Center | 3 (lowest) | Customer-reported data via support interactions |

Trust level is used as the second-priority tiebreaker in survivorship (after completeness, before recency).

### Source Field Mappings

| CRM_A | CRM_B | CRM_C | Common Field |
|-------|-------|-------|-------------|
| `src_customer_id` | `customer_key` | `ticket_customer_id` | source_key |
| `first_name` | `name` (full, split) | `caller_name` (full, split) | first_name |
| `last_name` | (split from name) | (split from caller_name) | last_name |
| `email` | `email_address` | `callback_email` | email |
| `phone` | `mobile` | `callback_phone` | phone |

**Normalization applied during mapping:**
- Email: `LOWER(TRIM(...))`
- Phone: `REGEXP_REPLACE` to strip non-numeric characters, normalize to digits-only
- Name splitting: CRM_B and CRM_C provide full names; split on first space into first_name / last_name

---

## Matching Rule Catalog

### Blocking Keys

Blocking keys create candidate pairs without full O(n^2) comparison. Candidates are retrieved via OR-joined query across three blocking indexes:

| Block Key | Column | Algorithm | Example |
|-----------|--------|-----------|---------|
| `block_soundex` | last_name | Soundex (jellyfish library) | "Smith" -> "S530" |
| `block_email_domain` | email | Substring from `@` onwards | "john@acme.com" -> "@acme.com" |
| `block_phone_suffix` | phone | Last 4 digits | "+41791234567" -> "4567" |

Each blocking key has an IS NOT NULL guard. A candidate pair is generated if ANY blocking key matches.

### Deterministic Rules (take MAX score)

| Rule ID | Field | Condition | Score |
|---------|-------|-----------|-------|
| MATCH-D01 | email | Exact equality (case-normalized) | 1.00 |
| MATCH-D02 | phone | Last 10 digits equality (normalized) | 0.95 |
| MATCH-C01 | canonical_first_name + last_name | Exact equality (case-insensitive) | 0.80 |

### Probabilistic Rules (SUM all that fire)

| Rule ID | Fields | Condition | Score |
|---------|--------|-----------|-------|
| MATCH-P01 | first_name + last_name | Full name Jaro-Winkler >= 0.85 | jw_score * 0.30 |
| MATCH-P02 | street + postal_code | Street Jaro-Winkler + postal exact | 0.25 (Phase 3 -- requires BIZ-13) |
| MATCH-P03 | last_name | SOUNDEX equality | 0.20 |
| MATCH-P04 | email domain + first_name | Same email domain + first name JW >= 0.90 | 0.15 |
| MATCH-P05 | phone + city | Phone last-7 match + same city | 0.10 (Phase 3 -- requires BIZ-13) |

### Scoring Formula

```
final_score = MAX(MATCH-D01, MATCH-D02, MATCH-C01) + SUM(MATCH-P01, MATCH-P03, MATCH-P04)
```

Phase 3 adds: `+ SUM(MATCH-P02, MATCH-P05)` when address data is available.

### Merge Threshold

| Score | Action |
|-------|--------|
| >= 0.70 | Auto-merge into same cluster |
| < 0.70 | No merge (separate clusters) |

**Library:** `jellyfish` (Python) for Jaro-Winkler and Soundex. Bulk pipeline uses equivalent Snowflake SQL functions.

---

## Survivorship Rules

For each attribute in the golden record, the best value is selected from all source records in the cluster using the following priority cascade:

### Priority Order

1. **Completeness** -- non-null, valid format, sufficient length
2. **Source trust** -- CRM_A (1) > CRM_B (2) > CRM_C (3)
3. **Recency** -- most recent `event_timestamp` (NRT) or `file_date` (Bulk) wins

### Per-Attribute Rules

| Attribute | Completeness Check | Source Trust | Recency |
|-----------|-------------------|-------------|---------|
| first_name | Non-null, length > 1 | CRM_A > CRM_B > CRM_C | Most recent |
| last_name | Non-null, length > 1 | CRM_A > CRM_B > CRM_C | Most recent |
| email | Valid format (contains `@`) | CRM_A > CRM_B > CRM_C | Most recent |
| phone | Valid length (>= 7 digits) | CRM_A > CRM_B > CRM_C | Most recent |
| address | Non-null, street length > 3, city non-null, postal non-null | CRM_A > CRM_B > CRM_C | Most recent |

**Address survivorship** applies when BIZ-13 (Phase 3) is implemented. Until then, address fields are not part of the golden record.

### Implementation

- **Bulk pipeline:** `FIRST_VALUE()` with `PARTITION BY customer_id ORDER BY completeness_rank, trust_level, file_date DESC`
- **NRT pipeline:** Python `pick_best()` function with the same priority cascade

### Example

Three source records for the same person:
- CRM_A (trust 1, date 2024-01): `first_name="Bill"`, `email="bill@acme.com"`, `phone=NULL`
- CRM_B (trust 2, date 2024-03): `first_name="William"`, `email=NULL`, `phone="+41791234567"`
- CRM_C (trust 3, date 2024-06): `first_name=NULL`, `email="w.smith@gmail.com"`, `phone="+41791234567"`

Golden record result:
- `first_name` = "William" (CRM_B: non-null, length>1, trust 2 beats CRM_C's null)
- `email` = "bill@acme.com" (CRM_A: valid, trust 1)
- `phone` = "+41791234567" (CRM_B: valid, trust 2 beats CRM_C's trust 3)

---

## DQ Rule Catalog

### Scoring Model

Base score = **100**. Apply penalties and bonuses. Clamp result to **0-100**.

### Penalty Rules

| Rule ID | Condition | Score | Severity |
|---------|-----------|-------|----------|
| DQ-001 | Invalid email format (no `@` or malformed) | -20 | Error |
| DQ-002 | Disposable email domain (e.g., mailinator.com) | -5 | Warning |
| DQ-003 | Missing or short first_name (length <= 1) | -20 | Error |
| DQ-004 | Special characters in first_name | -5 | Warning |
| DQ-005 | Missing or short last_name (length <= 1) | -20 | Error |
| DQ-006 | Special characters in last_name | -5 | Warning |
| DQ-007 | Invalid phone format (< 7 digits after normalization) | -5 | Warning |
| DQ-008 | Placeholder phone (0000000000, 1111111111, etc.) | -20 | Error |
| DQ-C01 | No contact method (no valid email AND no valid phone) | -20 | Error (compound) |
| DQ-C02 | No complete name (both first and last missing/invalid) | -20 | Error (compound) |

### Bonus Rules

| Rule ID | Condition | Score |
|---------|-----------|-------|
| DQ-X03 | Name appears in email (first_name or last_name substring of email local part) | +5 |
| DQ-X04 | Complete address fields (street, city, postal, country all non-null) | +10 (Phase 3) |

### AI-Enriched Rules (Bulk pipeline only)

| Rule ID | Condition | Score | Pipeline |
|---------|-----------|-------|----------|
| DQ-AI01 | Fake/suspicious name detected by AI_CLASSIFY | -20 | Bulk only (Cortex AI) |

NRT explicitly skips AI-based DQ rules due to sub-millisecond latency constraints. See Intentional Differences section.

### DQ Tiers

| Score Range | Tier | Business Usage |
|-------------|------|---------------|
| 90-100 | Excellent | Suitable for all downstream use (marketing, compliance, analytics) |
| 70-89 | Good | Suitable for most use cases; minor data gaps acceptable |
| 50-69 | Fair | Usable with caveats; may need manual review |
| < 50 | Poor | Requires data stewardship intervention before use |

**Business thresholds:**
- Marketing campaigns: DQ >= 80 (Good or better)
- Compliance reporting: DQ >= 70 (Good or better)
- Stewardship queue: DQ < 50 (Poor -- flag for review)

---

## Golden Record Schema (Canonical Fields)

The golden record contains these core fields regardless of pipeline:

| Field | Type | Description |
|-------|------|-------------|
| customer_id / cluster_id | BIGINT | Unique identifier for the consolidated entity |
| first_name | VARCHAR(200) | Best surviving first name |
| last_name | VARCHAR(200) | Best surviving last name |
| email | VARCHAR(255) | Best surviving email |
| phone | VARCHAR(50) | Best surviving phone |
| dq_score | SMALLINT | Data quality score (0-100) |
| source_count | SMALLINT | Number of source records in the cluster |
| row_hash | VARCHAR(128) | SHA256 of (first_name, last_name, email, phone, dq_score) for CDC |
| valid_from | TIMESTAMPTZ | SCD2: when this version became current |
| valid_to | TIMESTAMPTZ | SCD2: when this version was superseded (9999-12-31 if current) |
| is_current | BOOLEAN | TRUE for the latest version |

**Snowflake target table naming:**
- Bulk pipeline: `CRMA_AGG_DT_CUSTOMER` (current), `CRMA_AGG_DT_CUSTOMER_HISTORY` (SCD2)
- NRT pipeline: `CRMA_NRT_TB_CUSTOMER` (current), `CRMA_NRT_TB_CUSTOMER_HISTORY` (SCD2)

Both use the same schema. The prefix distinguishes pipeline origin for downstream consumers.

---

## Intentional Differences Between Pipelines

These are deliberate design decisions, not bugs or drift.

| Topic | Bulk Pipeline | NRT Pipeline | Reason |
|-------|-------------|-------------|--------|
| AI enrichment | Cortex COMPLETE (nickname canonicalization) + AI_CLASSIFY (fake name detection) | Not used | NRT requires sub-millisecond per-event latency; model inference is too slow |
| canonical_first_name | AI-computed via Cortex COMPLETE | Phase 1: defaults to `LOWER(TRIM(first_name))`. Phase 2: lightweight static lookup table for common nicknames (Bill->William, etc.) | NRT matching uses static fallback; Bulk uses AI. Parity gap on uncommon nicknames. |
| Address pipeline | In scope (street, city, postal, country) | Phase 3 (BIZ-13) | Deferred for NRT MVP |
| DQ-AI01 (fake name) | Active (-20 penalty) | Skipped | No AI inference in hot path |
| DQ-X04 (address bonus) | Active (+10) | Phase 3 | Depends on BIZ-13 |
| Table naming | `CRMA_AGG_DT_*` | `CRMA_NRT_TB_*` | Intentional namespacing to distinguish pipeline origin |
| Computation model | Declarative SQL (Dynamic Tables, TARGET_LAG) | Procedural Python (SPCS containers) | Architectural choice: Bulk=Snowflake-native, NRT=containerized for Kafka integration |
| SCD2 implementation | SHA2 row-hash + LAG() in Dynamic Table | close/insert in Postgres transaction | Same semantics, different execution |
| Match suppressions | Not yet implemented | BIZ-15 (Done) | Bulk should honor NRT suppressions -- pending integration |
| XREF table | Not explicitly maintained | customer_xref table (BIZ-12, Done) | Bulk should adopt for lineage/audit |
| Record count (1,500 test set) | 1,115 golden records | ~1,113 golden records | See Parity & Tolerances section below |

---

## Cross-Pipeline Parity and Tolerances

### Expected Record Variance

From the same 1,500-record test set, the Bulk pipeline produces 1,115 golden records while NRT produces ~1,113. The ~0.2% variance (2 records) stems from differences in canonical name handling:

- **Bulk** uses Cortex AI (`CORTEX.COMPLETE`) to normalize nicknames (e.g., "Bill" -> "William"), enabling MATCH-C01 (canonical name match) to fire on pairs that NRT misses.
- **NRT Phase 1** uses `LOWER(TRIM(first_name))` as a static fallback, which does not resolve nicknames. These boundary-score pairs fall just below the 0.70 merge threshold.

This variance is acceptable for Phase 1. It will narrow when NRT Phase 2 adds a static nickname lookup table.

### Parity Tolerance Thresholds

| Metric | Acceptable Variance | Action if Exceeded |
|--------|--------------------|--------------------|
| Golden record count | +/- 0.5% (same test set) | Investigate scoring formula or threshold drift |
| DQ score distribution | Average within +/- 2 points | Verify DQ rule parity |
| Merge rate | Within +/- 1% | Check blocking key coverage |

### Parity Gaps Requiring Action

1. **canonical_first_name**: NRT Phase 1 uses `LOWER(TRIM(first_name))` as a static fallback. Phase 2 should add a lightweight nickname lookup table (e.g., Bill->William, Bob->Robert) to improve matching parity with Bulk's Cortex AI canonicalization.
2. **Match suppressions**: Bulk matching SQL should query `match_suppressions` table and exclude suppressed pairs.
3. **XREF in Bulk**: Bulk pipeline should produce a cross-reference table for lineage parity with NRT.
