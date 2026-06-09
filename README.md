# Snowflake-Native Master Data Management

## Why MDM?

Organizations running multiple systems (CRM, ERP, billing) inevitably end up with the same real-world entity -- a customer, product, or account -- scattered as conflicting records across systems. Without MDM:

- **No single source of truth** -- which system has the "correct" email for customer #4521?
- **Unknown data quality** -- how many records have invalid phones or duplicate entries?
- **Invisible history** -- when did this customer's address change, and what was it before?

Master Data Management solves this by **resolving** duplicates across sources, applying **survivorship rules** to pick the best values, computing a **golden record**, and tracking changes over time.

## What is this project?

This repository implements MDM using only Snowflake-native features (batch pipeline) and a lightweight Kafka+Postgres engine (near-real-time pipeline). No commercial MDM platform required.

For a detailed walkthrough of the batch approach, see: [Master Data Management (MDM) with Snowflake Native Features](https://medium.com/@marcel.daeppen_74522/master-data-management-mdm-with-snowflake-native-features-01b27456039f)

Two implementations, same matching logic, same golden record output:

| | Batch (bulk/) | Near-Real-Time (nrt/) |
|---|---|---|
| **Engine** | Snowflake Dynamic Tables | Python + Kafka + Postgres |
| **Latency** | Hourly (DT target_lag) | ~103ms per update |
| **Scale** | 1,500 records (dev) | Tested at 1M records |
| **Deployment** | Snowflake DCM | Docker Compose (local) / SPCS (prod) |

## Repository Structure

```
MasterDataManagement/
├── bulk/                   Batch MDM on Snowflake (Dynamic Tables, DCM, hourly lag)
│   ├── sources/            SQL definitions (DTs, views, serving layers)
│   ├── sqlunit/            SQL unit tests
│   ├── tests/              E2E validation & notebooks
│   ├── app/                Streamlit dashboard
│   ├── pre_deploy.sql      DCM pre-deploy hook
│   └── post_deploy.sql     DCM post-deploy hook
├── nrt/                    Near-Real-Time MDM (Kafka + Postgres, sub-second)
│   ├── src/nrt_mdm/        Python consumer, matching, survivorship, DQ
│   ├── schema/             Postgres DDL
│   ├── app/                Streamlit golden record viewer
│   ├── tests/              Unit + E2E tests + load tests
│   ├── docker-compose.yml  Local dev stack
│   └── Dockerfile          MDM engine container
├── shared/                 Shared between bulk and NRT
│   └── scripts/            Test data generator (1.5K to 1M records)
├── manifest.yml            DCM project manifest (root for tooling compatibility)
└── README.md               This file
```

## Testing (NRT Pipeline)

### Quick Start

```bash
cd nrt/tests
./run_e2e.sh                          # functional test: 1,500 records + single update
```

### Test Modes

| Command | What it does |
|---------|-------------|
| `./run_e2e.sh` | Load 1,500 records, send 1 update, show BEFORE/AFTER/CDC trace |
| `./run_e2e.sh --mode continuous --rate 5` | Continuous updates at 5/sec with latency stats |
| `./run_e2e.sh --transport rest --mode single` | Single update via REST API (synchronous CDC response) |
| `./run_e2e.sh --transport both --mode continuous` | Compare Kafka vs REST latency side-by-side |
| `./run_e2e.sh --scale medium --duration 60` | 100K records + 60s steady-state at 100/sec |
| `./run_e2e.sh --scale large --duration 300` | 1M records + 5min steady-state (load test) |

### Unit Tests

```bash
cd nrt && pytest tests/ -v            # 147 tests: mappers, matching, survivorship, DQ, audit, regression, interfaces
```

### Streamlit Viewers

```bash
docker compose up -d                  # starts all services
open http://localhost:8501             # golden record viewer (browse records, XREF, SCD2 history)
open http://localhost:8502             # audit log viewer (SEC-04: live tail of audit events)
```

### Kafka UI

```bash
open http://localhost:8080             # inspect topics, messages, consumer lag
```

### Kafka Topics

| Topic | Direction | Description |
|-------|-----------|-------------|
| `topic.crm.a` | Inbound | CRM A customer events (src_customer_id, first_name, last_name, email, phone) |
| `topic.crm.b` | Inbound | CRM B customer events (customer_key, name, email_address, mobile) |
| `topic.crm.c` | Inbound | CRM C customer events (ticket_customer_id, caller_name, callback_email, callback_phone) |
| `topic.mdm.golden` | Outbound | Golden record CDC events (INSERT/UPDATE with full record + row_hash) |
| `topic.mdm.xref` | Outbound | XREF assignment events (ASSIGN/REASSIGN source key to customer_id) |
| `topic.mdm.audit` | Outbound | Audit events (SEC-04: ingest, changes, reads) for real-time consumers |

### REST API (port 8000)

Alternative to Kafka for ingest — synchronous request/response with CDC result:

```bash
# Ingest an event and get golden record change in response
curl -X POST http://localhost:8000/api/v1/ingest/crm_a \
  -H "Content-Type: application/json" \
  -d '{"src_customer_id": "A001", "first_name": "Bill", "last_name": "Smith", "email": "bill@acme.com", "phone": "+11043321819"}'

# Response (synchronous, ~32ms):
# {"changed": true, "customer_id": 42, "event_type": "UPDATE", "first_name": "Bill", ...}

# Read endpoints
curl http://localhost:8000/api/v1/customers/42
curl http://localhost:8000/api/v1/customers/42/sources
curl http://localhost:8000/api/v1/customers/42/history
```

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/ingest/{source}` | POST | Ingest event, return CDC synchronously |
| `/api/v1/customers/{id}` | GET | Current golden record |
| `/api/v1/customers/{id}/sources` | GET | Source records in cluster |
| `/api/v1/customers/{id}/history` | GET | SCD2 history |
| `/api/v1/health` | GET | Health check |

### NRT Pipeline Latency (1M records)

```
produce to Kafka ─────> consumer polls ─> map ─> UPSERT ─> resolve ─> survivorship ─> DQ ─> SCD2 write ─> done
       |                                                                                                   |
       └────────────────────────────────────────── ~100ms ─────────────────────────────────────────────────┘
```

To consume events live in a terminal:
```bash
docker exec mdm-nrt-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topic.mdm.golden
```

## Batch Pipeline (bulk/)

The batch pipeline merges **1,500 customer records and their addresses from 3 CRM systems** (600+400+500) into **1,115 golden customer records**, achieving a 24% merge rate, weighted DQ scoring, and full SCD2 change history.

- **Engine:** Snowflake Dynamic Tables with DCM deployment
- **Two variants:** AI pipeline (Cortex-powered nickname resolution) and FUZZY pipeline (classical matching, zero AI cost)
- **Deployment:** `snow dcm deploy MDM_DEV.MDM_DCM.MDM_PROJECT -c <connection> --target DEV`

For full details (architecture, matching rules, survivorship, DQ scoring), see [MDM_SPEC_Bulk.md](MDM_SPEC_Bulk.md).

## NRT Pipeline (nrt/)

The near-real-time pipeline processes single Kafka events through entity resolution in ~103ms at 1M records.

- **Engine:** Python consumer + Postgres + Kafka
- **Matching:** 6 rules (email exact, phone normalized, canonical name, Jaro-Winkler, SOUNDEX, email domain + name)
- **Threshold:** Combined score >= 0.70 to merge
- **Golden record:** Survivorship (completeness > source priority > recency) + 11 DQ rules + SCD2 history
- **CDC:** Golden record and XREF changes published to outbound Kafka topics
- **Deployment:** `docker compose up -d` (local) / SPCS (production)

For full details (requirements, architecture, processing guarantees), see [MDM_SPEC_Near-Real-Time.md](MDM_SPEC_Near-Real-Time.md).

| Source | Description | Trust | Records |
|--------|-------------|-------|---------|
| CRM_A | Legacy system | 1 (highest) | 600-400K |
| CRM_B | Acquired company | 2 | 400-350K |
| CRM_C | Call center | 3 (lowest) | 500-250K |
