"""NRT MDM REST API — Synchronous ingest + golden record queries.

Accepts inbound customer events via HTTP POST, processes through the full
MDM pipeline, and returns the CDC result synchronously in the response.

Run:
  uvicorn nrt_mdm.api:app --host 0.0.0.0 --port 8000
"""

import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import psycopg
from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException

from nrt_mdm.pipeline import process_event

logger = logging.getLogger(__name__)

POSTGRES_DSN = os.environ.get("POSTGRES_DSN", "postgresql://mdm:mdm@localhost:5432/mdm")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# Shared resources (initialized on startup)
_pg_conn = None
_producer = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize Postgres connection and Kafka producer on startup."""
    global _pg_conn, _producer
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    _pg_conn = psycopg.connect(POSTGRES_DSN, autocommit=False)
    _producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "acks": "all",
        "enable.idempotence": True,
    })
    logger.info("API started: Postgres and Kafka connected")
    yield
    _pg_conn.close()
    logger.info("API stopped")


app = FastAPI(
    title="NRT MDM API",
    description="Synchronous MDM pipeline: ingest events and get golden record CDC in the response.",
    version="0.1.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Ingest endpoint (POST)
# ---------------------------------------------------------------------------

VALID_SOURCES = {"crm_a", "crm_b", "crm_c"}


@app.post("/api/v1/ingest/{source_system}")
def ingest(source_system: str, payload: dict):
    """Ingest a customer event and return the CDC result synchronously.

    The event is processed through the full MDM pipeline:
    map -> UPSERT -> resolve -> survivorship -> DQ -> SCD2 -> CDC

    The same change is also published to Kafka outbound topics.
    """
    if source_system.lower() not in VALID_SOURCES:
        raise HTTPException(status_code=400, detail=f"Invalid source_system: {source_system}. Valid: {VALID_SOURCES}")

    event_ts = datetime.now(timezone.utc)

    try:
        result = process_event(
            source_system=source_system.lower(),
            payload=payload,
            event_ts=event_ts,
            pg_conn=_pg_conn,
            producer=_producer,
        )
    except Exception as e:
        _pg_conn.rollback()
        logger.exception("Error processing event")
        raise HTTPException(status_code=500, detail=str(e))

    return result


# ---------------------------------------------------------------------------
# Read endpoints (GET)
# ---------------------------------------------------------------------------

@app.get("/api/v1/customers/{customer_id}")
def get_customer(customer_id: int):
    """Get the current golden record for a customer."""
    with _pg_conn.cursor() as cur:
        cur.execute("""
            SELECT cluster_id, first_name, last_name, email, phone,
                   dq_score, source_count, row_hash, valid_from
            FROM golden_customers
            WHERE cluster_id = %s AND is_current = TRUE
        """, (customer_id,))
        row = cur.fetchone()

    if row is None:
        raise HTTPException(status_code=404, detail=f"Customer {customer_id} not found")

    return {
        "customer_id": row[0],
        "first_name": row[1],
        "last_name": row[2],
        "email": row[3],
        "phone": row[4],
        "dq_score": row[5],
        "source_count": row[6],
        "row_hash": row[7],
        "valid_from": row[8].isoformat() if row[8] else None,
    }


@app.get("/api/v1/customers/{customer_id}/sources")
def get_sources(customer_id: int):
    """Get all source records belonging to a customer's cluster."""
    with _pg_conn.cursor() as cur:
        cur.execute("""
            SELECT sc.source_system, sc.source_key, sc.first_name, sc.last_name,
                   sc.email, sc.phone, sc.event_timestamp
            FROM source_customers sc
            JOIN customer_clusters cc ON sc.source_system = cc.source_system AND sc.source_key = cc.source_key
            WHERE cc.cluster_id = %s
            ORDER BY sc.source_system, sc.source_key
        """, (customer_id,))
        rows = cur.fetchall()

    if not rows:
        raise HTTPException(status_code=404, detail=f"No sources for customer {customer_id}")

    return [
        {
            "source_system": r[0], "source_key": r[1],
            "first_name": r[2], "last_name": r[3],
            "email": r[4], "phone": r[5],
            "event_timestamp": r[6].isoformat() if r[6] else None,
        }
        for r in rows
    ]


@app.get("/api/v1/customers/{customer_id}/history")
def get_history(customer_id: int):
    """Get SCD2 history for a customer."""
    with _pg_conn.cursor() as cur:
        cur.execute("""
            SELECT id, first_name, last_name, email, phone, dq_score, source_count,
                   row_hash, valid_from, valid_to, is_current
            FROM golden_customers
            WHERE cluster_id = %s
            ORDER BY valid_from DESC
        """, (customer_id,))
        rows = cur.fetchall()

    if not rows:
        raise HTTPException(status_code=404, detail=f"No history for customer {customer_id}")

    return [
        {
            "id": r[0], "first_name": r[1], "last_name": r[2],
            "email": r[3], "phone": r[4], "dq_score": r[5], "source_count": r[6],
            "row_hash": r[7],
            "valid_from": r[8].isoformat() if r[8] else None,
            "valid_to": r[9].isoformat() if r[9] else None,
            "is_current": r[10],
        }
        for r in rows
    ]


@app.get("/api/v1/health")
def health():
    """Health check."""
    return {"status": "ok"}
