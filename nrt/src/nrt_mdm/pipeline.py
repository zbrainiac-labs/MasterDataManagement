"""Core MDM pipeline: shared processing logic for Kafka consumer and REST API.

This module contains the event processing function that both transports call.
"""

import json
import logging
import time
from datetime import datetime, timezone

from nrt_mdm.dq import compute_dq_score
from nrt_mdm.mappers import TOPIC_MAPPER, map_crm_a, map_crm_b, map_crm_c
from nrt_mdm.producer import publish_golden_if_changed, publish_xref_change, compute_row_hash, GET_CURRENT_GOLDEN_SQL
from nrt_mdm.resolver import resolve
from nrt_mdm.survivorship import compute_golden
from nrt_mdm.upsert import upsert_source_customer

logger = logging.getLogger(__name__)

# Source system to mapper dispatch (for REST API)
SOURCE_MAPPER = {
    "crm_a": map_crm_a,
    "crm_b": map_crm_b,
    "crm_c": map_crm_c,
}


def process_event(
    source_system: str,
    payload: dict,
    event_ts: datetime,
    pg_conn,
    producer=None,
) -> dict:
    """Process a single event through the full MDM pipeline.

    Args:
        source_system: One of 'crm_a', 'crm_b', 'crm_c'
        payload: The raw event payload (source-specific fields)
        event_ts: Event timestamp
        pg_conn: Postgres connection (autocommit=False)
        producer: Kafka producer (optional — if None, CDC is not published to Kafka)

    Returns:
        dict with CDC result:
        {
            "changed": bool,
            "customer_id": int,
            "event_type": "INSERT" | "UPDATE" | "NO_CHANGE" | "SKIPPED",
            "first_name": ..., "last_name": ..., "email": ..., "phone": ...,
            "dq_score": int, "source_count": int,
            "row_hash": str, "previous_hash": str | None,
            "latency_ms": int,
        }
    """
    start = time.time()

    # Resolve mapper
    mapper = SOURCE_MAPPER.get(source_system.lower())
    if mapper is None:
        return {"changed": False, "event_type": "ERROR", "error": f"Unknown source_system: {source_system}"}

    # Map to common schema
    record = mapper(payload, event_ts)

    # UPSERT into source_customers (with out-of-order protection)
    updated = upsert_source_customer(pg_conn, record)

    if not updated:
        pg_conn.commit()
        return {
            "changed": False,
            "event_type": "SKIPPED",
            "reason": "out-of-order event (older than current state)",
            "latency_ms": int((time.time() - start) * 1000),
        }

    # Resolve: blocking + matching + clustering
    cluster_id, cluster_changed = resolve(pg_conn, record)

    # Recompute golden record for affected cluster
    golden = compute_golden(pg_conn, cluster_id)
    if golden is None:
        pg_conn.commit()
        return {
            "changed": False,
            "event_type": "ERROR",
            "error": "No golden record computed",
            "latency_ms": int((time.time() - start) * 1000),
        }

    # Compute DQ score
    golden.dq_score = compute_dq_score(golden)

    # Compute new hash
    new_hash = compute_row_hash(golden)
    golden.row_hash = new_hash

    # Get previous hash
    with pg_conn.cursor() as cur:
        cur.execute(GET_CURRENT_GOLDEN_SQL, {"cluster_id": golden.cluster_id})
        row = cur.fetchone()
    previous_hash = row[0] if row else None

    changed = previous_hash != new_hash

    if changed:
        # SCD2 write + Kafka CDC publish
        publish_golden_if_changed(producer, pg_conn, golden)
        event_type = "INSERT" if previous_hash is None else "UPDATE"
    else:
        event_type = "NO_CHANGE"

    # Publish XREF change if cluster assignment changed
    if cluster_changed and producer:
        publish_xref_change(producer, record.source_system, record.source_key, cluster_id)

    # Commit DB transaction
    pg_conn.commit()

    latency_ms = int((time.time() - start) * 1000)

    return {
        "changed": changed,
        "customer_id": cluster_id,
        "event_type": event_type,
        "first_name": golden.first_name,
        "last_name": golden.last_name,
        "email": golden.email,
        "phone": golden.phone,
        "dq_score": golden.dq_score,
        "source_count": golden.source_count,
        "row_hash": new_hash,
        "previous_hash": previous_hash,
        "latency_ms": latency_ms,
    }
