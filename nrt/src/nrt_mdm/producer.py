"""Kafka outbound producer: CDC for golden records and XREF changes."""

import hashlib
import json
import logging
from datetime import datetime, timezone

from nrt_mdm.models import GoldenCustomer

logger = logging.getLogger(__name__)

GOLDEN_TOPIC = "topic.mdm.golden"
XREF_TOPIC = "topic.mdm.xref"
DLQ_TOPIC = "topic.mdm.dlq"


def compute_row_hash(golden: GoldenCustomer) -> str:
    """Compute SHA256 hash of golden record key fields for CDC detection."""
    payload = "|".join([
        golden.first_name or "",
        golden.last_name or "",
        golden.email or "",
        golden.phone or "",
        str(golden.dq_score),
        str(golden.source_count),
    ])
    return hashlib.sha256(payload.encode()).hexdigest()


def _golden_to_event(golden: GoldenCustomer, event_type: str, previous_hash: str | None, valid_from: str) -> dict:
    """Build the outbound event payload for a golden record change."""
    return {
        "customer_id": golden.cluster_id,
        "event_type": event_type,
        "first_name": golden.first_name,
        "last_name": golden.last_name,
        "email": golden.email,
        "phone": golden.phone,
        "dq_score": golden.dq_score,
        "source_count": golden.source_count,
        "row_hash": golden.row_hash,
        "previous_hash": previous_hash,
        "valid_from": valid_from,
        "published_at": datetime.now(timezone.utc).isoformat(),
    }


def _xref_event(source_system: str, source_key: str, customer_id: int, event_type: str = "ASSIGN", previous_customer_id: int | None = None) -> dict:
    """Build the outbound event payload for an XREF change."""
    return {
        "source_system": source_system,
        "source_key": source_key,
        "customer_id": customer_id,
        "event_type": event_type,
        "previous_customer_id": previous_customer_id,
        "published_at": datetime.now(timezone.utc).isoformat(),
    }


# SQL for golden record CDC
GET_CURRENT_GOLDEN_SQL = """
SELECT row_hash FROM golden_customers
WHERE cluster_id = %(cluster_id)s AND is_current = TRUE
"""

CLOSE_GOLDEN_SQL = """
UPDATE golden_customers SET valid_to = NOW(), is_current = FALSE
WHERE cluster_id = %(cluster_id)s AND is_current = TRUE
"""

INSERT_GOLDEN_SQL = """
INSERT INTO golden_customers (cluster_id, first_name, last_name, email, phone, dq_score, source_count, row_hash, valid_from, valid_to, is_current)
VALUES (%(cluster_id)s, %(first_name)s, %(last_name)s, %(email)s, %(phone)s, %(dq_score)s, %(source_count)s, %(row_hash)s, NOW(), '9999-12-31', TRUE)
"""


def publish_golden_if_changed(producer, conn, golden: GoldenCustomer) -> bool:
    """Compare golden with current, write SCD2, and publish if changed.

    Returns True if a change was detected and published.
    """
    new_hash = compute_row_hash(golden)
    golden.row_hash = new_hash

    # Get current hash
    with conn.cursor() as cur:
        cur.execute(GET_CURRENT_GOLDEN_SQL, {"cluster_id": golden.cluster_id})
        row = cur.fetchone()

    previous_hash = row[0] if row else None

    if previous_hash == new_hash:
        # No change -- no-op
        return False

    # SCD2: close current row and insert new
    with conn.cursor() as cur:
        if previous_hash is not None:
            cur.execute(CLOSE_GOLDEN_SQL, {"cluster_id": golden.cluster_id})
        cur.execute(INSERT_GOLDEN_SQL, {
            "cluster_id": golden.cluster_id,
            "first_name": golden.first_name,
            "last_name": golden.last_name,
            "email": golden.email,
            "phone": golden.phone,
            "dq_score": golden.dq_score,
            "source_count": golden.source_count,
            "row_hash": new_hash,
        })

    # Publish to Kafka
    event_type = "INSERT" if previous_hash is None else "UPDATE"
    valid_from = datetime.now(timezone.utc).isoformat()
    event = _golden_to_event(golden, event_type, previous_hash, valid_from)

    if producer is not None:
        producer.produce(
            GOLDEN_TOPIC,
            key=str(golden.cluster_id),
            value=json.dumps(event).encode(),
        )
        producer.flush()

    logger.info("Published golden %s for cluster_id=%d", event_type, golden.cluster_id)
    return True


def publish_xref_change(producer, source_system: str, source_key: str, customer_id: int, previous_customer_id: int | None = None) -> None:
    """Publish an XREF assignment/reassignment to Kafka."""
    event_type = "REASSIGN" if previous_customer_id else "ASSIGN"
    event = _xref_event(source_system, source_key, customer_id, event_type, previous_customer_id)

    producer.produce(
        XREF_TOPIC,
        key=f"{source_system}|{source_key}",
        value=json.dumps(event).encode(),
    )
    producer.flush()

    logger.info("Published XREF %s: %s|%s -> customer_id=%d", event_type, source_system, source_key, customer_id)
