"""Audit logging for SEC-04 compliance.

Writes audit records transactionally to Postgres (same transaction as mutation)
and publishes asynchronously to Kafka topic.mdm.audit for Snowflake mirroring.
"""

import json
import logging
import uuid
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

AUDIT_TOPIC = "topic.mdm.audit"

INSERT_AUDIT_SQL = """
    INSERT INTO audit_events (event_id, event_type, actor, source_system, source_key, cluster_id, action, detail)
    VALUES (%(event_id)s, %(event_type)s, %(actor)s, %(source_system)s, %(source_key)s, %(cluster_id)s, %(action)s, %(detail)s)
"""


def log_audit(
    pg_conn,
    *,
    event_type: str,
    actor: str = "mdm-engine",
    source_system: str | None = None,
    source_key: str | None = None,
    cluster_id: int | None = None,
    action: str | None = None,
    detail: dict | None = None,
) -> dict:
    """Write an audit record within the current transaction.

    Must be called BEFORE pg_conn.commit() to be transactional.
    Returns the audit record dict (for optional Kafka publish).
    """
    event_id = str(uuid.uuid4())
    record = {
        "event_id": event_id,
        "event_type": event_type,
        "actor": actor,
        "source_system": source_system,
        "source_key": source_key,
        "cluster_id": cluster_id,
        "action": action,
        "detail": json.dumps(detail) if detail else None,
    }

    with pg_conn.cursor() as cur:
        cur.execute(INSERT_AUDIT_SQL, record)

    return {**record, "detail": detail, "created_at": datetime.now(timezone.utc).isoformat()}


def publish_audit_async(producer, audit_record: dict) -> None:
    """Fire-and-forget publish to topic.mdm.audit for Snowflake mirroring.

    Called AFTER commit. Non-blocking — delivery failures are logged but don't fail the pipeline.
    """
    if producer is None:
        return

    try:
        value = json.dumps(audit_record, default=str).encode()
        producer.produce(
            AUDIT_TOPIC,
            key=audit_record.get("event_id", "").encode(),
            value=value,
            on_delivery=_delivery_callback,
        )
        producer.poll(0)
    except Exception:
        logger.warning("Failed to publish audit event to Kafka", exc_info=True)


def _delivery_callback(err, msg):
    """Kafka delivery callback — log failures but don't raise."""
    if err is not None:
        logger.warning(f"Audit event delivery failed: {err}")
