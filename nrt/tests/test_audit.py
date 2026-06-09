"""Unit tests for audit.py (SEC-04)."""

import json
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_pg_conn():
    """Mock Postgres connection with cursor context manager."""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return conn, cursor


@pytest.fixture
def mock_producer():
    """Mock Kafka producer."""
    producer = MagicMock()
    producer.produce = MagicMock()
    producer.poll = MagicMock()
    return producer


# ---------------------------------------------------------------------------
# Tests: log_audit
# ---------------------------------------------------------------------------

class TestLogAudit:
    def test_writes_to_audit_events(self, mock_pg_conn):
        from nrt_mdm.audit import log_audit

        conn, cursor = mock_pg_conn

        result = log_audit(
            conn,
            event_type="INGEST",
            source_system="CRM_A",
            source_key="A001",
            cluster_id=42,
            action="INSERT",
            detail={"previous_hash": None, "new_hash": "abc123"},
        )

        # Verify INSERT was executed
        cursor.execute.assert_called_once()
        call_args = cursor.execute.call_args
        sql = call_args[0][0]
        params = call_args[0][1]

        assert "INSERT INTO audit_events" in sql
        assert params["event_type"] == "INGEST"
        assert params["source_system"] == "CRM_A"
        assert params["source_key"] == "A001"
        assert params["cluster_id"] == 42
        assert params["action"] == "INSERT"

        # Verify detail is JSON-serialized
        detail = json.loads(params["detail"])
        assert detail["new_hash"] == "abc123"

    def test_returns_record_dict(self, mock_pg_conn):
        from nrt_mdm.audit import log_audit

        conn, cursor = mock_pg_conn

        result = log_audit(
            conn,
            event_type="READ",
            actor="test-user",
            cluster_id=99,
            action="READ",
        )

        assert result["event_type"] == "READ"
        assert result["actor"] == "test-user"
        assert result["cluster_id"] == 99
        assert result["action"] == "READ"
        assert "event_id" in result
        assert "created_at" in result

    def test_default_actor(self, mock_pg_conn):
        from nrt_mdm.audit import log_audit

        conn, cursor = mock_pg_conn

        result = log_audit(conn, event_type="INGEST", action="UPDATE")
        assert result["actor"] == "mdm-engine"

    def test_none_detail_stored_as_null(self, mock_pg_conn):
        from nrt_mdm.audit import log_audit

        conn, cursor = mock_pg_conn

        log_audit(conn, event_type="ADMIN", action="TRUNCATE")

        params = cursor.execute.call_args[0][1]
        assert params["detail"] is None


# ---------------------------------------------------------------------------
# Tests: publish_audit_async
# ---------------------------------------------------------------------------

class TestPublishAuditAsync:
    def test_produces_to_audit_topic(self, mock_producer):
        from nrt_mdm.audit import publish_audit_async, AUDIT_TOPIC

        record = {
            "event_id": "test-uuid-123",
            "event_type": "INGEST",
            "actor": "mdm-engine",
            "source_system": "CRM_A",
            "action": "INSERT",
        }

        publish_audit_async(mock_producer, record)

        mock_producer.produce.assert_called_once()
        call_kwargs = mock_producer.produce.call_args
        assert call_kwargs[1]["topic"] if "topic" in (call_kwargs[1] or {}) else call_kwargs[0][0] == AUDIT_TOPIC

    def test_none_producer_is_noop(self):
        from nrt_mdm.audit import publish_audit_async

        # Should not raise
        publish_audit_async(None, {"event_id": "x", "event_type": "INGEST"})

    def test_produce_failure_does_not_raise(self, mock_producer):
        from nrt_mdm.audit import publish_audit_async

        mock_producer.produce.side_effect = Exception("Kafka down")

        # Should not raise — failures are logged, not propagated
        publish_audit_async(mock_producer, {"event_id": "x", "event_type": "INGEST"})
