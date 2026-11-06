"""Unit tests for the REST API (FastAPI TestClient, no Postgres, no Kafka)."""

from datetime import datetime, timezone
from unittest.mock import patch

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")

from fastapi.testclient import TestClient  # noqa: E402

from fakes import FakeConn  # noqa: E402
from nrt_mdm import api  # noqa: E402

TS = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def client():
    """TestClient without the lifespan hook (which would connect to Postgres/Kafka)."""
    return TestClient(api.app)


@pytest.fixture
def conn():
    fake = FakeConn()
    with patch.object(api, "_pg_conn", fake), patch.object(api, "_producer", "producer"), \
            patch.object(api, "log_audit"):
        yield fake


class TestIngest:
    def test_valid_source_returns_pipeline_result(self, client, conn):
        with patch.object(api, "process_event", return_value={"cluster_id": 7}) as process:
            resp = client.post("/api/v1/ingest/CRM_A", json={"id": "A1"})
        assert resp.status_code == 200
        assert resp.json() == {"cluster_id": 7}
        kwargs = process.call_args.kwargs
        assert kwargs["source_system"] == "crm_a"
        assert kwargs["actor"] == "rest:crm_a"
        assert kwargs["payload"] == {"id": "A1"}

    def test_unknown_source_is_rejected(self, client, conn):
        resp = client.post("/api/v1/ingest/crm_x", json={})
        assert resp.status_code == 400
        assert "Invalid source_system" in resp.json()["detail"]

    def test_pipeline_error_rolls_back(self, client, conn):
        with patch.object(api, "process_event", side_effect=RuntimeError("boom")), \
                patch.object(conn, "rollback") as rollback:
            resp = client.post("/api/v1/ingest/crm_b", json={})
        assert resp.status_code == 500
        assert resp.json()["detail"] == "boom"
        rollback.assert_called_once()


class TestReads:
    def test_get_customer(self, client, conn):
        conn.results = [(7, "Ann", "Lee", "ann@x.com", "+1", 95, 2, "h", TS)]
        body = client.get("/api/v1/customers/7").json()
        assert body["customer_id"] == 7
        assert body["valid_from"] == TS.isoformat()

    def test_get_customer_not_found(self, client, conn):
        assert client.get("/api/v1/customers/7").status_code == 404

    def test_get_sources(self, client, conn):
        conn.results = [[("CRM_A", "A1", "Ann", "Lee", "a@x.com", None, TS), ("CRM_B", "B1", "Ann", "Lee", None, None, None)]]
        body = client.get("/api/v1/customers/7/sources").json()
        assert [s["source_key"] for s in body] == ["A1", "B1"]
        assert body[1]["event_timestamp"] is None

    def test_get_sources_not_found(self, client, conn):
        conn.results = [[]]
        assert client.get("/api/v1/customers/7/sources").status_code == 404

    def test_get_history(self, client, conn):
        conn.results = [[(1, "Ann", "Lee", "a@x.com", None, 90, 1, "h", TS, None, True)]]
        body = client.get("/api/v1/customers/7/history").json()
        assert body[0]["is_current"] is True
        assert body[0]["valid_to"] is None

    def test_get_history_not_found(self, client, conn):
        conn.results = [[]]
        assert client.get("/api/v1/customers/7/history").status_code == 404

    def test_health(self, client):
        assert client.get("/api/v1/health").json() == {"status": "ok"}


class TestReadAudit:
    def test_successful_customer_read_is_audited(self, client, conn):
        conn.results = [(7, "Ann", "Lee", None, None, 80, 1, "h", None)]
        client.get("/api/v1/customers/7")
        kwargs = api.log_audit.call_args.kwargs
        assert (kwargs["event_type"], kwargs["cluster_id"]) == ("READ", 7)

    def test_failed_read_is_not_audited(self, client, conn):
        client.get("/api/v1/customers/7")
        api.log_audit.assert_not_called()

    def test_audit_failure_does_not_break_the_read(self, client, conn):
        conn.results = [(7, "Ann", "Lee", None, None, 80, 1, "h", None)]
        with patch.object(api, "log_audit", side_effect=RuntimeError("audit down")):
            assert client.get("/api/v1/customers/7").status_code == 200


class TestAdminUnmatch:
    BODY = {"cluster_id": 7, "source_records_to_split": [{"source_system": "CRM_B", "source_key": "B1"}],
            "reason": "false positive"}

    def test_success(self, client, conn):
        with patch("nrt_mdm.unmatch.unmatch_records", return_value={"success": True}) as unmatch:
            resp = client.post("/api/v1/admin/unmatch", json=self.BODY)
        assert resp.json() == {"success": True}
        assert unmatch.call_args.kwargs["actor"] == "admin"

    @pytest.mark.parametrize("missing", ["cluster_id", "source_records_to_split", "reason"])
    def test_required_fields(self, client, conn, missing):
        body = {k: v for k, v in self.BODY.items() if k != missing}
        assert client.post("/api/v1/admin/unmatch", json=body).status_code == 400

    @pytest.mark.parametrize("error, status", [(ValueError("not in cluster"), 400), (RuntimeError("db"), 500)])
    def test_errors_roll_back(self, client, conn, error, status):
        with patch("nrt_mdm.unmatch.unmatch_records", side_effect=error), \
                patch.object(conn, "rollback") as rollback:
            resp = client.post("/api/v1/admin/unmatch", json=self.BODY)
        assert resp.status_code == status
        rollback.assert_called_once()
