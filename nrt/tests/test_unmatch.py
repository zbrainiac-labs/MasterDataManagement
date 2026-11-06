"""Unit tests for BIZ-15 cluster split / unmatch (no DB, no Kafka)."""

from unittest.mock import MagicMock, patch

import pytest

from fakes import FakeConn
from nrt_mdm import unmatch
from nrt_mdm.models import GoldenCustomer

MEMBERS = [("CRM_A", "A1"), ("CRM_B", "B1"), ("CRM_C", "C1")]


@pytest.fixture
def deps():
    golden = GoldenCustomer(cluster_id=0, first_name="Ann", last_name="Lee",
                            email="ann@x.com", phone=None, dq_score=0, source_count=1)
    with patch.object(unmatch, "compute_golden", return_value=golden) as compute, \
            patch.object(unmatch, "publish_golden_if_changed") as publish, \
            patch.object(unmatch, "publish_xref_change") as xref, \
            patch.object(unmatch, "publish_audit_async") as audit_pub, \
            patch.object(unmatch, "log_audit", return_value={"id": 1}) as audit:
        yield {"compute": compute, "publish": publish, "xref": xref, "audit_pub": audit_pub, "audit": audit}


def _split(keys, producer=None, members=MEMBERS):
    conn = FakeConn(results=[list(members), (99,)])
    records = [{"source_system": s, "source_key": k} for s, k in keys]
    return conn, unmatch.unmatch_records(conn, producer, 7, records, reason="false positive", actor="tester")


class TestValidation:
    def test_empty_cluster(self, deps):
        with pytest.raises(ValueError, match="not found"):
            _split([("CRM_A", "A1")], members=[])

    def test_record_not_in_cluster(self, deps):
        with pytest.raises(ValueError, match="Records not in cluster"):
            _split([("CRM_X", "X1")])

    def test_cannot_split_everything(self, deps):
        with pytest.raises(ValueError, match="At least one must remain"):
            _split(MEMBERS)


class TestSplit:
    def test_result_and_suppressions(self, deps):
        conn, result = _split([("CRM_B", "B1")])
        assert result["new_cluster_id"] == 99
        assert result["suppressions_created"] == 2
        assert sorted(r["source_key"] for r in result["remaining_records"]) == ["A1", "C1"]
        assert conn.commits == 1
        assert result["original_golden"]["first_name"] == "Ann"

    def test_suppression_pairs_are_ordered(self, deps):
        conn, _ = _split([("CRM_B", "B1")])
        pairs = [p for sql, p in conn.executed if sql == unmatch.INSERT_SUPPRESSION_SQL]
        assert all((p["sys_a"], p["key_a"]) < (p["sys_b"], p["key_b"]) for p in pairs)

    def test_cache_points_to_new_cluster(self, deps):
        _split([("CRM_B", "B1")])
        assert unmatch.cluster_cache.get("CRM_B", "B1") == 99

    def test_xref_events_only_with_producer(self, deps):
        _split([("CRM_B", "B1")])
        deps["xref"].assert_not_called()
        producer = MagicMock()
        _split([("CRM_B", "B1")], producer=producer)
        deps["xref"].assert_called_once_with(producer, "CRM_B", "B1", 99, previous_customer_id=7)

    def test_audit_and_golden_republished(self, deps):
        _split([("CRM_B", "B1")])
        assert deps["publish"].call_count == 2
        assert deps["audit"].call_args.kwargs["action"] == "UNMATCH"
        deps["audit_pub"].assert_called_once()

    def test_missing_golden(self, deps):
        deps["compute"].return_value = None
        _, result = _split([("CRM_B", "B1")])
        assert result["original_golden"] is None
        assert result["new_golden"] is None
        deps["publish"].assert_not_called()
