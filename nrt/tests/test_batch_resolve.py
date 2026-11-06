"""Unit tests for in-memory batch re-resolution (no DB, no Kafka)."""

from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from fakes import FakeConn
from nrt_mdm import batch_resolve
from nrt_mdm.batch_resolve import UnionFind, batch_resolve_fast

TS = datetime(2026, 6, 1, tzinfo=timezone.utc)


def _row(system, key, first, last, email, phone, domain, ts=TS):
    """Row in FETCH_ALL_SOURCES_SQL column order."""
    return (system, key, first, last, first, email, phone, None, domain, None, ts)


ROWS = [
    _row("CRM_A", "A1", "William", "Smith", "w.smith@acme.com", "+11043321819", "acme.com"),
    _row("CRM_B", "B1", "Bill", "Smith", "w.smith@acme.com", "+19999999999", "acme.com"),
    _row("CRM_C", "C1", "Jane", "Doe", "jane@other.org", "+12223334444", "other.org"),
]


def _run(rows, **kwargs):
    conn = FakeConn(results=[rows])
    with patch.object(batch_resolve.psycopg, "connect", return_value=conn), \
            patch.object(batch_resolve, "log_audit") as audit:
        batch_resolve_fast(**kwargs)
    return conn, audit


class TestUnionFind:
    def test_union_joins_and_reports_change(self):
        uf = UnionFind(4)
        assert uf.union(0, 1) is True
        assert uf.union(1, 0) is False
        assert uf.find(0) == uf.find(1)
        assert uf.find(2) != uf.find(0)

    def test_transitive_union(self):
        uf = UnionFind(5)
        uf.union(0, 1)
        uf.union(2, 3)
        uf.union(1, 3)
        assert len({uf.find(i) for i in range(4)}) == 1
        assert uf.find(4) == 4


class TestBatchResolveFast:
    def test_matching_records_share_a_cluster(self):
        conn, _ = _run(ROWS)
        clusters = conn.rows_inserted_into("customer_clusters")
        assert clusters == [("CRM_A", "A1", 1), ("CRM_B", "B1", 1), ("CRM_C", "C1", 2)]
        assert conn.rows_inserted_into("customer_xref") == clusters

    def test_one_golden_record_per_cluster(self):
        conn, audit = _run(ROWS)
        golden = conn.rows_inserted_into("golden_customers")
        assert [g[0] for g in golden] == [1, 2]
        merged = golden[0]
        assert merged[3] == "w.smith@acme.com"
        assert merged[6] == 2  # source_count
        assert audit.call_args.kwargs["detail"]["cluster_count"] == 2
        assert conn.closed

    def test_sequence_advanced_past_last_cluster(self):
        conn, _ = _run(ROWS)
        assert ("SELECT setval('cluster_seq', %s)", (3,)) in conn.executed

    def test_oversized_blocks_are_skipped(self):
        with patch.object(batch_resolve, "MAX_BLOCK_SIZE", 1):
            conn, _ = _run(ROWS)
        assert [c[2] for c in conn.rows_inserted_into("customer_clusters")] == [1, 2, 3]

    def test_empty_source_table(self):
        conn, audit = _run([])
        assert conn.executed_many == []
        audit.assert_not_called()
        assert conn.closed

    @pytest.mark.parametrize("reset", [True, False])
    def test_reset_truncates_first(self, reset):
        conn, _ = _run([], reset=reset)
        truncated = any(sql == batch_resolve.RESET_CLUSTER_SQL for sql, _ in conn.executed)
        assert truncated is reset

    def test_batches_large_inserts(self):
        rows = [_row("CRM_A", f"A{i}", f"N{i}", "X", None, None, None) for i in range(5001)]
        conn, _ = _run(rows)
        cluster_calls = [r for sql, r in conn.executed_many if "INSERT INTO customer_clusters" in sql]
        assert [len(r) for r in cluster_calls] == [5000, 1]
        golden_calls = [r for sql, r in conn.executed_many if "INSERT INTO golden_customers" in sql]
        assert sum(len(r) for r in golden_calls) == 5001
