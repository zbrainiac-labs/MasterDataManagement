"""Unit tests for cluster resolution (blocking, caching, merge orchestration)."""

from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from fakes import FakeConn
from nrt_mdm import resolver
from nrt_mdm.models import SourceCustomer

TS = datetime(2026, 6, 1, tzinfo=timezone.utc)


def _rec(key, email=None, system="CRM_A", **blocks):
    return SourceCustomer(
        source_system=system, source_key=key, first_name="Ann", last_name="Lee",
        email=email, phone=None, event_timestamp=TS, canonical_first_name="Ann", **blocks,
    )


def _row(key, system="CRM_B"):
    return (system, key, "Ann", "Lee", "Ann", None, None, None, None, None, TS)


@pytest.fixture(autouse=True)
def empty_cache():
    resolver.cluster_cache._cache.clear()
    yield
    resolver.cluster_cache._cache.clear()


class TestFindCandidates:
    def test_no_block_keys_runs_no_queries(self):
        conn = FakeConn()
        assert resolver.find_candidates(conn, _rec("1")) == []
        assert conn.executed == []

    def test_tiers_are_deduplicated_in_order(self):
        conn = FakeConn(results=[[_row("B1"), _row("B2")], [_row("B2"), _row("B3")], [_row("B3"), _row("B4")]])
        rec = _rec("1", block_email_domain="x.com", block_phone_suffix="1234", block_soundex="L000")
        keys = [c.source_key for c in resolver.find_candidates(conn, rec)]
        assert keys == ["B1", "B2", "B3", "B4"]
        assert [sql for sql, _ in conn.executed] == [
            resolver.FIND_BY_EMAIL_DOMAIN_SQL, resolver.FIND_BY_PHONE_SUFFIX_SQL, resolver.FIND_BY_SOUNDEX_SQL,
        ]

    def test_soundex_skipped_when_enough_candidates(self):
        many = [_row(f"B{i}") for i in range(resolver.CANDIDATE_LIMIT)]
        conn = FakeConn(results=[many])
        rec = _rec("1", block_email_domain="x.com", block_soundex="L000")
        assert len(resolver.find_candidates(conn, rec)) == resolver.CANDIDATE_LIMIT
        assert len(conn.executed) == 1

    def test_soundex_tier_hard_cap(self):
        conn = FakeConn(results=[[_row(f"S{i}") for i in range(300)]])
        rec = _rec("1", block_soundex="L000")
        assert len(resolver.find_candidates(conn, rec)) == resolver.CANDIDATE_LIMIT * 2


class TestClusterLookup:
    def test_cache_hit_skips_sql(self):
        resolver.cluster_cache.put("CRM_A", "1", 7)
        conn = FakeConn()
        assert resolver.get_cluster_id(conn, "CRM_A", "1") == 7
        assert conn.executed == []

    def test_sql_fallback_populates_cache(self):
        conn = FakeConn(results=[(9,)])
        assert resolver.get_cluster_id(conn, "CRM_A", "1") == 9
        assert resolver.cluster_cache.get("CRM_A", "1") == 9

    def test_unknown_record(self):
        assert resolver.get_cluster_id(FakeConn(), "CRM_A", "1") is None

    def test_batch_mixes_cache_and_sql(self):
        resolver.cluster_cache.put("CRM_A", "1", 1)
        conn = FakeConn(results=[[("CRM_B", "2", 2)]])
        result = resolver.get_cluster_ids_batch(conn, [("CRM_A", "1"), ("CRM_B", "2"), ("CRM_C", "3")])
        assert result == {("CRM_A", "1"): 1, ("CRM_B", "2"): 2}
        assert conn.executed[0][1] == ["CRM_B", "2", "CRM_C", "3"]

    def test_batch_empty(self):
        assert resolver.get_cluster_ids_batch(FakeConn(), []) == {}

    def test_invalidate_cluster_repoints_entries(self):
        cache = resolver.ClusterCache()
        cache.put("A", "1", 1)
        cache.put("B", "2", 2)
        cache.invalidate_cluster(2, 1)
        assert cache.get("B", "2") == 1
        assert len(cache) == 2


class TestClusterWrites:
    def test_create_new_cluster(self):
        conn = FakeConn(results=[(11,)])
        assert resolver.create_new_cluster(conn, "CRM_A", "1") == 11
        assert resolver.cluster_cache.get("CRM_A", "1") == 11

    def test_merge_absorbs_smaller_cluster(self):
        conn = FakeConn(results=[(1,), (5,)])
        assert resolver.merge_clusters(conn, 10, 20) == 20
        assert conn.executed[2][1] == {"target_id": 20, "source_id": 10}

    def test_is_suppressed(self):
        assert resolver.is_suppressed(FakeConn(results=[(1,)]), _rec("1"), _rec("2")) is True
        assert resolver.is_suppressed(FakeConn(), _rec("1"), _rec("2")) is False


class TestResolve:
    def _patch(self, candidates=(), score=1.0, suppressed=False, cluster_map=None, current=None):
        return patch.multiple(
            resolver,
            find_candidates=lambda *_: list(candidates),
            compute_match_score=lambda *_: score,
            is_suppressed=lambda *_: suppressed,
            get_cluster_ids_batch=lambda *_: dict(cluster_map or {}),
            get_cluster_id=lambda *_: current,
        )

    def test_no_match_creates_cluster(self):
        with self._patch(), patch.object(resolver, "create_new_cluster", return_value=42) as create:
            assert resolver.resolve(FakeConn(), _rec("1")) == (42, True)
        create.assert_called_once()

    def test_suppressed_match_is_ignored(self):
        with self._patch(candidates=[_rec("2")], suppressed=True, cluster_map={("CRM_A", "2"): 5}), \
                patch.object(resolver, "create_new_cluster", return_value=42):
            assert resolver.resolve(FakeConn(), _rec("1")) == (42, True)

    def test_low_score_is_ignored(self):
        with self._patch(candidates=[_rec("2")], score=0.1, cluster_map={("CRM_A", "2"): 5}), \
                patch.object(resolver, "create_new_cluster", return_value=42):
            assert resolver.resolve(FakeConn(), _rec("1"))[0] == 42

    def test_existing_member_unchanged(self):
        with self._patch(current=5), patch.object(resolver, "assign_to_cluster") as assign, \
                patch.object(resolver, "merge_clusters") as merge:
            assert resolver.resolve(FakeConn(), _rec("1")) == (5, False)
        assign.assert_called_once()
        merge.assert_not_called()

    def test_joins_matched_cluster(self):
        with self._patch(candidates=[_rec("2")], cluster_map={("CRM_A", "2"): 5}), \
                patch.object(resolver, "assign_to_cluster"):
            assert resolver.resolve(FakeConn(), _rec("1")) == (5, True)

    def test_bridging_record_merges_clusters(self):
        cmap = {("CRM_A", "2"): 8, ("CRM_A", "3"): 3}
        with self._patch(candidates=[_rec("2"), _rec("3")], cluster_map=cmap, current=3), \
                patch.object(resolver, "assign_to_cluster"), \
                patch.object(resolver, "merge_clusters") as merge:
            assert resolver.resolve(FakeConn(), _rec("1")) == (3, True)
        merge.assert_called_once_with(merge.call_args.args[0], 3, 8)
