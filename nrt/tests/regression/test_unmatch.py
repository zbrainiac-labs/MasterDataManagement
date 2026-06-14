"""TST-05 Regression tests for BIZ-15: Cluster Split / Unmatch."""

import pytest
from datetime import datetime, timezone


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _insert_and_resolve(pipeline, source, key, email, phone, first="Test", last="User"):
    """Insert a record via pipeline and return the result."""
    payloads = {
        "crm_a": {"src_customer_id": key, "first_name": first, "last_name": last, "email": email, "phone": phone},
        "crm_b": {"customer_key": key, "name": f"{first} {last}", "email_address": email, "mobile": phone},
        "crm_c": {"ticket_customer_id": key, "caller_name": f"{first} {last}", "callback_email": email, "callback_phone": phone},
    }
    return pipeline(source, payloads[source])


# ---------------------------------------------------------------------------
# Basic split tests
# ---------------------------------------------------------------------------

class TestUnmatchBasic:
    def test_split_2_source_cluster(self, pg_conn, pipeline):
        """Split a 2-source cluster into 1+1."""
        # Create a merged cluster (same email = match)
        r1 = _insert_and_resolve(pipeline, "crm_a", "SPLIT-A1", "split@test.com", "+11111111111", "Alice", "Split")
        r2 = _insert_and_resolve(pipeline, "crm_b", "SPLIT-B1", "split@test.com", "+12222222222", "Alice", "Split")

        # Both should be in same cluster
        assert r1["customer_id"] == r2["customer_id"]
        cluster_id = r1["customer_id"]
        assert r2["source_count"] == 2

        # Unmatch: split CRM_B out
        from nrt_mdm.unmatch import unmatch_records
        result = unmatch_records(
            pg_conn, None, cluster_id,
            [{"source_system": "CRM_B", "source_key": "SPLIT-B1"}],
            reason="Test split",
        )

        assert result["success"] is True
        assert result["original_cluster_id"] == cluster_id
        assert result["new_cluster_id"] != cluster_id
        assert result["suppressions_created"] == 1
        assert result["original_golden"]["source_count"] == 1
        assert result["new_golden"]["source_count"] == 1

    def test_split_multi_source_cluster(self, pg_conn, pipeline):
        """Split 1 record from a 3-source cluster -> 2+1."""
        r1 = _insert_and_resolve(pipeline, "crm_a", "MULTI-A", "multi@test.com", "+13333333333", "Bob", "Multi")
        r2 = _insert_and_resolve(pipeline, "crm_b", "MULTI-B", "multi@test.com", "+14444444444", "Bob", "Multi")
        r3 = _insert_and_resolve(pipeline, "crm_c", "MULTI-C", "multi@test.com", "+15555555555", "Bob", "Multi")

        cluster_id = r1["customer_id"]
        assert r3["source_count"] == 3

        from nrt_mdm.unmatch import unmatch_records
        result = unmatch_records(
            pg_conn, None, cluster_id,
            [{"source_system": "CRM_C", "source_key": "MULTI-C"}],
            reason="Wrong person",
        )

        assert result["success"] is True
        assert result["original_golden"]["source_count"] == 2
        assert result["new_golden"]["source_count"] == 1
        # 1 split record x 2 remaining = 2 suppressions
        assert result["suppressions_created"] == 2


# ---------------------------------------------------------------------------
# Suppression enforcement
# ---------------------------------------------------------------------------

class TestSuppressionEnforcement:
    def test_suppressed_records_dont_re_merge(self, pg_conn, pipeline):
        """After unmatch, sending matching data does NOT re-merge."""
        # Create merged cluster
        r1 = _insert_and_resolve(pipeline, "crm_a", "SUP-A", "suppress@test.com", "+16666666666", "Carl", "Suppress")
        r2 = _insert_and_resolve(pipeline, "crm_b", "SUP-B", "suppress@test.com", "+17777777777", "Carl", "Suppress")
        cluster_id = r1["customer_id"]
        assert r2["source_count"] == 2

        # Split
        from nrt_mdm.unmatch import unmatch_records
        result = unmatch_records(
            pg_conn, None, cluster_id,
            [{"source_system": "CRM_B", "source_key": "SUP-B"}],
            reason="False positive",
        )
        new_cluster_id = result["new_cluster_id"]

        # Now update CRM_B with same email — should NOT re-merge into original
        r3 = pipeline("crm_b", {
            "customer_key": "SUP-B",
            "name": "Carl Suppress",
            "email_address": "suppress@test.com",
            "mobile": "+17777777777",
        })

        # Key assertion: must NOT be back in original cluster
        assert r3["customer_id"] != cluster_id, (
            f"Record re-merged into original cluster {cluster_id} despite suppression"
        )


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

class TestUnmatchErrors:
    def test_cluster_not_found(self, pg_conn):
        """Unmatch with non-existent cluster raises ValueError."""
        from nrt_mdm.unmatch import unmatch_records
        with pytest.raises(ValueError, match="not found or empty"):
            unmatch_records(pg_conn, None, 999999, [{"source_system": "CRM_A", "source_key": "X"}], "test")

    def test_record_not_in_cluster(self, pg_conn, pipeline):
        """Unmatch with record not in the specified cluster raises ValueError."""
        r1 = _insert_and_resolve(pipeline, "crm_a", "ERR-A", "err@test.com", "+18888888888", "Dan", "Error")
        from nrt_mdm.unmatch import unmatch_records
        with pytest.raises(ValueError, match="not in cluster"):
            unmatch_records(
                pg_conn, None, r1["customer_id"],
                [{"source_system": "CRM_B", "source_key": "NONEXISTENT"}],
                "test",
            )

    def test_cannot_split_all_records(self, pg_conn, pipeline):
        """Cannot split ALL records out of a cluster."""
        r1 = _insert_and_resolve(pipeline, "crm_a", "ALL-A", "all@test.com", "+19999999999", "Eve", "All")
        from nrt_mdm.unmatch import unmatch_records
        with pytest.raises(ValueError, match="Cannot split ALL"):
            unmatch_records(
                pg_conn, None, r1["customer_id"],
                [{"source_system": "CRM_A", "source_key": "ALL-A"}],
                "test",
            )


# ---------------------------------------------------------------------------
# Audit and CDC
# ---------------------------------------------------------------------------

class TestUnmatchAudit:
    def test_audit_record_written(self, pg_conn, pipeline):
        """Unmatch writes an ADMIN/UNMATCH audit record."""
        r1 = _insert_and_resolve(pipeline, "crm_a", "AUD-A", "aud@test.com", "+10000000001", "Fay", "Audit")
        r2 = _insert_and_resolve(pipeline, "crm_b", "AUD-B", "aud@test.com", "+10000000002", "Fay", "Audit")
        cluster_id = r1["customer_id"]

        from nrt_mdm.unmatch import unmatch_records
        unmatch_records(
            pg_conn, None, cluster_id,
            [{"source_system": "CRM_B", "source_key": "AUD-B"}],
            reason="Audit test",
            actor="test-user",
        )

        # Check audit_events
        with pg_conn.cursor() as cur:
            cur.execute("""
                SELECT event_type, action, actor, cluster_id
                FROM audit_events
                WHERE action = 'UNMATCH' AND cluster_id = %s
                ORDER BY created_at DESC LIMIT 1
            """, (cluster_id,))
            row = cur.fetchone()

        assert row is not None
        assert row[0] == "ADMIN"
        assert row[1] == "UNMATCH"
        assert row[2] == "test-user"
        assert row[3] == cluster_id
