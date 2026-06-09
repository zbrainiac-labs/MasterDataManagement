"""TST-05 Regression matrix — parametrized full-path tests.

Tests every combination of: source system x field mutation x expected outcome.
Calls process_event() directly against real Postgres (no Kafka overhead).
"""

import pytest
from datetime import datetime, timezone, timedelta

from .contracts import SAMPLE_PAYLOADS
from .manifest import SOURCES


# ---------------------------------------------------------------------------
# Test payload builders (one per source)
# ---------------------------------------------------------------------------

def _build_payload(source: str, mutation: str, key_suffix: str = "001") -> dict:
    """Build a payload for a given source with a specific field mutation applied."""
    base = SAMPLE_PAYLOADS[source].copy()

    # Override key to make unique per test
    if source == "crm_a":
        base["src_customer_id"] = f"REG-A-{key_suffix}"
    elif source == "crm_b":
        base["customer_key"] = f"REG-B-{key_suffix}"
    elif source == "crm_c":
        base["ticket_customer_id"] = f"REG-C-{key_suffix}"

    if mutation == "email":
        _mutate_email(base, source)
    elif mutation == "phone":
        _mutate_phone(base, source)
    elif mutation == "name":
        _mutate_name(base, source)
    elif mutation == "all_fields":
        _mutate_email(base, source)
        _mutate_phone(base, source)
        _mutate_name(base, source)
    # "no_change" — return base as-is

    return base


def _mutate_email(payload: dict, source: str):
    if source == "crm_a":
        payload["email"] = "updated@newdomain.com"
    elif source == "crm_b":
        payload["email_address"] = "updated@newdomain.com"
    elif source == "crm_c":
        payload["callback_email"] = "updated@newdomain.com"


def _mutate_phone(payload: dict, source: str):
    if source == "crm_a":
        payload["phone"] = "+19999999999"
    elif source == "crm_b":
        payload["mobile"] = "+19999999999"
    elif source == "crm_c":
        payload["callback_phone"] = "+19999999999"


def _mutate_name(payload: dict, source: str):
    if source == "crm_a":
        payload["first_name"] = "Updated"
        payload["last_name"] = "Person"
    elif source == "crm_b":
        payload["name"] = "Updated Person"
    elif source == "crm_c":
        payload["caller_name"] = "Updated Person"


# ---------------------------------------------------------------------------
# NEW_CLUSTER tests: first event creates a new golden record
# ---------------------------------------------------------------------------

class TestNewCluster:
    @pytest.mark.parametrize("source", ["crm_a", "crm_b", "crm_c"])
    def test_new_record_creates_cluster(self, pipeline, source):
        """A brand-new source key creates a new cluster (INSERT)."""
        payload = _build_payload(source, "no_change", key_suffix="NEW001")
        result = pipeline(source, payload)

        assert result["event_type"] == "INSERT"
        assert result["changed"] is True
        assert result["customer_id"] is not None
        assert result["source_count"] == 1
        assert 0 <= result["dq_score"] <= 100

    @pytest.mark.parametrize("source", ["crm_a", "crm_b", "crm_c"])
    @pytest.mark.parametrize("mutation", ["email", "phone", "name", "all_fields"])
    def test_new_record_with_mutations(self, pipeline, source, mutation):
        """New records with various field values all produce INSERT."""
        payload = _build_payload(source, mutation, key_suffix=f"NEW-{mutation}")
        result = pipeline(source, payload)
        assert result["event_type"] == "INSERT"
        assert result["changed"] is True


# ---------------------------------------------------------------------------
# UPDATE tests: same source key, changed data -> golden updates
# ---------------------------------------------------------------------------

class TestUpdate:
    @pytest.mark.parametrize("source", ["crm_a", "crm_b", "crm_c"])
    @pytest.mark.parametrize("mutation", ["email", "phone", "name", "all_fields"])
    def test_update_changes_golden(self, pipeline, source, mutation):
        """Sending updated data for existing key produces UPDATE."""
        key = f"UPD-{mutation}"
        # Insert original
        original = _build_payload(source, "no_change", key_suffix=key)
        r1 = pipeline(source, original)
        assert r1["event_type"] == "INSERT"

        # Send update with mutation
        updated = _build_payload(source, mutation, key_suffix=key)
        r2 = pipeline(source, updated)

        assert r2["event_type"] == "UPDATE"
        assert r2["changed"] is True
        assert r2["customer_id"] == r1["customer_id"]
        assert r2["row_hash"] != r1["row_hash"]


# ---------------------------------------------------------------------------
# NO_CHANGE tests: same data re-sent -> no golden change
# ---------------------------------------------------------------------------

class TestNoChange:
    @pytest.mark.parametrize("source", ["crm_a", "crm_b", "crm_c"])
    def test_same_data_produces_no_change(self, pipeline, source):
        """Re-sending identical data produces NO_CHANGE."""
        key = "NOCHANGE001"
        payload = _build_payload(source, "no_change", key_suffix=key)

        r1 = pipeline(source, payload)
        assert r1["event_type"] == "INSERT"

        r2 = pipeline(source, payload)
        assert r2["event_type"] == "NO_CHANGE"
        assert r2["changed"] is False


# ---------------------------------------------------------------------------
# SKIPPED tests: out-of-order events
# ---------------------------------------------------------------------------

class TestSkipped:
    @pytest.mark.parametrize("source", ["crm_a", "crm_b", "crm_c"])
    def test_older_event_is_skipped(self, pipeline, source):
        """An event with older timestamp than current state is SKIPPED."""
        from nrt_mdm.pipeline import process_event

        key = "SKIP001"
        payload = _build_payload(source, "no_change", key_suffix=key)

        now = datetime.now(timezone.utc)
        older = now - timedelta(hours=1)

        # Send recent event first
        r1 = pipeline(source, payload, event_ts=now)
        assert r1["event_type"] == "INSERT"

        # Send older event -> should be skipped
        r2 = pipeline(source, payload, event_ts=older)
        assert r2["event_type"] == "SKIPPED"
        assert r2["changed"] is False


# ---------------------------------------------------------------------------
# MERGE tests: different source key matches existing cluster
# ---------------------------------------------------------------------------

class TestMerge:
    def test_merge_via_email_match(self, pipeline):
        """Two records from different sources with same email merge into one cluster."""
        # CRM_A record
        r1 = pipeline("crm_a", {
            "src_customer_id": "MERGE-A001",
            "first_name": "Alice",
            "last_name": "Johnson",
            "email": "alice.merge@example.com",
            "phone": "+14155550001",
        })
        assert r1["event_type"] == "INSERT"
        assert r1["source_count"] == 1

        # CRM_B record with same email -> should merge
        r2 = pipeline("crm_b", {
            "customer_key": "MERGE-B001",
            "name": "Alice Johnson",
            "email_address": "alice.merge@example.com",
            "mobile": "+14155550002",
        })

        # Should join existing cluster (source_count increases)
        assert r2["customer_id"] == r1["customer_id"]
        assert r2["source_count"] == 2

    def test_merge_via_phone_match(self, pipeline):
        """Two records with same phone (last 10 digits) merge."""
        r1 = pipeline("crm_a", {
            "src_customer_id": "MERGE-PH-A",
            "first_name": "Bob",
            "last_name": "Brown",
            "email": "bob1@unique.com",
            "phone": "+14155559876",
        })
        assert r1["event_type"] == "INSERT"

        r2 = pipeline("crm_c", {
            "ticket_customer_id": "MERGE-PH-C",
            "caller_name": "Bob Brown",
            "callback_email": "bob2@different.com",
            "callback_phone": "+14155559876",
        })

        assert r2["customer_id"] == r1["customer_id"]
        assert r2["source_count"] == 2

    def test_no_merge_different_data(self, pipeline):
        """Records with completely different data don't merge."""
        r1 = pipeline("crm_a", {
            "src_customer_id": "NOMERGE-A",
            "first_name": "Charlie",
            "last_name": "Alpha",
            "email": "charlie@alpha.com",
            "phone": "+11111111111",
        })

        r2 = pipeline("crm_b", {
            "customer_key": "NOMERGE-B",
            "name": "David Beta",
            "email_address": "david@beta.com",
            "mobile": "+12222222222",
        })

        assert r2["customer_id"] != r1["customer_id"]
        assert r2["source_count"] == 1
