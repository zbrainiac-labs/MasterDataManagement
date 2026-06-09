"""TST-05 Boundary tests — edge cases, adversarial inputs, and overflow scenarios."""

import pytest
from datetime import datetime, timezone


# ---------------------------------------------------------------------------
# Unicode and special characters
# ---------------------------------------------------------------------------

class TestUnicode:
    def test_accented_names(self, pipeline):
        """Names with accents are handled correctly."""
        result = pipeline("crm_a", {
            "src_customer_id": "UNI-ACCENT",
            "first_name": "José",
            "last_name": "Müller",
            "email": "jose.muller@example.com",
            "phone": "+41791234567",
        })
        assert result["event_type"] == "INSERT"
        assert result["first_name"] == "José"
        assert result["last_name"] == "Müller"

    def test_cjk_characters(self, pipeline):
        """CJK characters in names don't crash the pipeline."""
        result = pipeline("crm_b", {
            "customer_key": "UNI-CJK",
            "name": "田中 太郎",
            "email_address": "tanaka@example.jp",
            "mobile": "+81901234567",
        })
        assert result["event_type"] == "INSERT"

    def test_emoji_in_name(self, pipeline):
        """Emoji in name field doesn't crash (handled gracefully)."""
        result = pipeline("crm_a", {
            "src_customer_id": "UNI-EMOJI",
            "first_name": "Test🎉",
            "last_name": "User👋",
            "email": "emoji@test.com",
            "phone": "+14155551234",
        })
        assert result["event_type"] == "INSERT"


# ---------------------------------------------------------------------------
# Extreme field lengths
# ---------------------------------------------------------------------------

class TestExtremeLengths:
    def test_max_length_name(self, pipeline):
        """200-char name (max VARCHAR(200)) is stored correctly."""
        long_name = "A" * 200
        result = pipeline("crm_a", {
            "src_customer_id": "LEN-MAXNAME",
            "first_name": long_name,
            "last_name": long_name,
            "email": "long@example.com",
            "phone": "+14155551234",
        })
        assert result["event_type"] == "INSERT"

    def test_max_length_email(self, pipeline):
        """255-char email (max VARCHAR(255)) is handled."""
        # local@domain format with long local part
        long_email = "a" * 240 + "@example.com"
        result = pipeline("crm_a", {
            "src_customer_id": "LEN-MAXEMAIL",
            "first_name": "Test",
            "last_name": "Long",
            "email": long_email[:255],
            "phone": "+14155551234",
        })
        assert result["event_type"] == "INSERT"

    def test_15_digit_phone(self, pipeline):
        """15-digit phone (max international) is valid."""
        result = pipeline("crm_a", {
            "src_customer_id": "LEN-MAXPHONE",
            "first_name": "Test",
            "last_name": "Phone",
            "email": "test@example.com",
            "phone": "+123456789012345",
        })
        assert result["event_type"] == "INSERT"

    def test_empty_string_fields(self, pipeline):
        """Empty strings for optional fields don't crash."""
        result = pipeline("crm_a", {
            "src_customer_id": "LEN-EMPTY",
            "first_name": "",
            "last_name": "",
            "email": "",
            "phone": "",
        })
        assert result["event_type"] == "INSERT"
        # DQ score should be heavily penalized
        assert result["dq_score"] < 50


# ---------------------------------------------------------------------------
# SQL injection (must be safely handled by parameterized queries)
# ---------------------------------------------------------------------------

class TestSQLInjection:
    def test_sql_injection_in_name(self, pipeline):
        """SQL injection attempt in name is safely stored as literal string."""
        result = pipeline("crm_a", {
            "src_customer_id": "INJ-NAME",
            "first_name": "'; DROP TABLE source_customers; --",
            "last_name": "Robert'); DROP TABLE golden_customers;--",
            "email": "bobby@tables.com",
            "phone": "+14155551234",
        })
        assert result["event_type"] == "INSERT"
        # Table must still exist (not dropped!)
        result2 = pipeline("crm_a", {
            "src_customer_id": "INJ-VERIFY",
            "first_name": "Still",
            "last_name": "Works",
            "email": "still@works.com",
            "phone": "+14155559999",
        })
        assert result2["event_type"] == "INSERT"

    def test_sql_injection_in_email(self, pipeline):
        """SQL injection in email field handled safely."""
        result = pipeline("crm_b", {
            "customer_key": "INJ-EMAIL",
            "name": "Test User",
            "email_address": "test@x.com' OR '1'='1",
            "mobile": "+14155551234",
        })
        assert result["event_type"] == "INSERT"


# ---------------------------------------------------------------------------
# Null and missing field handling
# ---------------------------------------------------------------------------

class TestNullHandling:
    def test_null_optional_fields(self, pipeline):
        """Null values for non-key fields are accepted."""
        result = pipeline("crm_a", {
            "src_customer_id": "NULL-OPT",
            "first_name": None,
            "last_name": None,
            "email": None,
            "phone": None,
        })
        assert result["event_type"] == "INSERT"
        # DQ should be heavily penalized
        assert result["dq_score"] <= 40

    def test_mixed_null_and_valid(self, pipeline):
        """Mix of null and valid fields is handled."""
        result = pipeline("crm_a", {
            "src_customer_id": "NULL-MIX",
            "first_name": "Valid",
            "last_name": None,
            "email": "valid@email.com",
            "phone": None,
        })
        assert result["event_type"] == "INSERT"
        # Partial penalty but not max
        assert 40 < result["dq_score"] <= 100


# ---------------------------------------------------------------------------
# Phone format variations
# ---------------------------------------------------------------------------

class TestPhoneFormats:
    @pytest.mark.parametrize("phone,expected_valid", [
        ("+41 79 123 45 67", True),
        ("(415) 555-0123", True),
        ("0041791234567", True),
        ("+14155551234", True),
        ("abc-not-a-phone", False),
        ("123", False),
    ])
    def test_phone_variants(self, pipeline, phone, expected_valid):
        """Various phone formats are handled without crashing."""
        result = pipeline("crm_a", {
            "src_customer_id": f"PH-{phone[:8].replace(' ', '')}",
            "first_name": "Phone",
            "last_name": "Test",
            "email": "phone@test.com",
            "phone": phone,
        })
        assert result["event_type"] == "INSERT"
        # Invalid phones get DQ penalty but still process


# ---------------------------------------------------------------------------
# Email format edge cases
# ---------------------------------------------------------------------------

class TestEmailFormats:
    @pytest.mark.parametrize("email", [
        "user+tag@domain.com",
        "user@subdomain.domain.co.uk",
        "simple@example.com",
    ])
    def test_valid_emails(self, pipeline, email):
        """Valid email variants pass through correctly."""
        result = pipeline("crm_a", {
            "src_customer_id": f"EM-{email[:10]}",
            "first_name": "Email",
            "last_name": "Test",
            "email": email,
            "phone": "+14155551234",
        })
        assert result["event_type"] == "INSERT"
        assert result["dq_score"] >= 80

    @pytest.mark.parametrize("email", [
        "@broken",
        "no-at-sign",
        "",
    ])
    def test_invalid_emails(self, pipeline, email):
        """Invalid emails get DQ penalty but don't crash."""
        result = pipeline("crm_a", {
            "src_customer_id": f"EM-BAD-{hash(email) % 1000}",
            "first_name": "Bad",
            "last_name": "Email",
            "email": email,
            "phone": "+14155551234",
        })
        assert result["event_type"] == "INSERT"
        # Should have email-related DQ penalty
        assert result["dq_score"] < 100


# ---------------------------------------------------------------------------
# Idempotency and duplicate handling
# ---------------------------------------------------------------------------

class TestIdempotency:
    def test_duplicate_event_is_no_change(self, pipeline):
        """Exact same event sent twice -> second is NO_CHANGE."""
        payload = {
            "src_customer_id": "IDEMP-001",
            "first_name": "Duplicate",
            "last_name": "Test",
            "email": "dup@test.com",
            "phone": "+14155551234",
        }
        r1 = pipeline("crm_a", payload)
        assert r1["event_type"] == "INSERT"

        r2 = pipeline("crm_a", payload)
        assert r2["event_type"] == "NO_CHANGE"
        assert r2["changed"] is False
        assert r2["row_hash"] == r1["row_hash"]

    def test_triple_send_still_no_change(self, pipeline):
        """Third identical send is still NO_CHANGE."""
        payload = {
            "src_customer_id": "IDEMP-TRIPLE",
            "first_name": "Triple",
            "last_name": "Send",
            "email": "triple@test.com",
            "phone": "+14155551234",
        }
        pipeline("crm_a", payload)
        pipeline("crm_a", payload)
        r3 = pipeline("crm_a", payload)
        assert r3["event_type"] == "NO_CHANGE"
