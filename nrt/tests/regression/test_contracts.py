"""TST-05 Contract tests — validate inbound/outbound schemas per source system."""

import pytest
from jsonschema import validate, ValidationError

from .contracts import CDC_OUTPUT_SCHEMA, INBOUND_SCHEMAS, SAMPLE_PAYLOADS
from .manifest import SOURCES, validate_manifest


# ---------------------------------------------------------------------------
# Manifest integrity
# ---------------------------------------------------------------------------

class TestManifestCoverage:
    def test_manifest_matches_code(self):
        """Fail if code registrations diverge from manifest."""
        errors = validate_manifest()
        assert not errors, f"Manifest-code mismatch: {errors}"

    def test_all_sources_have_schemas(self):
        """Every source in manifest has an inbound schema defined."""
        for src in SOURCES:
            assert src["source_system"] in INBOUND_SCHEMAS, (
                f"Missing inbound schema for {src['source_system']}"
            )

    def test_all_sources_have_sample_payloads(self):
        """Every source has a sample payload for testing."""
        for src in SOURCES:
            assert src["source_system"] in SAMPLE_PAYLOADS, (
                f"Missing sample payload for {src['source_system']}"
            )


# ---------------------------------------------------------------------------
# Inbound contract validation
# ---------------------------------------------------------------------------

class TestInboundContracts:
    @pytest.mark.parametrize("source", ["crm_a", "crm_b", "crm_c"])
    def test_valid_payload_passes_schema(self, source):
        """A valid sample payload passes its inbound schema."""
        schema = INBOUND_SCHEMAS[source]
        payload = SAMPLE_PAYLOADS[source]
        validate(instance=payload, schema=schema)

    @pytest.mark.parametrize("source", ["crm_a", "crm_b", "crm_c"])
    def test_missing_required_field_fails(self, source):
        """Removing a required field fails schema validation."""
        schema = INBOUND_SCHEMAS[source]
        payload = SAMPLE_PAYLOADS[source].copy()
        # Remove first required field
        first_required = schema["required"][0]
        del payload[first_required]
        with pytest.raises(ValidationError):
            validate(instance=payload, schema=schema)

    @pytest.mark.parametrize("source", ["crm_a", "crm_b", "crm_c"])
    def test_extra_fields_accepted(self, source):
        """Extra fields are silently accepted (additionalProperties: true)."""
        schema = INBOUND_SCHEMAS[source]
        payload = {**SAMPLE_PAYLOADS[source], "extra_field": "ignored"}
        validate(instance=payload, schema=schema)

    @pytest.mark.parametrize("source", ["crm_a", "crm_b", "crm_c"])
    def test_empty_key_field_fails(self, source):
        """Empty string for key field fails (minLength: 1)."""
        schema = INBOUND_SCHEMAS[source]
        payload = SAMPLE_PAYLOADS[source].copy()
        key_field = schema["required"][0]
        payload[key_field] = ""
        with pytest.raises(ValidationError):
            validate(instance=payload, schema=schema)


# ---------------------------------------------------------------------------
# Outbound CDC contract validation
# ---------------------------------------------------------------------------

class TestCDCOutputContract:
    def test_valid_insert_output(self):
        """A valid INSERT CDC output passes schema."""
        output = {
            "changed": True,
            "customer_id": 1,
            "event_type": "INSERT",
            "first_name": "John",
            "last_name": "Smith",
            "email": "john@example.com",
            "phone": "+14155551234",
            "dq_score": 95,
            "source_count": 1,
            "row_hash": "abc123def456",
            "previous_hash": None,
            "latency_ms": 42,
        }
        validate(instance=output, schema=CDC_OUTPUT_SCHEMA)

    def test_valid_no_change_output(self):
        """A NO_CHANGE output is valid."""
        output = {
            "changed": False,
            "customer_id": 1,
            "event_type": "NO_CHANGE",
            "first_name": "John",
            "last_name": "Smith",
            "email": "john@example.com",
            "phone": "+14155551234",
            "dq_score": 95,
            "source_count": 2,
            "row_hash": "abc123",
            "previous_hash": "abc123",
            "latency_ms": 5,
        }
        validate(instance=output, schema=CDC_OUTPUT_SCHEMA)

    def test_invalid_event_type_fails(self):
        """Invalid event_type rejected by schema."""
        output = {"changed": True, "event_type": "INVALID_TYPE"}
        with pytest.raises(ValidationError):
            validate(instance=output, schema=CDC_OUTPUT_SCHEMA)

    def test_dq_score_out_of_range_fails(self):
        """DQ score > 100 rejected."""
        output = {
            "changed": True, "event_type": "INSERT",
            "customer_id": 1, "dq_score": 150, "source_count": 1,
            "row_hash": "x", "previous_hash": None, "latency_ms": 1,
        }
        with pytest.raises(ValidationError):
            validate(instance=output, schema=CDC_OUTPUT_SCHEMA)


# ---------------------------------------------------------------------------
# Pipeline integration (valid payload -> valid CDC output)
# ---------------------------------------------------------------------------

class TestPipelineContract:
    @pytest.mark.parametrize("source", ["crm_a", "crm_b", "crm_c"])
    def test_valid_payload_produces_valid_cdc(self, pipeline, source):
        """Valid inbound payload produces a CDC output conforming to schema."""
        payload = SAMPLE_PAYLOADS[source]
        result = pipeline(source, payload)
        validate(instance=result, schema=CDC_OUTPUT_SCHEMA)

    @pytest.mark.parametrize("source", ["crm_a", "crm_b", "crm_c"])
    def test_first_event_is_insert(self, pipeline, source):
        """First event for a new source key produces INSERT."""
        payload = SAMPLE_PAYLOADS[source]
        result = pipeline(source, payload)
        assert result["event_type"] == "INSERT"
        assert result["changed"] is True
        assert result["source_count"] == 1
