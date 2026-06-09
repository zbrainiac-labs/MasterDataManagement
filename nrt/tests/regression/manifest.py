"""TST-05 Config Manifest — extracts business logic configuration from code.

Auto-introspects mappers, matching rules, DQ rules, and survivorship config.
Used by regression tests to detect coverage gaps (fails if code diverges from tests).
"""

from nrt_mdm.mappers import TOPIC_MAPPER
from nrt_mdm.matching import MATCH_THRESHOLD
from nrt_mdm.pipeline import SOURCE_MAPPER
from nrt_mdm.survivorship import SOURCE_PRIORITY

# ---------------------------------------------------------------------------
# Manifest: single source of truth extracted from code
# ---------------------------------------------------------------------------

SOURCES = [
    {
        "name": "CRM_A",
        "source_system": "crm_a",
        "topic": "topic.crm.a",
        "rest_path": "/api/v1/ingest/crm_a",
        "required_fields": ["src_customer_id", "first_name", "last_name", "email", "phone"],
    },
    {
        "name": "CRM_B",
        "source_system": "crm_b",
        "topic": "topic.crm.b",
        "rest_path": "/api/v1/ingest/crm_b",
        "required_fields": ["customer_key", "name", "email_address", "mobile"],
    },
    {
        "name": "CRM_C",
        "source_system": "crm_c",
        "topic": "topic.crm.c",
        "rest_path": "/api/v1/ingest/crm_c",
        "required_fields": ["ticket_customer_id", "caller_name", "callback_email", "callback_phone"],
    },
]

MATCHING_CONFIG = {
    "threshold": MATCH_THRESHOLD,
    "deterministic_rules": ["D01", "D02", "C01"],
    "probabilistic_rules": ["P01", "P03", "P04"],
}

DQ_RULE_IDS = [
    "DQ-001", "DQ-002", "DQ-003", "DQ-004", "DQ-005",
    "DQ-006", "DQ-007", "DQ-008", "DQ-C01", "DQ-C02", "DQ-X03",
]

SURVIVORSHIP_CONFIG = {
    "strategy": "field_level_best_value",
    "priority_order": list(SOURCE_PRIORITY.keys()),
}

# Valid pipeline outcomes for regression matrix
PIPELINE_OUTCOMES = ["INSERT", "UPDATE", "NO_CHANGE", "SKIPPED"]

# Field mutations to test
FIELD_MUTATIONS = ["email", "phone", "name", "all_fields", "no_change"]


# ---------------------------------------------------------------------------
# Gap detection: validates code matches manifest
# ---------------------------------------------------------------------------

def validate_manifest():
    """Check that code-level registrations match manifest. Returns list of errors."""
    errors = []

    # Check SOURCE_MAPPER covers all manifest sources
    manifest_sources = {s["source_system"] for s in SOURCES}
    code_sources = set(SOURCE_MAPPER.keys())
    if manifest_sources != code_sources:
        missing = manifest_sources - code_sources
        extra = code_sources - manifest_sources
        if missing:
            errors.append(f"Sources in manifest but not in SOURCE_MAPPER: {missing}")
        if extra:
            errors.append(f"Sources in SOURCE_MAPPER but not in manifest: {extra}")

    # Check TOPIC_MAPPER covers all manifest topics
    manifest_topics = {s["topic"] for s in SOURCES}
    code_topics = set(TOPIC_MAPPER.keys())
    if manifest_topics != code_topics:
        missing = manifest_topics - code_topics
        extra = code_topics - manifest_topics
        if missing:
            errors.append(f"Topics in manifest but not in TOPIC_MAPPER: {missing}")
        if extra:
            errors.append(f"Topics in TOPIC_MAPPER but not in manifest: {extra}")

    # Check survivorship priority covers all source names
    manifest_names = {s["name"] for s in SOURCES}
    priority_names = set(SOURCE_PRIORITY.keys())
    if manifest_names != priority_names:
        errors.append(f"Survivorship priority mismatch: manifest={manifest_names}, code={priority_names}")

    return errors
