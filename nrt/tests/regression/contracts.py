"""TST-05 Contract definitions — JSON Schemas for inbound payloads and CDC output."""

INBOUND_SCHEMAS = {
    "crm_a": {
        "type": "object",
        "required": ["src_customer_id", "first_name", "last_name", "email", "phone"],
        "properties": {
            "src_customer_id": {"type": "string", "minLength": 1},
            "first_name": {"type": "string"},
            "last_name": {"type": "string"},
            "email": {"type": "string"},
            "phone": {"type": "string"},
        },
        "additionalProperties": True,
    },
    "crm_b": {
        "type": "object",
        "required": ["customer_key", "name", "email_address", "mobile"],
        "properties": {
            "customer_key": {"type": "string", "minLength": 1},
            "name": {"type": "string"},
            "email_address": {"type": "string"},
            "mobile": {"type": "string"},
        },
        "additionalProperties": True,
    },
    "crm_c": {
        "type": "object",
        "required": ["ticket_customer_id", "caller_name", "callback_email", "callback_phone"],
        "properties": {
            "ticket_customer_id": {"type": "string", "minLength": 1},
            "caller_name": {"type": "string"},
            "callback_email": {"type": "string"},
            "callback_phone": {"type": "string"},
        },
        "additionalProperties": True,
    },
}

CDC_OUTPUT_SCHEMA = {
    "type": "object",
    "required": ["changed", "event_type"],
    "properties": {
        "changed": {"type": "boolean"},
        "customer_id": {"type": "integer"},
        "event_type": {"type": "string", "enum": ["INSERT", "UPDATE", "NO_CHANGE", "SKIPPED", "ERROR"]},
        "first_name": {"type": ["string", "null"]},
        "last_name": {"type": ["string", "null"]},
        "email": {"type": ["string", "null"]},
        "phone": {"type": ["string", "null"]},
        "dq_score": {"type": "integer", "minimum": 0, "maximum": 100},
        "source_count": {"type": "integer", "minimum": 1},
        "row_hash": {"type": "string"},
        "previous_hash": {"type": ["string", "null"]},
        "latency_ms": {"type": "integer", "minimum": 0},
    },
    "additionalProperties": True,
}

# Sample valid payloads per source (used for contract testing)
SAMPLE_PAYLOADS = {
    "crm_a": {
        "src_customer_id": "TEST-A001",
        "first_name": "John",
        "last_name": "Smith",
        "email": "john.smith@example.com",
        "phone": "+14155551234",
    },
    "crm_b": {
        "customer_key": "TEST-B001",
        "name": "Jane Doe",
        "email_address": "jane.doe@example.com",
        "mobile": "+442071234567",
    },
    "crm_c": {
        "ticket_customer_id": "TEST-C001",
        "caller_name": "Bob Wilson",
        "callback_email": "bob.wilson@example.com",
        "callback_phone": "+61291234567",
    },
}
