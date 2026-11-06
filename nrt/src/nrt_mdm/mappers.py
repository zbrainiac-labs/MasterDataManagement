"""Field mappers for CRM source systems.

Each mapper transforms a source-specific JSON payload into a SourceCustomer dataclass
with normalized fields and pre-computed blocking keys.
"""

import re
from datetime import datetime

import jellyfish

from nrt_mdm.models import SourceCustomer


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _initcap(s: str | None) -> str | None:
    """Title-case and strip whitespace. Returns None for empty/null."""
    return s.strip().title() if s and s.strip() else None


def _lower_trim(s: str | None) -> str | None:
    """Lowercase and strip. Returns None for empty/null."""
    return s.strip().lower() if s and s.strip() else None


def _digits_only(s: str | None) -> str | None:
    """Keep only digits and '+'. Returns None if empty."""
    if not s:
        return None
    d = re.sub(r"[^0-9+]", "", s)
    return d if d else None


def _soundex(s: str | None) -> str | None:
    """Compute SOUNDEX code. Returns None for empty/null."""
    return jellyfish.soundex(s) if s and s.strip() else None


def _email_domain(email: str | None) -> str | None:
    """Extract domain portion starting from '@'. Returns None if no @."""
    if email and "@" in email:
        return email[email.index("@"):]
    return None


def _phone_suffix(phone: str | None, n: int) -> str | None:
    """Extract last n digits from phone. Returns None if insufficient digits."""
    if phone:
        digits = re.sub(r"\D", "", phone, flags=re.ASCII)
        return digits[-n:] if len(digits) >= n else None
    return None


# ---------------------------------------------------------------------------
# Source-specific mappers
# ---------------------------------------------------------------------------

def map_crm_a(msg: dict, event_ts: datetime) -> SourceCustomer:
    """Map CRM_A payload: separate first_name/last_name fields."""
    first = _initcap(msg.get("first_name"))
    last = _initcap(msg.get("last_name"))
    email = _lower_trim(msg.get("email"))
    phone = _digits_only(msg.get("phone"))
    return SourceCustomer(
        source_system="CRM_A",
        source_key=msg["src_customer_id"],
        first_name=first,
        last_name=last,
        email=email,
        phone=phone,
        event_timestamp=event_ts,
        canonical_first_name=_initcap(first),
        block_soundex=_soundex(last),
        block_email_domain=_email_domain(email),
        block_phone_suffix=_phone_suffix(phone, 4),
    )


def map_crm_b(msg: dict, event_ts: datetime) -> SourceCustomer:
    """Map CRM_B payload: splits 'name' into first/last."""
    parts = (msg.get("name") or "").split(" ", 1)
    first = _initcap(parts[0]) if parts and parts[0] else None
    last = _initcap(parts[1]) if len(parts) > 1 else None
    email = _lower_trim(msg.get("email_address"))
    phone = _digits_only(msg.get("mobile"))
    return SourceCustomer(
        source_system="CRM_B",
        source_key=msg["customer_key"],
        first_name=first,
        last_name=last,
        email=email,
        phone=phone,
        event_timestamp=event_ts,
        canonical_first_name=_initcap(first),
        block_soundex=_soundex(last),
        block_email_domain=_email_domain(email),
        block_phone_suffix=_phone_suffix(phone, 4),
    )


def map_crm_c(msg: dict, event_ts: datetime) -> SourceCustomer:
    """Map CRM_C payload: splits 'caller_name' into first/last."""
    parts = (msg.get("caller_name") or "").split(" ", 1)
    first = _initcap(parts[0]) if parts and parts[0] else None
    last = _initcap(parts[1]) if len(parts) > 1 else None
    email = _lower_trim(msg.get("callback_email"))
    phone = _digits_only(msg.get("callback_phone"))
    return SourceCustomer(
        source_system="CRM_C",
        source_key=msg["ticket_customer_id"],
        first_name=first,
        last_name=last,
        email=email,
        phone=phone,
        event_timestamp=event_ts,
        canonical_first_name=_initcap(first),
        block_soundex=_soundex(last),
        block_email_domain=_email_domain(email),
        block_phone_suffix=_phone_suffix(phone, 4),
    )


# Topic-to-mapper dispatch
TOPIC_MAPPER = {
    "topic.crm.a": map_crm_a,
    "topic.crm.b": map_crm_b,
    "topic.crm.c": map_crm_c,
}
