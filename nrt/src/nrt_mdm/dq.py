"""Data Quality scoring engine.

Applies 11 non-AI rules to a golden record. Base score 100, clamped 0-100.
Same rules as the batch Snowflake pipeline (DT_CUSTOMER_GOLDEN_FUZZY) minus DQ-AI01.
"""

import re

from nrt_mdm.models import GoldenCustomer

DISPOSABLE_DOMAINS = {"mailinator.com", "tempmail.com", "guerrillamail.com", "10minutemail.com"}
PLACEHOLDER_PHONES = {"0000000000", "1111111111", "1234567890"}

EMAIL_REGEX = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")
PHONE_REGEX = re.compile(r"^\+?\d{10,15}$", re.ASCII)
NAME_REGEX = re.compile(r"^[A-Za-z '\-]+$")


def _has_name(value: str | None) -> bool:
    return value is not None and len(value.strip()) > 1


def _digits(value: str) -> str:
    return re.sub(r"\D", "", value, flags=re.ASCII)


def _email_penalty(email: str | None) -> int:
    # DQ-001: Invalid email format (-20)
    penalty = 0 if email and EMAIL_REGEX.match(email) else 20
    # DQ-002: Disposable email domain (-5)
    if email and "@" in email and email.split("@")[1].lower() in DISPOSABLE_DOMAINS:
        penalty += 5
    return penalty


def _name_penalty(name: str | None) -> int:
    # DQ-003 / DQ-005: Missing/short name (-20)
    if not _has_name(name):
        return 20
    # DQ-004 / DQ-006: Special chars in name (-5)
    return 0 if NAME_REGEX.match(name) else 5


def _phone_penalty(phone: str | None) -> int:
    if not phone:
        return 0
    penalty = 0
    # DQ-007: Invalid phone format (-5)
    if not PHONE_REGEX.match(re.sub(r"[^0-9+]", "", phone)):
        penalty += 5
    # DQ-008: Placeholder phone (-20)
    if _digits(phone) in PLACEHOLDER_PHONES:
        penalty += 20
    return penalty


def _completeness_penalty(golden: GoldenCustomer) -> int:
    penalty = 0
    # DQ-C01: No contact method (-20)
    has_valid_email = golden.email is not None and EMAIL_REGEX.match(golden.email)
    has_valid_phone = golden.phone is not None and len(_digits(golden.phone)) >= 7
    if not has_valid_email and not has_valid_phone:
        penalty += 20
    # DQ-C02: No complete name (-20)
    if not _has_name(golden.first_name) and not _has_name(golden.last_name):
        penalty += 20
    return penalty


def _name_in_email_bonus(golden: GoldenCustomer) -> int:
    # DQ-X03: Name appears in email (+5 bonus)
    if _has_name(golden.first_name) and golden.email \
            and golden.first_name.strip().lower() in golden.email.lower():
        return 5
    return 0


def compute_dq_score(golden: GoldenCustomer) -> int:
    """Compute DQ score for a golden record. Returns 0-100."""
    score = (
        100
        - _email_penalty(golden.email)
        - _name_penalty(golden.first_name)
        - _name_penalty(golden.last_name)
        - _phone_penalty(golden.phone)
        - _completeness_penalty(golden)
        + _name_in_email_bonus(golden)
    )
    return max(0, min(100, score))
