"""Characterization tables for DQ and match scoring.

The values were recorded from the implementation before the complexity
refactor; any change here is a behaviour change, not a refactor.
"""

from datetime import datetime, timezone

import pytest

from nrt_mdm.dq import compute_dq_score
from nrt_mdm.matching import compute_match_score
from nrt_mdm.models import GoldenCustomer, SourceCustomer

TS = datetime(2026, 6, 1, tzinfo=timezone.utc)


@pytest.mark.parametrize("first, last, email, phone, expected", [
    ("Ann", "Lee", "ann.lee@acme.com", "+14155550100", 100),
    (None, None, None, None, 0),
    ("A", "L", "bad", "123", 0),
    ("Ann", "Lee", "x@mailinator.com", "0000000000", 75),
    ("Ann3", "O%Brien", "ann@acme.com", "(415) 555-0100", 90),
    ("  ", "Lee", "lee@acme.com", None, 80),
    ("Ann", None, None, "5550100", 55),
    ("Ann", "Lee", "noat", "1234567890", 60),
    ("Zed", "Lee", "x@y", "+1-415-555-0100", 80),
    ("Ann", "Lee", None, "abc", 55),
])
def test_dq_score_table(first, last, email, phone, expected):
    golden = GoldenCustomer(1, first, last, email, phone, 0, 1)
    assert compute_dq_score(golden) == expected


def _src(first, last, email, phone, soundex=None):
    return SourceCustomer("CRM_A", "1", first, last, email, phone, TS,
                          canonical_first_name=first, block_soundex=soundex)


@pytest.mark.parametrize("a, b, expected", [
    (_src("Ann", "Lee", "a@x.com", None), _src("Bob", "Kim", "a@x.com", None), 1.0),
    (_src(None, None, None, "+14155550100"), _src(None, None, None, "4155550100"), 0.95),
    (_src("Ann", "Lee", None, None, "L000"), _src("ann", "LEE", None, None, "L000"), 1.0),
    (_src("Jonathan", "Smith", None, None), _src("Jonathon", "Smith", None, None), 0.291429),
    (_src("Ann", "Lee", "ann@x.com", None), _src("Anne", "Park", "anne@x.com", None), 0.15),
    (_src("Ann", "Lee", None, "123"), _src("Bob", "Kim", None, "123"), 0.0),
    (_src(None, None, "bad", None), _src(None, None, "bad2", None), 0.0),
    (_src("Ann", "Lee", "a@x.com", "+14155550100", "L000"),
     _src("Ann", "Lee", "a@x.com", "+14155550100", "L000"), 1.65),
], ids=["email", "phone", "canonical", "jaro-winkler", "domain-first-name", "short-phone", "no-at", "all"])
def test_match_score_table(a, b, expected):
    assert compute_match_score(a, b) == pytest.approx(expected, abs=1e-6)
