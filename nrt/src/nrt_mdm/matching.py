"""Rule-based matching engine for entity resolution.

Implements deterministic + probabilistic scoring with a configurable threshold.
No ML model -- uses jellyfish for Jaro-Winkler similarity and SOUNDEX.
"""

import jellyfish

from nrt_mdm.models import SourceCustomer

# Threshold for merge decision
MATCH_THRESHOLD = 0.70


def _has_full_name(r: SourceCustomer) -> bool:
    return bool(r.canonical_first_name and r.last_name)


def _email_exact(a: SourceCustomer, b: SourceCustomer) -> float:
    # MATCH-D01: Email exact equality
    return 1.0 if (a.email and b.email and a.email == b.email) else 0.0


def _phone_last10(a: SourceCustomer, b: SourceCustomer) -> float:
    # MATCH-D02: Phone last-10 digits equality
    if not (a.phone and b.phone):
        return 0.0
    a_digits = a.phone.replace("+", "")
    b_digits = b.phone.replace("+", "")
    if len(a_digits) >= 10 and len(b_digits) >= 10 and a_digits[-10:] == b_digits[-10:]:
        return 0.95
    return 0.0


def _canonical_name_exact(a: SourceCustomer, b: SourceCustomer) -> float:
    # MATCH-C01: Canonical name exact (case-insensitive)
    if (_has_full_name(a) and _has_full_name(b)
            and a.canonical_first_name.lower() == b.canonical_first_name.lower()
            and a.last_name.lower() == b.last_name.lower()):
        return 0.80
    return 0.0


def _full_name_similarity(a: SourceCustomer, b: SourceCustomer) -> float:
    # MATCH-P01: Full name Jaro-Winkler similarity
    if not (_has_full_name(a) and _has_full_name(b)):
        return 0.0
    jw = jellyfish.jaro_winkler_similarity(
        f"{a.canonical_first_name} {a.last_name}", f"{b.canonical_first_name} {b.last_name}"
    )
    return jw * 0.30 if jw >= 0.85 else 0.0


def _soundex_equal(a: SourceCustomer, b: SourceCustomer) -> float:
    # MATCH-P03: Last name SOUNDEX equality
    return 0.20 if (a.block_soundex and b.block_soundex and a.block_soundex == b.block_soundex) else 0.0


def _email_domain_and_first_name(a: SourceCustomer, b: SourceCustomer) -> float:
    # MATCH-P04: Email domain match + first name JW >= 0.90
    if not (a.email and b.email and "@" in a.email and "@" in b.email):
        return 0.0
    if a.email[a.email.index("@"):] != b.email[b.email.index("@"):]:
        return 0.0
    if not (a.canonical_first_name and b.canonical_first_name):
        return 0.0
    fn_jw = jellyfish.jaro_winkler_similarity(a.canonical_first_name.lower(), b.canonical_first_name.lower())
    return 0.15 if fn_jw >= 0.90 else 0.0


DETERMINISTIC_RULES = (_email_exact, _phone_last10, _canonical_name_exact)
PROBABILISTIC_RULES = (_full_name_similarity, _soundex_equal, _email_domain_and_first_name)


def compute_match_score(a: SourceCustomer, b: SourceCustomer) -> float:
    """Compute combined match score between two source records.

    Formula: MAX(deterministic) + SUM(probabilistic)

    Returns a float score. If >= MATCH_THRESHOLD, records should be merged.
    """
    deterministic_max = max(rule(a, b) for rule in DETERMINISTIC_RULES)
    probabilistic_sum = sum(rule(a, b) for rule in PROBABILISTIC_RULES)
    return deterministic_max + probabilistic_sum


def is_match(a: SourceCustomer, b: SourceCustomer) -> bool:
    """Determine if two records should be merged."""
    return compute_match_score(a, b) >= MATCH_THRESHOLD
