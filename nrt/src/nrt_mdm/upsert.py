"""Postgres UPSERT logic with out-of-order protection."""

from nrt_mdm.models import SourceCustomer

UPSERT_SQL = """
INSERT INTO source_customers (
    source_system, source_key, first_name, last_name,
    canonical_first_name, email, phone,
    block_soundex, block_email_domain, block_phone_suffix,
    event_timestamp, ingested_at
) VALUES (
    %(source_system)s, %(source_key)s, %(first_name)s, %(last_name)s,
    %(canonical_first_name)s, %(email)s, %(phone)s,
    %(block_soundex)s, %(block_email_domain)s, %(block_phone_suffix)s,
    %(event_timestamp)s, NOW()
)
ON CONFLICT (source_system, source_key) DO UPDATE SET
    first_name = EXCLUDED.first_name,
    last_name = EXCLUDED.last_name,
    canonical_first_name = EXCLUDED.canonical_first_name,
    email = EXCLUDED.email,
    phone = EXCLUDED.phone,
    block_soundex = EXCLUDED.block_soundex,
    block_email_domain = EXCLUDED.block_email_domain,
    block_phone_suffix = EXCLUDED.block_phone_suffix,
    event_timestamp = EXCLUDED.event_timestamp,
    ingested_at = NOW()
WHERE source_customers.event_timestamp <= EXCLUDED.event_timestamp
"""


def upsert_source_customer(conn, record: SourceCustomer) -> bool:
    """UPSERT a source customer record into Postgres.

    Returns True if the row was inserted/updated, False if skipped (out-of-order).
    The WHERE clause ensures older events never overwrite newer state.
    Uses prepare=True for server-side prepared statement (reused across calls).
    """
    params = {
        "source_system": record.source_system,
        "source_key": record.source_key,
        "first_name": record.first_name,
        "last_name": record.last_name,
        "canonical_first_name": record.canonical_first_name,
        "email": record.email,
        "phone": record.phone,
        "block_soundex": record.block_soundex,
        "block_email_domain": record.block_email_domain,
        "block_phone_suffix": record.block_phone_suffix,
        "event_timestamp": record.event_timestamp,
    }
    with conn.cursor() as cur:
        cur.execute(UPSERT_SQL, params, prepare=True)
        return cur.rowcount > 0
