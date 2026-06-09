"""TST-05 Shared fixtures for regression tests.

Provides a real Postgres connection (uses the Docker container) with schema
setup/teardown per test, and a pipeline helper for calling process_event().
"""

import os
from datetime import datetime, timezone

import psycopg
import pytest

POSTGRES_DSN = os.environ.get("POSTGRES_DSN", "postgresql://mdm:mdm@localhost:5432/mdm")

# Schema DDL (loaded once per session)
_SCHEMA_FILES = [
    "001_source_tables.sql",
    "002_golden_tables.sql",
    "003_audit_tables.sql",
]

SCHEMA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "schema")


def _read_schema():
    """Read all schema DDL files."""
    ddl = []
    for f in _SCHEMA_FILES:
        path = os.path.join(SCHEMA_DIR, f)
        if os.path.exists(path):
            with open(path) as fh:
                ddl.append(fh.read())
    return "\n".join(ddl)


@pytest.fixture
def pg_conn():
    """Real Postgres connection with clean tables per test.

    Uses TRUNCATE + RESTART IDENTITY between tests for isolation.
    Requires Docker postgres container running (port 5432).
    """
    conn = psycopg.connect(POSTGRES_DSN, autocommit=False)

    # Ensure schema exists (idempotent)
    with conn.cursor() as cur:
        cur.execute(_read_schema())
    conn.commit()

    # Truncate all data for clean state
    with conn.cursor() as cur:
        cur.execute("""
            TRUNCATE source_customers, customer_clusters, golden_customers,
                     customer_xref, audit_events RESTART IDENTITY CASCADE
        """)
    conn.commit()

    yield conn

    conn.rollback()
    conn.close()


@pytest.fixture
def pipeline(pg_conn):
    """Helper to call process_event() with standard defaults.

    Returns a callable: pipeline(source_system, payload, event_ts=None) -> dict
    """
    from nrt_mdm.pipeline import process_event

    def _call(source_system: str, payload: dict, event_ts=None):
        ts = event_ts or datetime.now(timezone.utc)
        return process_event(
            source_system=source_system,
            payload=payload,
            event_ts=ts,
            pg_conn=pg_conn,
            producer=None,  # No Kafka in unit tests
        )

    return _call


@pytest.fixture
def insert_source(pg_conn):
    """Helper to insert a source_customer record directly (for precondition setup)."""
    from nrt_mdm.upsert import upsert_source_customer
    from nrt_mdm.mappers import map_crm_a, map_crm_b, map_crm_c

    SOURCE_MAPPERS = {"crm_a": map_crm_a, "crm_b": map_crm_b, "crm_c": map_crm_c}

    def _insert(source_system: str, payload: dict, event_ts=None):
        ts = event_ts or datetime.now(timezone.utc)
        mapper = SOURCE_MAPPERS[source_system]
        record = mapper(payload, ts)
        upsert_source_customer(pg_conn, record)
        pg_conn.commit()
        return record

    return _insert
