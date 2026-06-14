"""E2E test for BIZ-15 Unmatch via REST API.

Tests the full unmatch workflow: merge → verify → split → verify → suppression holds.
Requires Docker stack running (postgres, kafka, mdm-api).

Run:
  python tests/e2e_unmatch.py
"""

import json
import os
import sys
import time

import httpx
from confluent_kafka import Consumer

API_BASE_URL = os.environ.get("MDM_API_URL", "http://localhost:8000")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
POSTGRES_DSN = os.environ.get("POSTGRES_DSN", "postgresql://mdm:mdm@localhost:5432/mdm")
GOLDEN_TOPIC = "topic.mdm.golden"
XREF_TOPIC = "topic.mdm.xref"
AUDIT_TOPIC = "topic.mdm.audit"

client = httpx.Client(base_url=API_BASE_URL, timeout=30.0)


def log(msg):
    print(f"  {msg}")


def section(title):
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


def ingest(source, payload):
    """POST ingest and return result."""
    resp = client.post(f"/api/v1/ingest/{source}", json=payload)
    resp.raise_for_status()
    return resp.json()


def get_customer(customer_id):
    resp = client.get(f"/api/v1/customers/{customer_id}")
    resp.raise_for_status()
    return resp.json()


def get_sources(customer_id):
    resp = client.get(f"/api/v1/customers/{customer_id}/sources")
    if resp.status_code == 404:
        return []
    resp.raise_for_status()
    return resp.json()


def get_history(customer_id):
    resp = client.get(f"/api/v1/customers/{customer_id}/history")
    resp.raise_for_status()
    return resp.json()


def unmatch(cluster_id, records_to_split, reason):
    resp = client.post("/api/v1/admin/unmatch", json={
        "cluster_id": cluster_id,
        "source_records_to_split": records_to_split,
        "reason": reason,
    })
    resp.raise_for_status()
    return resp.json()


def main():
    ts = int(time.time() * 1000)
    father_key = f"FATHER-{ts}"
    son_key = f"SON-{ts}"
    shared_email = f"smith-{ts}@family-test.com"
    errors = []

    print("=" * 60)
    print("  BIZ-15 E2E Unmatch Test (REST API)")
    print("=" * 60)

    # ─── Step 1: Create two records that merge ───────────────────
    section("Step 1: Ingest father + son (same email → merge)")

    r1 = ingest("crm_a", {
        "src_customer_id": father_key,
        "first_name": "Zxq-father",
        "last_name": "Unmatchtest",
        "email": shared_email,
        "phone": f"+999000{ts % 1000000:06d}",
    })
    log(f"Father: cluster={r1['customer_id']}, type={r1['event_type']}, sources={r1.get('source_count')}")

    r2 = ingest("crm_b", {
        "customer_key": son_key,
        "name": "Zxq-son Unmatchtest",
        "email_address": shared_email,
        "mobile": f"+999001{ts % 1000000:06d}",
    })
    log(f"Son:    cluster={r2['customer_id']}, type={r2['event_type']}, sources={r2.get('source_count')}")

    cluster_id = r1["customer_id"]

    if r2["customer_id"] != cluster_id:
        errors.append(f"FAIL: Records did not merge (different clusters: {r1['customer_id']} vs {r2['customer_id']})")
        print(f"\n  ERROR: {errors[-1]}")
        print("  Cannot continue test without merge. Exiting.")
        sys.exit(1)

    if r2.get("source_count") != 2:
        errors.append(f"FAIL: Expected source_count=2, got {r2.get('source_count')}")

    log(f"PASS: Both records merged into cluster {cluster_id} (source_count=2)")

    # ─── Step 2: Verify the incorrect merge ──────────────────────
    section("Step 2: Verify merged state")

    sources = get_sources(cluster_id)
    log(f"Sources in cluster {cluster_id}: {len(sources)}")
    for s in sources:
        log(f"  {s['source_system']}|{s['source_key']} — {s['first_name']} {s['last_name']} ({s['email']})")

    if len(sources) != 2:
        errors.append(f"FAIL: Expected 2 sources, got {len(sources)}")

    # ─── Step 2b: Confirm split ────────────────────────────────
    print()
    print(f"  {'─' * 56}")
    print(f"  REVIEW: Cluster {cluster_id} contains {len(sources)} source records.")
    print(f"  The following record will be SPLIT OUT into a new cluster:")
    print(f"    → CRM_B|{son_key} ({sources[1]['first_name']} {sources[1]['last_name']})")
    print(f"  Reason: \"Father and son share family email\"")
    print(f"  {'─' * 56}")
    input("\n  Press ENTER to confirm unmatch (or Ctrl+C to cancel)... ")
    print()

    # ─── Step 3: Unmatch (split son out) ─────────────────────────
    section("Step 3: Unmatch — split son out of cluster")

    # Subscribe to CDC topics BEFORE calling unmatch (so we catch the events)
    cdc_consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": f"e2e-unmatch-cdc-{ts}",
        "auto.offset.reset": "end",
        "enable.auto.commit": False,
    })
    cdc_consumer.subscribe([GOLDEN_TOPIC, XREF_TOPIC])
    # Force partition assignment and seek to end
    for _ in range(10):
        cdc_consumer.poll(timeout=0.5)
    time.sleep(2)  # ensure assignment is stable

    # NOW call unmatch — CDC will be published after this
    result = unmatch(cluster_id, [
        {"source_system": "CRM_B", "source_key": son_key}
    ], "E2E test: father/son share email")

    new_cluster_id = result["new_cluster_id"]
    log(f"Unmatch result:")
    log(f"  Original cluster: {result['original_cluster_id']} (sources: {result['original_golden']['source_count']})")
    log(f"  New cluster:      {new_cluster_id} (sources: {result['new_golden']['source_count']})")
    log(f"  Suppressions:     {result['suppressions_created']}")

    if result["success"] is not True:
        errors.append("FAIL: unmatch returned success=false")
    if result["original_golden"]["source_count"] != 1:
        errors.append(f"FAIL: Original should have 1 source, got {result['original_golden']['source_count']}")
    if result["new_golden"]["source_count"] != 1:
        errors.append(f"FAIL: New cluster should have 1 source, got {result['new_golden']['source_count']}")
    if result["suppressions_created"] < 1:
        errors.append("FAIL: No suppressions created")

    # ─── Step 3b: Collect CDC Kafka events ───────────────────────
    section("Step 3b: CDC Kafka events (golden + xref)")

    cdc_events = []
    deadline = time.time() + 8.0
    while time.time() < deadline:
        msg = cdc_consumer.poll(timeout=1.0)
        if msg is None or msg.error():
            continue
        try:
            evt = json.loads(msg.value())
            evt["_topic"] = msg.topic()
            cdc_events.append(evt)
        except (json.JSONDecodeError, TypeError):
            pass
    cdc_consumer.close()

    if cdc_events:
        for evt in cdc_events:
            topic = evt.pop("_topic", "")
            topic_label = "golden" if "golden" in topic else "xref"
            log(f"[{topic_label}] {json.dumps(evt, indent=None, default=str)}")
    else:
        log("(no CDC events captured)")

    # ─── Step 4: Verify the split ────────────────────────────────
    section("Step 4: Verify split state")

    # Original cluster
    orig_sources = get_sources(cluster_id)
    log(f"Original cluster {cluster_id}: {len(orig_sources)} source(s)")
    for s in orig_sources:
        log(f"  {s['source_system']}|{s['source_key']}")

    # New cluster
    new_sources = get_sources(new_cluster_id)
    log(f"New cluster {new_cluster_id}: {len(new_sources)} source(s)")
    for s in new_sources:
        log(f"  {s['source_system']}|{s['source_key']}")

    if len(orig_sources) != 1:
        errors.append(f"FAIL: Original cluster should have 1 source, got {len(orig_sources)}")
    if len(new_sources) != 1:
        errors.append(f"FAIL: New cluster should have 1 source, got {len(new_sources)}")

    # ─── Step 5: Verify suppression holds ────────────────────────
    section("Step 5: Re-send son with same email — must NOT re-merge")

    r3 = ingest("crm_b", {
        "customer_key": son_key,
        "name": "Zxq-son Unmatchtest",
        "email_address": shared_email,
        "mobile": f"+999001{ts % 1000000:06d}",
    })
    log(f"Re-sent son: cluster={r3['customer_id']}, type={r3['event_type']}")

    if r3["customer_id"] == cluster_id:
        errors.append(f"FAIL: Son re-merged into father's cluster {cluster_id} despite suppression!")
    else:
        log(f"PASS: Son stayed in cluster {r3['customer_id']} (not re-merged into {cluster_id})")

    # ─── Step 6: Check SCD2 history ──────────────────────────────
    section("Step 6: Verify SCD2 history")

    history = get_history(cluster_id)
    log(f"History rows for original cluster {cluster_id}: {len(history)}")
    for h in history:
        log(f"  valid_from={h['valid_from'][:19]} is_current={h['is_current']} sources={h['source_count']}")

    if len(history) < 2:
        errors.append(f"FAIL: Expected >=2 history rows (before/after split), got {len(history)}")

    # ─── Results ─────────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    if errors:
        print(f"  RESULT: {len(errors)} FAILURE(S)")
        for e in errors:
            print(f"    {e}")
    else:
        print("  RESULT: ALL CHECKS PASSED")
        print(f"    Merge verified (cluster {cluster_id}, 2 sources)")
        print(f"    Unmatch verified (split into {cluster_id} + {new_cluster_id})")
        print(f"    Suppression holds (son not re-merged)")
        print(f"    SCD2 history preserved ({len(history)} rows)")
    print("=" * 60)

    # ─── Step 7: Cleanup test data (always runs) ─────────────────
    print()
    input("  Press ENTER to clean up ALL test data (removes FATHER-*/SON-* records)... ")
    section("Step 7: Cleaning up test data")

    import psycopg
    pg_conn = psycopg.connect(POSTGRES_DSN, autocommit=False)
    with pg_conn.cursor() as cur:
        # Get cluster_ids BEFORE deleting cluster assignments
        cur.execute("SELECT DISTINCT cluster_id FROM customer_clusters WHERE source_key LIKE 'FATHER-%' OR source_key LIKE 'SON-%'")
        test_clusters = [row[0] for row in cur.fetchall()]

        cur.execute("DELETE FROM match_suppressions WHERE source_key_a LIKE 'FATHER-%' OR source_key_a LIKE 'SON-%' OR source_key_b LIKE 'FATHER-%' OR source_key_b LIKE 'SON-%'")
        cur.execute("DELETE FROM audit_events WHERE detail::text LIKE '%FATHER-%' OR detail::text LIKE '%SON-%'")

        # Delete golden records for test clusters
        if test_clusters:
            cur.execute("DELETE FROM golden_customers WHERE cluster_id = ANY(%s)", (test_clusters,))

        cur.execute("DELETE FROM customer_xref WHERE source_key LIKE 'FATHER-%' OR source_key LIKE 'SON-%'")
        cur.execute("DELETE FROM customer_clusters WHERE source_key LIKE 'FATHER-%' OR source_key LIKE 'SON-%'")
        cur.execute("DELETE FROM source_customers WHERE source_key LIKE 'FATHER-%' OR source_key LIKE 'SON-%'")

        # Also clean any orphaned golden records with test names
        cur.execute("DELETE FROM golden_customers WHERE first_name LIKE 'Zxq%'")
    pg_conn.commit()
    pg_conn.close()
    log("ALL test data (FATHER-*/SON-*) removed. Database is clean.")

    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
