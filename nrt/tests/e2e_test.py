#!/usr/bin/env python3
"""E2E test script for NRT MDM pipeline (TST-02 + TST-04).

Modes:
  --mode single      (default) Send 1 random update, show BEFORE/AFTER/CDC, exit.
  --mode continuous  Send updates at --rate/sec, print latency stats. Ctrl+C to stop.

Scale:
  --scale small      (default) 1,500 records from CSV, single update trace.
  --scale medium     100K records in-memory, timed steady-state + report.
  --scale large      1M records in-memory, timed steady-state + report.

Examples:
  ./run_e2e.sh                                     # quick functional test
  ./run_e2e.sh --mode continuous --rate 5          # latency monitoring
  ./run_e2e.sh --scale medium --duration 60        # 100K load + 1 min steady-state
  ./run_e2e.sh --scale large --duration 300        # 1M load + 5 min steady-state

Prerequisites:
  docker compose up -d  (postgres + kafka + mdm-engine must be running)
"""

import argparse
import json
import os
import random
import subprocess
import sys
import time

import psycopg
from confluent_kafka import Consumer, Producer, KafkaError

# Connection defaults match docker-compose.yml
POSTGRES_DSN = os.environ.get("POSTGRES_DSN", "postgresql://mdm:mdm@localhost:5432/mdm")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
GOLDEN_TOPIC = "topic.mdm.golden"
XREF_TOPIC = "topic.mdm.xref"

TOPIC_CRM_A = "topic.crm.a"
TOPIC_CRM_B = "topic.crm.b"
TOPIC_CRM_C = "topic.crm.c"


# ---------------------------------------------------------------------------
# Phase 1: Initial Load
# ---------------------------------------------------------------------------

def get_golden_count(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM golden_customers WHERE is_current = TRUE")
        return cur.fetchone()[0]


def get_source_count(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM source_customers")
        return cur.fetchone()[0]


def run_initial_load_csv():
    """Load CSV data directly into Postgres source_customers via COPY-like bulk insert.
    Then run batch_resolve to compute clusters + golden records.
    Much faster than Kafka replay (~seconds vs minutes).
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(os.path.dirname(script_dir))
    shared_dir = os.path.join(repo_root, "shared")

    # Generate CSV data if not present
    output_dir = os.path.join(shared_dir, "output", "initial")
    if not os.path.exists(output_dir):
        print("  Generating test data...")
        subprocess.run([sys.executable, os.path.join(shared_dir, "scripts", "generate_test_data.py")],
                       capture_output=True, timeout=120)

    # Read CSVs and bulk insert into Postgres
    import csv
    from datetime import datetime, timezone

    conn = psycopg.connect(POSTGRES_DSN)
    conn.autocommit = False
    now = datetime.now(timezone.utc)
    count = 0

    print("  Loading CSVs directly into Postgres...")

    # CRM_A
    crm_a_dir = os.path.join(output_dir, "A", "customer")
    if os.path.exists(crm_a_dir):
        for csv_file in sorted(os.listdir(crm_a_dir)):
            if csv_file.endswith(".csv"):
                with open(os.path.join(crm_a_dir, csv_file)) as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        _insert_source(conn, "CRM_A", row["src_customer_id"],
                                       row.get("first_name"), row.get("last_name"),
                                       row.get("email"), row.get("phone"), now)
                        count += 1

    # CRM_B
    crm_b_dir = os.path.join(output_dir, "B", "customer")
    if os.path.exists(crm_b_dir):
        for csv_file in sorted(os.listdir(crm_b_dir)):
            if csv_file.endswith(".csv"):
                with open(os.path.join(crm_b_dir, csv_file)) as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        name = row.get("name", "")
                        parts = name.split(" ", 1)
                        first = parts[0] if parts else None
                        last = parts[1] if len(parts) > 1 else None
                        _insert_source(conn, "CRM_B", row["customer_key"],
                                       first, last,
                                       row.get("email_address"), row.get("mobile"), now)
                        count += 1

    # CRM_C
    crm_c_dir = os.path.join(output_dir, "C", "customer")
    if os.path.exists(crm_c_dir):
        for csv_file in sorted(os.listdir(crm_c_dir)):
            if csv_file.endswith(".csv"):
                with open(os.path.join(crm_c_dir, csv_file)) as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        name = row.get("caller_name", "")
                        parts = name.split(" ", 1)
                        first = parts[0] if parts else None
                        last = parts[1] if len(parts) > 1 else None
                        _insert_source(conn, "CRM_C", row["ticket_customer_id"],
                                       first, last,
                                       row.get("callback_email"), row.get("callback_phone"), now)
                        count += 1

    conn.commit()
    conn.close()
    print(f"  Inserted {count:,} source records into Postgres")

    # Run batch_resolve to compute clusters + golden records
    print("  Running batch_resolve...")
    env = os.environ.copy()
    env["POSTGRES_DSN"] = POSTGRES_DSN
    env["KAFKA_BOOTSTRAP_SERVERS"] = KAFKA_BOOTSTRAP
    result = subprocess.run(
        [sys.executable, "-m", "nrt_mdm.batch_resolve", "--reset"],
        cwd=os.path.dirname(script_dir),
        env=env,
        capture_output=True,
        text=True,
        timeout=600,
    )
    if result.returncode != 0:
        print(f"  ERROR: batch_resolve failed: {result.stderr[-500:]}")
        return False
    # Extract last line of output
    lines = result.stdout.strip().split("\n")
    for line in lines[-3:]:
        print(f"  {line.strip()}")
    return True


def run_initial_load_inmemory(scale: str):
    """Generate customers in-memory, bulk insert to Postgres, batch_resolve."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(os.path.dirname(script_dir))
    sys.path.insert(0, os.path.join(repo_root, "shared", "scripts"))

    from generate_test_data import generate_all

    print(f"  Generating {scale} dataset in-memory...")
    gen_start = time.time()
    customers_a, customers_b, customers_c = generate_all(scale)
    gen_elapsed = time.time() - gen_start
    total = len(customers_a) + len(customers_b) + len(customers_c)
    print(f"  Generated {total:,} records in {gen_elapsed:.1f}s")

    # Bulk insert into Postgres
    print(f"  Bulk inserting into Postgres...")
    from datetime import datetime, timezone
    conn = psycopg.connect(POSTGRES_DSN)
    conn.autocommit = False
    now = datetime.now(timezone.utc)
    count = 0

    for c in customers_a:
        _insert_source(conn, "CRM_A", c.id, c.first_name, c.last_name, c.email, c.phone, now)
        count += 1
        if count % 10000 == 0:
            conn.commit()
            sys.stdout.write(f"\r  Inserted: {count:,} / {total:,}")
            sys.stdout.flush()

    for c in customers_b:
        _insert_source(conn, "CRM_B", c.id, c.first_name, c.last_name, c.email, c.phone, now)
        count += 1
        if count % 10000 == 0:
            conn.commit()
            sys.stdout.write(f"\r  Inserted: {count:,} / {total:,}")
            sys.stdout.flush()

    for c in customers_c:
        _insert_source(conn, "CRM_C", c.id, c.first_name, c.last_name, c.email, c.phone, now)
        count += 1
        if count % 10000 == 0:
            conn.commit()
            sys.stdout.write(f"\r  Inserted: {count:,} / {total:,}")
            sys.stdout.flush()

    conn.commit()
    conn.close()
    insert_elapsed = time.time() - gen_start - gen_elapsed
    print(f"\r  Inserted {count:,} records in {insert_elapsed:.1f}s ({count/insert_elapsed:.0f} rows/s)")

    # Run batch_resolve
    print("  Running batch_resolve...")
    env = os.environ.copy()
    env["POSTGRES_DSN"] = POSTGRES_DSN
    env["KAFKA_BOOTSTRAP_SERVERS"] = KAFKA_BOOTSTRAP
    result = subprocess.run(
        [sys.executable, "-m", "nrt_mdm.batch_resolve", "--reset"],
        cwd=os.path.join(repo_root, "nrt"),
        env=env,
        capture_output=True,
        text=True,
        timeout=3600,
    )
    if result.returncode != 0:
        print(f"  ERROR: batch_resolve failed: {result.stderr[-500:]}")
        return False
    lines = result.stdout.strip().split("\n")
    for line in lines[-3:]:
        print(f"  {line.strip()}")
    return True


def _insert_source(conn, source_system: str, source_key: str,
                   first_name: str | None, last_name: str | None,
                   email: str | None, phone: str | None, event_ts) -> None:
    """Insert a single source record with computed blocking keys."""
    import re
    import jellyfish

    # Normalize
    first_name = first_name.strip().title() if first_name and first_name.strip() else None
    last_name = last_name.strip().title() if last_name and last_name.strip() else None
    email = email.strip().lower() if email and email.strip() else None
    phone = re.sub(r"[^0-9+]", "", phone) if phone else None

    canonical_first = first_name
    block_soundex = jellyfish.soundex(last_name) if last_name else None
    block_email_domain = email[email.index("@"):] if email and "@" in email else None
    block_phone_suffix = re.sub(r"[^0-9]", "", phone)[-4:] if phone and len(re.sub(r"[^0-9]", "", phone)) >= 4 else None

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO source_customers (source_system, source_key, first_name, last_name,
                canonical_first_name, email, phone, block_soundex, block_email_domain,
                block_phone_suffix, event_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_system, source_key) DO NOTHING
        """, (source_system, source_key, first_name, last_name,
              canonical_first, email, phone, block_soundex, block_email_domain,
              block_phone_suffix, event_ts))


def wait_for_golden_stable(conn, timeout: int = 180, min_expected: int = 10) -> int:
    """Wait for golden count to stabilize (consumer finished processing)."""
    prev_count = 0
    stable_ticks = 0
    deadline = time.time() + timeout

    while time.time() < deadline:
        count = get_golden_count(conn)
        if count == prev_count and count >= min_expected:
            stable_ticks += 1
            if stable_ticks >= 3:
                return count
        else:
            stable_ticks = 0
        prev_count = count
        sys.stdout.write(f"\r  Golden records: {count:,} (waiting to stabilize...)")
        sys.stdout.flush()
        time.sleep(3)

    print()
    return prev_count


# ---------------------------------------------------------------------------
# Phase 2: Single Update
# ---------------------------------------------------------------------------

def pick_random_source(conn) -> dict | None:
    """Pick a random source record from any CRM that has a golden record."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT sc.source_system, sc.source_key, sc.first_name, sc.last_name,
                   sc.email, sc.phone, cc.cluster_id
            FROM source_customers sc
            JOIN customer_clusters cc ON sc.source_system = cc.source_system AND sc.source_key = cc.source_key
            ORDER BY RANDOM()
            LIMIT 1
        """)
        row = cur.fetchone()
    if row is None:
        return None
    return {
        "source_system": row[0], "source_key": row[1],
        "first_name": row[2], "last_name": row[3],
        "email": row[4], "phone": row[5], "cluster_id": row[6],
    }


def build_update_event(source_rec: dict) -> tuple[str, str, dict]:
    """Build an update event with a changed email."""
    ss = source_rec["source_system"]
    key = source_rec["source_key"]
    new_email = source_rec["email"]

    if new_email and "@" in new_email:
        local = new_email.split("@")[0]
        new_email = f"{local}@updated-corp.com"

    if ss == "CRM_A":
        return TOPIC_CRM_A, key, {
            "src_customer_id": key,
            "first_name": source_rec["first_name"] or "",
            "last_name": source_rec["last_name"] or "",
            "email": new_email or "",
            "phone": source_rec["phone"] or "",
        }
    elif ss == "CRM_B":
        name = f"{source_rec['first_name'] or ''} {source_rec['last_name'] or ''}".strip()
        return TOPIC_CRM_B, key, {
            "customer_key": key, "name": name,
            "email_address": new_email or "", "mobile": source_rec["phone"] or "",
        }
    else:
        caller = f"{source_rec['first_name'] or ''} {source_rec['last_name'] or ''}".strip()
        return TOPIC_CRM_C, key, {
            "ticket_customer_id": key, "caller_name": caller,
            "callback_email": new_email or "", "callback_phone": source_rec["phone"] or "",
        }


def get_golden_for_cluster(conn, cluster_id: int) -> dict | None:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT first_name, last_name, email, phone, dq_score, source_count, row_hash
            FROM golden_customers WHERE cluster_id = %s AND is_current = TRUE
        """, (cluster_id,))
        row = cur.fetchone()
    if row is None:
        return None
    return {"cluster_id": cluster_id, "first_name": row[0], "last_name": row[1],
            "email": row[2], "phone": row[3], "dq_score": row[4],
            "source_count": row[5], "row_hash": row[6]}


def format_golden(g: dict | None) -> str:
    if g is None:
        return "(none)"
    return (f"cluster={g['cluster_id']}: {g['first_name']} {g['last_name']} | "
            f"email={g['email']} | phone={g['phone']} | "
            f"dq={g['dq_score']} | sources={g['source_count']}")


# ---------------------------------------------------------------------------
# Continuous / Timed mode
# ---------------------------------------------------------------------------

def run_continuous(conn, rate: float = 1.0, duration: int | None = None):
    """Send updates continuously, printing latency for each.

    If duration is set, stop after that many seconds and print report.
    """
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    latencies = []
    count = 0
    interval = 1.0 / rate
    start_time = time.time()

    mode_label = f"{duration}s timed" if duration else "indefinite (Ctrl+C)"
    print(f"\n  Continuous mode: {rate} update(s)/sec, {mode_label}")
    print(f"  {'#':<6} {'Source':<20} {'Cluster':<8} {'Latency':>10} {'p50':>8} {'p95':>8} {'p99':>8}")
    print(f"  {'-'*6} {'-'*20} {'-'*8} {'-'*10} {'-'*8} {'-'*8} {'-'*8}")

    try:
        while True:
            if duration and (time.time() - start_time) >= duration:
                break

            source_rec = pick_random_source(conn)
            if source_rec is None:
                time.sleep(1)
                continue

            cluster_id = source_rec["cluster_id"]
            golden_before = get_golden_for_cluster(conn, cluster_id)

            topic, key, payload = build_update_event(source_rec)
            ts_ms = int(time.time() * 1000)
            producer.produce(topic, key=key, value=json.dumps(payload).encode(), timestamp=ts_ms)
            producer.flush()
            produce_time = time.time()

            # Poll until change detected
            for _ in range(100):  # max 5s
                time.sleep(0.05)
                golden_after = get_golden_for_cluster(conn, cluster_id)
                if golden_after and golden_after.get("row_hash") != (golden_before or {}).get("row_hash"):
                    break

            latency_ms = int((time.time() - produce_time) * 1000)
            latencies.append(latency_ms)
            count += 1

            s = sorted(latencies)
            p50 = s[len(s) // 2]
            p95 = s[int(len(s) * 0.95)]
            p99 = s[int(len(s) * 0.99)]

            source_label = f"{source_rec['source_system']}|{source_rec['source_key']}"
            print(f"  {count:<6} {source_label:<20} {cluster_id:<8} {latency_ms:>7}ms {p50:>5}ms {p95:>5}ms {p99:>5}ms")

            # Throttle
            elapsed = time.time() - produce_time
            if elapsed < interval:
                time.sleep(interval - elapsed)

    except KeyboardInterrupt:
        pass

    total_elapsed = time.time() - start_time
    print(f"\n  Completed {count} updates in {total_elapsed:.1f}s")
    if latencies:
        s = sorted(latencies)
        print(f"  Latency: min={s[0]}ms  p50={s[len(s)//2]}ms  p95={s[int(len(s)*0.95)]}ms  p99={s[int(len(s)*0.99)]}ms  max={s[-1]}ms")

    return latencies


# ---------------------------------------------------------------------------
# Phase 3: Report (medium/large scale)
# ---------------------------------------------------------------------------

def print_report(conn, latencies: list[int], bulk_elapsed: float, scale: str):
    """Print summary report with Postgres stats."""
    print(f"\n{'=' * 70}")
    print(f"LOAD TEST REPORT (scale={scale})")
    print(f"{'=' * 70}")

    # Record counts
    source_count = get_source_count(conn)
    golden_count = get_golden_count(conn)
    merge_rate = (1 - golden_count / source_count) * 100 if source_count > 0 else 0

    print(f"\n  Source records:   {source_count:>12,}")
    print(f"  Golden records:   {golden_count:>12,}")
    print(f"  Merge rate:       {merge_rate:>11.1f}%")
    print(f"  Bulk load time:   {bulk_elapsed:>11.1f}s")
    if bulk_elapsed > 0:
        print(f"  Bulk throughput:  {source_count / bulk_elapsed:>11.0f} msg/s")

    # Latency stats
    if latencies:
        s = sorted(latencies)
        print(f"\n  Steady-state latency ({len(latencies)} updates):")
        print(f"    min:  {s[0]:>6}ms")
        print(f"    p50:  {s[len(s)//2]:>6}ms")
        print(f"    p95:  {s[int(len(s)*0.95)]:>6}ms")
        print(f"    p99:  {s[int(len(s)*0.99)]:>6}ms")
        print(f"    max:  {s[-1]:>6}ms")

    # Postgres table sizes
    with conn.cursor() as cur:
        for table in ["source_customers", "customer_clusters", "golden_customers", "customer_xref"]:
            cur.execute(f"SELECT pg_total_relation_size('{table}'), pg_size_pretty(pg_total_relation_size('{table}'))")
            _bytes, pretty = cur.fetchone()
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            row_count = cur.fetchone()[0]
            print(f"\n  {table}:")
            print(f"    rows: {row_count:,}  size: {pretty}")

    print(f"\n{'=' * 70}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="NRT MDM E2E Test")
    parser.add_argument("--mode", choices=["single", "continuous"], default="single",
                        help="single: 1 update + exit. continuous: loop with latency stats.")
    parser.add_argument("--scale", choices=["small", "medium", "large"], default="small",
                        help="Data volume: small=1.5K, medium=100K, large=1M")
    parser.add_argument("--rate", type=float, default=1.0,
                        help="Updates per second in continuous mode (default: 1.0)")
    parser.add_argument("--duration", type=int, default=300,
                        help="Steady-state duration in seconds for medium/large (default: 300)")
    args = parser.parse_args()

    # For medium/large scale with no explicit mode override, default to continuous
    if args.scale in ("medium", "large") and args.mode == "single" and "--mode" not in sys.argv:
        args.mode = "continuous"
        if args.rate == 1.0:
            args.rate = 100.0  # default to 100/sec for load tests

    print("=" * 70)
    print(f"NRT MDM E2E Test (scale={args.scale}, mode={args.mode})")
    print("=" * 70)

    # Connect to Postgres
    try:
        conn = psycopg.connect(POSTGRES_DSN)
        conn.autocommit = True
    except Exception as e:
        print(f"\n  FAIL: Cannot connect to Postgres: {e}")
        print("  Is docker compose running? Try: docker compose up -d")
        sys.exit(1)

    # =========================================================================
    # Phase 1: Initial Load
    # =========================================================================
    print("\n[Phase 1] Initial Load")
    golden_count = get_golden_count(conn)
    source_count = get_source_count(conn)

    bulk_elapsed = 0.0
    if golden_count > 100:
        print(f"  Already loaded: {source_count:,} source records -> {golden_count:,} golden records")
    else:
        bulk_start = time.time()

        if args.scale == "small":
            print("  Loading from CSV -> Postgres -> batch_resolve...")
            success = run_initial_load_csv()
        else:
            success = run_initial_load_inmemory(args.scale)

        if not success:
            print("  FAIL: Could not load data")
            sys.exit(1)

        bulk_elapsed = time.time() - bulk_start
        golden_count = get_golden_count(conn)
        source_count = get_source_count(conn)
        print(f"  Initial load complete: {source_count:,} sources -> {golden_count:,} golden ({bulk_elapsed:.1f}s)")

    if golden_count < 10:
        print("  FAIL: Insufficient golden records after initial load")
        sys.exit(1)

    # =========================================================================
    # Phase 2: Updates
    # =========================================================================
    if args.mode == "continuous":
        duration = args.duration if args.scale != "small" else None
        latencies = run_continuous(conn, rate=args.rate, duration=duration)

        # Phase 3: Report for medium/large
        if args.scale in ("medium", "large"):
            print_report(conn, latencies, bulk_elapsed, args.scale)
        sys.exit(0)

    # --- Single update mode (small scale default) ---
    print("\n[Phase 2] Single Random Update")

    source_rec = pick_random_source(conn)
    if source_rec is None:
        print("  FAIL: No source records found")
        sys.exit(1)

    cluster_id = source_rec["cluster_id"]
    print(f"  Selected: {source_rec['source_system']}|{source_rec['source_key']} (cluster={cluster_id})")

    golden_before = get_golden_for_cluster(conn, cluster_id)
    print(f"\n  --- Golden Record (BEFORE) ---")
    print(f"    {format_golden(golden_before)}")

    topic, key, payload = build_update_event(source_rec)
    print(f"\n  --- Inbound Event ---")
    print(f"  [{topic}] key={key}")
    print(f"    {json.dumps(payload)}")

    # Subscribe CDC consumer
    cdc_consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": f"e2e-cdc-{int(time.time())}",
        "auto.offset.reset": "end",
        "enable.auto.commit": False,
    })
    cdc_consumer.subscribe([GOLDEN_TOPIC, XREF_TOPIC])
    for _ in range(5):
        cdc_consumer.poll(timeout=1.0)
    time.sleep(1)

    # Produce
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    ts_ms = int(time.time() * 1000)
    producer.produce(topic, key=key, value=json.dumps(payload).encode(), timestamp=ts_ms)
    producer.flush()
    produce_time = time.time()
    print("\n  Produced 1 update event")

    # Poll until change
    print("  Waiting for processing...", end="", flush=True)
    golden_after = None
    for _ in range(50):
        time.sleep(0.1)
        golden_after = get_golden_for_cluster(conn, cluster_id)
        if golden_after and golden_after.get("row_hash") != (golden_before or {}).get("row_hash"):
            break
    latency_ms = int((time.time() - produce_time) * 1000)
    print(f" done ({latency_ms}ms)")

    golden_after = get_golden_for_cluster(conn, cluster_id)
    print(f"\n  --- Golden Record (AFTER) ---")
    print(f"    {format_golden(golden_after)}")

    # Diff
    if golden_before and golden_after:
        if golden_before["row_hash"] != golden_after.get("row_hash"):
            print("\n  --- Changes ---")
            for field in ["first_name", "last_name", "email", "phone", "dq_score", "source_count"]:
                old = golden_before.get(field)
                new = golden_after.get(field)
                if old != new:
                    print(f"    {field}: {old} -> {new}")
        else:
            print("    (no change in golden attributes)")

    # CDC events
    print(f"\n  --- CDC Kafka Events ---")
    cdc_events = []
    first_cdc_time = None
    deadline = time.time() + 8.0
    while time.time() < deadline:
        msg = cdc_consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            continue
        try:
            evt = json.loads(msg.value())
            evt["_topic"] = msg.topic()
            cdc_events.append(evt)
            if first_cdc_time is None:
                first_cdc_time = time.time()
        except (json.JSONDecodeError, TypeError):
            pass
    cdc_consumer.close()

    if not cdc_events:
        print("    (none captured)")
    else:
        e2e_latency_ms = int((first_cdc_time - produce_time) * 1000)
        print(f"    End-to-end latency (inbound Kafka -> outbound Kafka): {e2e_latency_ms}ms")
    for evt in cdc_events:
        t = evt.pop("_topic", "")
        topic_label = "golden" if t == GOLDEN_TOPIC else "xref"
        print(f"    [{topic_label}] {json.dumps(evt, indent=None)}")

    # Validation
    print(f"\n{'=' * 70}")
    errors = []
    if golden_after is None:
        errors.append("FAIL: No golden record found after update")
    elif golden_before and golden_after:
        if golden_after["dq_score"] is None or golden_after["dq_score"] < 0:
            errors.append(f"FAIL: Invalid DQ score: {golden_after['dq_score']}")

    if errors:
        print(f"RESULT: FAILED ({len(errors)} errors)")
        for e in errors:
            print(f"  {e}")
        sys.exit(1)
    else:
        print("RESULT: ALL CHECKS PASSED")
        print(f"  Golden records: {golden_count:,}")
        print(f"  Update processed for cluster {cluster_id}")
        sys.exit(0)


if __name__ == "__main__":
    main()
