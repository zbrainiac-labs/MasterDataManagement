#!/usr/bin/env python3
"""Batch re-resolution CLI.

Re-resolves all source records from scratch. Used for:
- Initial bulk load from batch Snowflake pipeline
- Full cluster rebuild after schema/rule changes
- Disaster recovery

Usage:
  python -m nrt_mdm.batch_resolve [--reset]
  python -m nrt_mdm.batch_resolve [--reset] [--legacy]  # slow per-record SQL approach
"""

import argparse
import logging
import os
import sys
import time
from collections import defaultdict

import psycopg
from confluent_kafka import Producer

from nrt_mdm.audit import log_audit
from nrt_mdm.dq import compute_dq_score
from nrt_mdm.matching import compute_match_score, MATCH_THRESHOLD
from nrt_mdm.models import SourceCustomer, GoldenCustomer
from nrt_mdm.producer import publish_golden_if_changed, compute_row_hash
from nrt_mdm.resolver import resolve
from nrt_mdm.survivorship import compute_golden, _pick_best, _is_valid_name, _is_valid_email, _is_valid_phone, SOURCE_PRIORITY

logger = logging.getLogger(__name__)

FETCH_ALL_SOURCES_SQL = """
SELECT source_system, source_key, first_name, last_name,
       canonical_first_name, email, phone,
       block_soundex, block_email_domain, block_phone_suffix,
       event_timestamp
FROM source_customers
ORDER BY event_timestamp ASC
"""

RESET_CLUSTER_SQL = "TRUNCATE customer_clusters, golden_customers, customer_xref RESTART IDENTITY"

# Maximum block size — blocks larger than this are too generic for matching
MAX_BLOCK_SIZE = 500


# ---------------------------------------------------------------------------
# In-memory Union-Find
# ---------------------------------------------------------------------------

class UnionFind:
    """Disjoint set / Union-Find with path compression and union by rank."""

    def __init__(self, n: int):
        self.parent = list(range(n))
        self.rank = [0] * n

    def find(self, x: int) -> int:
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]  # path compression
            x = self.parent[x]
        return x

    def union(self, x: int, y: int) -> bool:
        rx, ry = self.find(x), self.find(y)
        if rx == ry:
            return False
        if self.rank[rx] < self.rank[ry]:
            rx, ry = ry, rx
        self.parent[ry] = rx
        if self.rank[rx] == self.rank[ry]:
            self.rank[rx] += 1
        return True


# ---------------------------------------------------------------------------
# Fast batch resolve (in-memory)
# ---------------------------------------------------------------------------

def batch_resolve_fast(reset: bool = False):
    """Re-resolve all source records using in-memory blocking + Union-Find.

    Much faster than per-record SQL approach:
    - Loads all records into memory (~50MB for 1M records)
    - Builds blocking indexes as dicts
    - Resolves within each block using Union-Find
    - Writes results back to Postgres in bulk
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    dsn = os.environ.get("POSTGRES_DSN", "postgresql://mdm:mdm@localhost:5432/mdm")
    conn = psycopg.connect(dsn, autocommit=False)

    if reset:
        logger.info("Resetting clusters, golden records, and XREF...")
        with conn.cursor() as cur:
            cur.execute(RESET_CLUSTER_SQL)
        conn.commit()

    # --- Step 1: Load all source records into memory ---
    logger.info("Loading source records...")
    load_start = time.time()
    with conn.cursor() as cur:
        cur.execute(FETCH_ALL_SOURCES_SQL)
        rows = cur.fetchall()

    records: list[SourceCustomer] = []
    for row in rows:
        records.append(SourceCustomer(
            source_system=row[0], source_key=row[1],
            first_name=row[2], last_name=row[3],
            canonical_first_name=row[4], email=row[5], phone=row[6],
            block_soundex=row[7], block_email_domain=row[8],
            block_phone_suffix=row[9], event_timestamp=row[10],
        ))

    n = len(records)
    logger.info("Loaded %d records in %.1fs", n, time.time() - load_start)

    if n == 0:
        logger.info("No records to resolve.")
        conn.close()
        return

    # --- Step 2: Build blocking index ---
    logger.info("Building blocking index...")
    blocks: dict[tuple, list[int]] = defaultdict(list)
    for i, rec in enumerate(records):
        if rec.block_soundex:
            blocks[("S", rec.block_soundex)].append(i)
        if rec.block_email_domain:
            blocks[("E", rec.block_email_domain)].append(i)
        if rec.block_phone_suffix:
            blocks[("P", rec.block_phone_suffix)].append(i)

    # Filter out oversized blocks
    total_blocks = len(blocks)
    blocks = {k: v for k, v in blocks.items() if len(v) <= MAX_BLOCK_SIZE}
    skipped = total_blocks - len(blocks)
    if skipped:
        logger.info("Skipped %d oversized blocks (>%d members)", skipped, MAX_BLOCK_SIZE)
    logger.info("Active blocks: %d", len(blocks))

    # --- Step 3: Resolve within blocks using Union-Find ---
    logger.info("Resolving matches within blocks...")
    resolve_start = time.time()
    uf = UnionFind(n)
    comparisons = 0
    merges = 0

    sorted_blocks = sorted(blocks.items(), key=lambda x: len(x[1]), reverse=True)
    blocks_processed = 0

    for block_key, members in sorted_blocks:
        m = len(members)
        for i in range(m):
            for j in range(i + 1, m):
                idx_a, idx_b = members[i], members[j]
                # Skip if already in same cluster
                if uf.find(idx_a) == uf.find(idx_b):
                    continue
                comparisons += 1
                if compute_match_score(records[idx_a], records[idx_b]) >= MATCH_THRESHOLD:
                    uf.union(idx_a, idx_b)
                    merges += 1

        blocks_processed += 1
        if blocks_processed % 5000 == 0:
            elapsed = time.time() - resolve_start
            logger.info("  blocks: %d/%d  comparisons: %d  merges: %d  (%.1fs)",
                        blocks_processed, len(blocks), comparisons, merges, elapsed)

    resolve_elapsed = time.time() - resolve_start
    logger.info("Resolution complete: %d comparisons, %d merges in %.1fs",
                comparisons, merges, resolve_elapsed)

    # --- Step 4: Assign cluster IDs ---
    logger.info("Assigning cluster IDs...")
    # Map root -> cluster_id
    root_to_cluster: dict[int, int] = {}
    cluster_id_seq = 0
    record_clusters: list[int] = [0] * n

    for i in range(n):
        root = uf.find(i)
        if root not in root_to_cluster:
            cluster_id_seq += 1
            root_to_cluster[root] = cluster_id_seq
        record_clusters[i] = root_to_cluster[root]

    num_clusters = len(root_to_cluster)
    logger.info("Clusters: %d (from %d records, merge rate %.1f%%)",
                num_clusters, n, (1 - num_clusters / n) * 100)

    # --- Step 5: Write clusters + XREF to Postgres (bulk) ---
    logger.info("Writing clusters and XREF to Postgres...")
    write_start = time.time()
    with conn.cursor() as cur:
        # Bulk insert clusters
        batch = []
        for i, rec in enumerate(records):
            batch.append((rec.source_system, rec.source_key, record_clusters[i]))
            if len(batch) >= 5000:
                cur.executemany(
                    "INSERT INTO customer_clusters (source_system, source_key, cluster_id) VALUES (%s, %s, %s)",
                    batch
                )
                batch.clear()
        if batch:
            cur.executemany(
                "INSERT INTO customer_clusters (source_system, source_key, cluster_id) VALUES (%s, %s, %s)",
                batch
            )

        # Bulk insert XREF
        batch = []
        for i, rec in enumerate(records):
            batch.append((rec.source_system, rec.source_key, record_clusters[i]))
            if len(batch) >= 5000:
                cur.executemany(
                    "INSERT INTO customer_xref (source_system, source_key, customer_id) VALUES (%s, %s, %s)",
                    batch
                )
                batch.clear()
        if batch:
            cur.executemany(
                "INSERT INTO customer_xref (source_system, source_key, customer_id) VALUES (%s, %s, %s)",
                batch
            )

    conn.commit()
    logger.info("Clusters + XREF written in %.1fs", time.time() - write_start)

    # --- Step 6: Compute golden records per cluster ---
    logger.info("Computing golden records for %d clusters...", num_clusters)
    golden_start = time.time()

    # Group records by cluster for in-memory survivorship
    cluster_members: dict[int, list[int]] = defaultdict(list)
    for i in range(n):
        cluster_members[record_clusters[i]].append(i)

    golden_count = 0
    with conn.cursor() as cur:
        batch = []
        for cid, member_indices in cluster_members.items():
            # In-memory survivorship
            recs_data = []
            source_systems = set()
            for idx in member_indices:
                r = records[idx]
                recs_data.append({
                    "source_system": r.source_system,
                    "first_name": r.first_name,
                    "last_name": r.last_name,
                    "email": r.email,
                    "phone": r.phone,
                    "event_timestamp": r.event_timestamp,
                })
                source_systems.add(r.source_system)

            # Sort by recency
            recs_data.sort(key=lambda x: x["event_timestamp"], reverse=True)

            first_name = _pick_best(recs_data, "first_name", _is_valid_name)
            last_name = _pick_best(recs_data, "last_name", _is_valid_name)
            email = _pick_best(recs_data, "email", _is_valid_email)
            phone = _pick_best(recs_data, "phone", _is_valid_phone)

            golden = GoldenCustomer(
                cluster_id=cid, first_name=first_name, last_name=last_name,
                email=email, phone=phone, dq_score=0, source_count=len(source_systems),
            )
            golden.dq_score = compute_dq_score(golden)
            golden.row_hash = compute_row_hash(golden)

            batch.append((cid, first_name, last_name, email, phone,
                          golden.dq_score, len(source_systems), golden.row_hash))
            golden_count += 1

            if len(batch) >= 1000:
                cur.executemany("""
                    INSERT INTO golden_customers (cluster_id, first_name, last_name, email, phone,
                        dq_score, source_count, row_hash, valid_from, valid_to, is_current)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), '9999-12-31', TRUE)
                """, batch)
                batch.clear()

        if batch:
            cur.executemany("""
                INSERT INTO golden_customers (cluster_id, first_name, last_name, email, phone,
                    dq_score, source_count, row_hash, valid_from, valid_to, is_current)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), '9999-12-31', TRUE)
            """, batch)

    total_elapsed = time.time() - load_start

    # Audit record for batch operation
    log_audit(
        conn,
        event_type="BATCH_RESOLVE",
        actor="mdm-engine:batch",
        action="TRUNCATE_AND_REBUILD",
        detail={
            "source_count": n,
            "cluster_count": num_clusters,
            "golden_count": golden_count,
            "duration_seconds": round(total_elapsed, 1),
        },
    )

    conn.commit()
    conn.close()

    logger.info("Golden records: %d (computed in %.1fs)", golden_count, time.time() - golden_start)
    logger.info(
        "Batch re-resolution complete: %d records -> %d clusters -> %d golden records (%.1fs total)",
        n, num_clusters, golden_count, total_elapsed,
    )


# ---------------------------------------------------------------------------
# Legacy batch resolve (per-record SQL, slow for large datasets)
# ---------------------------------------------------------------------------

def batch_resolve_legacy(reset: bool = False):
    """Re-resolve all source records sequentially (original approach).

    Slow for >10K records due to per-record SQL queries.
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    dsn = os.environ.get("POSTGRES_DSN", "postgresql://mdm:mdm@localhost:5432/mdm")
    conn = psycopg.connect(dsn, autocommit=False)

    producer = Producer({
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "acks": "all",
        "enable.idempotence": True,
    })

    if reset:
        logger.info("Resetting clusters, golden records, and XREF...")
        with conn.cursor() as cur:
            cur.execute(RESET_CLUSTER_SQL)
        conn.commit()

    with conn.cursor() as cur:
        cur.execute(FETCH_ALL_SOURCES_SQL)
        rows = cur.fetchall()

    logger.info("Re-resolving %d source records (legacy mode)...", len(rows))
    start = time.time()
    processed = 0
    clusters_affected: set[int] = set()

    for row in rows:
        record = SourceCustomer(
            source_system=row[0], source_key=row[1],
            first_name=row[2], last_name=row[3],
            canonical_first_name=row[4], email=row[5], phone=row[6],
            block_soundex=row[7], block_email_domain=row[8],
            block_phone_suffix=row[9], event_timestamp=row[10],
        )
        cluster_id, _ = resolve(conn, record)
        clusters_affected.add(cluster_id)
        processed += 1
        if processed % 100 == 0:
            conn.commit()
            logger.info("  processed %d/%d records...", processed, len(rows))

    conn.commit()

    logger.info("Recomputing golden records for %d clusters...", len(clusters_affected))
    golden_count = 0
    for cluster_id in clusters_affected:
        golden = compute_golden(conn, cluster_id)
        if golden:
            golden.dq_score = compute_dq_score(golden)
            publish_golden_if_changed(producer, conn, golden)
            golden_count += 1

    conn.commit()
    producer.flush()

    elapsed = time.time() - start
    logger.info(
        "Batch re-resolution complete: %d records -> %d clusters -> %d golden records (%.1fs)",
        processed, len(clusters_affected), golden_count, elapsed,
    )
    conn.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Batch re-resolution CLI")
    parser.add_argument("--reset", action="store_true", help="Truncate clusters/golden/xref before re-resolving")
    parser.add_argument("--legacy", action="store_true", help="Use slow per-record SQL approach (for debugging)")
    args = parser.parse_args()

    if args.legacy:
        batch_resolve_legacy(reset=args.reset)
    else:
        batch_resolve_fast(reset=args.reset)


if __name__ == "__main__":
    main()
