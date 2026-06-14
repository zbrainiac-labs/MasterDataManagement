"""REST API throughput test — random GET /api/v1/customers/{id} at max TPS.

Sends concurrent requests to measure maximum transactions per second.
Goal: >150 TPS on local Docker Desktop.

Usage:
  python tests/bench_get_tps.py                    # default: 10s, 20 concurrent
  python tests/bench_get_tps.py --duration 30      # 30 seconds
  python tests/bench_get_tps.py --concurrency 50   # 50 parallel workers
"""

import argparse
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import httpx
import psycopg

API_BASE_URL = os.environ.get("MDM_API_URL", "http://localhost:8000")
POSTGRES_DSN = os.environ.get("POSTGRES_DSN", "postgresql://mdm:mdm@localhost:5432/mdm")


def get_cluster_ids(sample_size: int = 5000) -> list[int]:
    """Fetch random cluster_ids from the database."""
    conn = psycopg.connect(POSTGRES_DSN, autocommit=True)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT cluster_id FROM golden_customers
            WHERE is_current = TRUE
            ORDER BY random()
            LIMIT %s
        """, (sample_size,))
        ids = [row[0] for row in cur.fetchall()]
    conn.close()
    return ids


def run_bench(duration: int, concurrency: int):
    """Run GET requests at max throughput for given duration."""
    print(f"Loading cluster IDs from Postgres...")
    ids = get_cluster_ids(5000)
    if not ids:
        print("ERROR: No golden records found. Load data first.")
        return

    print(f"Loaded {len(ids)} cluster IDs for random selection")
    print(f"Target: GET {API_BASE_URL}/api/v1/customers/{{id}}")
    print(f"Config: duration={duration}s, concurrency={concurrency}")
    print(f"{'=' * 60}")

    # Stats
    total_requests = 0
    total_errors = 0
    latencies = []
    start_time = time.time()
    deadline = start_time + duration

    # Shared HTTP client with connection pooling
    client = httpx.Client(
        base_url=API_BASE_URL,
        timeout=5.0,
        limits=httpx.Limits(max_connections=concurrency + 10, max_keepalive_connections=concurrency),
    )

    def worker():
        """Single worker: send requests until deadline."""
        nonlocal total_requests, total_errors
        local_latencies = []
        while time.time() < deadline:
            cid = random.choice(ids)
            t0 = time.time()
            try:
                resp = client.get(f"/api/v1/customers/{cid}")
                elapsed_ms = (time.time() - t0) * 1000
                if resp.status_code == 200:
                    local_latencies.append(elapsed_ms)
                else:
                    total_errors += 1
            except Exception:
                total_errors += 1
        return local_latencies

    # Run concurrent workers
    print(f"\nRunning {concurrency} workers for {duration}s...\n")
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(worker) for _ in range(concurrency)]

        for future in as_completed(futures):
            latencies.extend(future.result())

    client.close()
    elapsed = time.time() - start_time
    total_requests = len(latencies) + total_errors

    # Report
    tps = len(latencies) / elapsed
    latencies.sort()

    print(f"{'=' * 60}")
    print(f"RESULTS")
    print(f"{'=' * 60}")
    print(f"  Duration:     {elapsed:.1f}s")
    print(f"  Total:        {total_requests} requests ({total_errors} errors)")
    print(f"  Successful:   {len(latencies)} requests")
    print(f"  TPS:          {tps:.1f} req/s")
    print()

    if latencies:
        p50 = latencies[len(latencies) // 2]
        p95 = latencies[int(len(latencies) * 0.95)]
        p99 = latencies[int(len(latencies) * 0.99)]
        avg = sum(latencies) / len(latencies)
        print(f"  Latency p50:  {p50:.1f}ms")
        print(f"  Latency p95:  {p95:.1f}ms")
        print(f"  Latency p99:  {p99:.1f}ms")
        print(f"  Latency avg:  {avg:.1f}ms")
        print()

    # Pass/Fail
    if tps >= 150:
        print(f"  PASS: {tps:.0f} TPS >= 150 TPS goal")
    else:
        print(f"  FAIL: {tps:.0f} TPS < 150 TPS goal")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="REST API GET throughput benchmark")
    parser.add_argument("--duration", type=int, default=10, help="Test duration in seconds (default: 10)")
    parser.add_argument("--concurrency", type=int, default=20, help="Concurrent workers (default: 20)")
    args = parser.parse_args()
    run_bench(args.duration, args.concurrency)
