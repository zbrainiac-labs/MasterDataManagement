"""Cluster manager: blocking, candidate lookup, matching, and clustering.

Performance-optimized for 1M+ records:
- Tiered blocking: email_domain (most precise) -> phone_suffix -> SOUNDEX (least precise)
- LIMIT 50 per tier to cap candidate set
- Batch cluster lookups (single WHERE IN query)
- In-memory cluster cache (populated by consumer)
"""

from nrt_mdm.matching import compute_match_score, MATCH_THRESHOLD
from nrt_mdm.models import SourceCustomer


# Maximum candidates per blocking tier
CANDIDATE_LIMIT = 50

# ---------------------------------------------------------------------------
# Tiered blocking queries (ordered by precision)
# ---------------------------------------------------------------------------

FIND_BY_EMAIL_DOMAIN_SQL = """
SELECT source_system, source_key, first_name, last_name,
       canonical_first_name, email, phone,
       block_soundex, block_email_domain, block_phone_suffix,
       event_timestamp
FROM source_customers
WHERE block_email_domain = %(block_email_domain)s
  AND (source_system, source_key) != (%(source_system)s, %(source_key)s)
LIMIT 50
"""

FIND_BY_PHONE_SUFFIX_SQL = """
SELECT source_system, source_key, first_name, last_name,
       canonical_first_name, email, phone,
       block_soundex, block_email_domain, block_phone_suffix,
       event_timestamp
FROM source_customers
WHERE block_phone_suffix = %(block_phone_suffix)s
  AND (source_system, source_key) != (%(source_system)s, %(source_key)s)
LIMIT 50
"""

FIND_BY_SOUNDEX_SQL = """
SELECT source_system, source_key, first_name, last_name,
       canonical_first_name, email, phone,
       block_soundex, block_email_domain, block_phone_suffix,
       event_timestamp
FROM source_customers
WHERE block_soundex = %(block_soundex)s
  AND (source_system, source_key) != (%(source_system)s, %(source_key)s)
LIMIT 50
"""


def _rows_to_candidates(rows) -> list[SourceCustomer]:
    """Convert raw rows to SourceCustomer objects."""
    return [SourceCustomer(
        source_system=r[0], source_key=r[1],
        first_name=r[2], last_name=r[3],
        canonical_first_name=r[4], email=r[5], phone=r[6],
        block_soundex=r[7], block_email_domain=r[8],
        block_phone_suffix=r[9], event_timestamp=r[10],
    ) for r in rows]


def find_candidates(conn, record: SourceCustomer) -> list[SourceCustomer]:
    """Find potential matches using tiered blocking with LIMIT.

    Strategy: query the most precise block first (email_domain), then
    phone_suffix, then SOUNDEX. Each tier is limited to 50 results.
    Deduplicates across tiers by (source_system, source_key).
    """
    seen: set[tuple[str, str]] = set()
    candidates: list[SourceCustomer] = []
    params = {
        "source_system": record.source_system,
        "source_key": record.source_key,
        "block_email_domain": record.block_email_domain,
        "block_phone_suffix": record.block_phone_suffix,
        "block_soundex": record.block_soundex,
    }

    with conn.cursor() as cur:
        # Tier 1: Email domain (most precise, smallest blocks)
        if record.block_email_domain:
            cur.execute(FIND_BY_EMAIL_DOMAIN_SQL, params)
            for c in _rows_to_candidates(cur.fetchall()):
                key = (c.source_system, c.source_key)
                if key not in seen:
                    seen.add(key)
                    candidates.append(c)

        # Tier 2: Phone suffix
        if record.block_phone_suffix:
            cur.execute(FIND_BY_PHONE_SUFFIX_SQL, params)
            for c in _rows_to_candidates(cur.fetchall()):
                key = (c.source_system, c.source_key)
                if key not in seen:
                    seen.add(key)
                    candidates.append(c)

        # Tier 3: SOUNDEX (least precise, largest blocks)
        if record.block_soundex and len(candidates) < CANDIDATE_LIMIT:
            cur.execute(FIND_BY_SOUNDEX_SQL, params)
            for c in _rows_to_candidates(cur.fetchall()):
                key = (c.source_system, c.source_key)
                if key not in seen:
                    seen.add(key)
                    candidates.append(c)
                    if len(candidates) >= CANDIDATE_LIMIT * 2:
                        break  # hard cap at 100 total candidates

    return candidates


# ---------------------------------------------------------------------------
# Cluster cache (in-memory, managed by consumer)
# ---------------------------------------------------------------------------

class ClusterCache:
    """In-memory cache for (source_system, source_key) -> cluster_id lookups.

    Eliminates repeated SQL queries for cluster assignments.
    Invalidated on merge operations.
    """

    def __init__(self):
        self._cache: dict[tuple[str, str], int] = {}

    def get(self, source_system: str, source_key: str) -> int | None:
        return self._cache.get((source_system, source_key))

    def put(self, source_system: str, source_key: str, cluster_id: int) -> None:
        self._cache[(source_system, source_key)] = cluster_id

    def invalidate_cluster(self, old_cluster_id: int, new_cluster_id: int) -> None:
        """Update all entries pointing to old_cluster_id to new_cluster_id."""
        for key, cid in list(self._cache.items()):
            if cid == old_cluster_id:
                self._cache[key] = new_cluster_id

    def __len__(self) -> int:
        return len(self._cache)


# Global cache instance (used by consumer)
cluster_cache = ClusterCache()


# ---------------------------------------------------------------------------
# Cluster lookup and management
# ---------------------------------------------------------------------------

GET_CLUSTER_SQL = """
SELECT cluster_id FROM customer_clusters
WHERE source_system = %(source_system)s AND source_key = %(source_key)s
"""

BATCH_GET_CLUSTERS_SQL = """
SELECT source_system, source_key, cluster_id FROM customer_clusters
WHERE (source_system, source_key) = ANY(%(keys)s)
"""

CREATE_CLUSTER_SQL = """
INSERT INTO customer_clusters (source_system, source_key, cluster_id)
VALUES (%(source_system)s, %(source_key)s, nextval('cluster_seq'))
RETURNING cluster_id
"""

ASSIGN_TO_CLUSTER_SQL = """
INSERT INTO customer_clusters (source_system, source_key, cluster_id)
VALUES (%(source_system)s, %(source_key)s, %(cluster_id)s)
ON CONFLICT (source_system, source_key) DO UPDATE SET cluster_id = EXCLUDED.cluster_id
"""

MERGE_CLUSTERS_SQL = """
UPDATE customer_clusters SET cluster_id = %(target_id)s
WHERE cluster_id = %(source_id)s
"""

CLUSTER_SIZE_SQL = """
SELECT COUNT(*) FROM customer_clusters WHERE cluster_id = %(cluster_id)s
"""

UPSERT_XREF_SQL = """
INSERT INTO customer_xref (source_system, source_key, customer_id)
VALUES (%(source_system)s, %(source_key)s, %(customer_id)s)
ON CONFLICT (source_system, source_key) DO UPDATE SET
    customer_id = EXCLUDED.customer_id,
    created_at = NOW()
"""

UPDATE_XREF_FOR_CLUSTER_SQL = """
UPDATE customer_xref SET customer_id = %(new_id)s, created_at = NOW()
WHERE customer_id = %(old_id)s
"""


def get_cluster_id(conn, source_system: str, source_key: str) -> int | None:
    """Get the cluster_id for a source record. Uses cache first, falls back to SQL."""
    # Check cache
    cached = cluster_cache.get(source_system, source_key)
    if cached is not None:
        return cached

    # SQL fallback
    with conn.cursor() as cur:
        cur.execute(GET_CLUSTER_SQL, {"source_system": source_system, "source_key": source_key})
        row = cur.fetchone()
        if row:
            cluster_cache.put(source_system, source_key, row[0])
            return row[0]
    return None


def get_cluster_ids_batch(conn, keys: list[tuple[str, str]]) -> dict[tuple[str, str], int]:
    """Batch lookup cluster_ids. Returns {(ss, sk): cluster_id} for found entries."""
    if not keys:
        return {}

    # Check cache first
    result: dict[tuple[str, str], int] = {}
    uncached: list[tuple[str, str]] = []
    for key in keys:
        cached = cluster_cache.get(key[0], key[1])
        if cached is not None:
            result[key] = cached
        else:
            uncached.append(key)

    # SQL for uncached
    if uncached:
        with conn.cursor() as cur:
            # Use VALUES list for batch lookup (more portable than ANY with composite)
            placeholders = ",".join(["(%s, %s)"] * len(uncached))
            flat_params = [v for k in uncached for v in k]
            cur.execute(f"""
                SELECT cc.source_system, cc.source_key, cc.cluster_id
                FROM customer_clusters cc
                JOIN (VALUES {placeholders}) AS v(ss, sk)
                ON cc.source_system = v.ss AND cc.source_key = v.sk
            """, flat_params)
            for row in cur.fetchall():
                key = (row[0], row[1])
                result[key] = row[2]
                cluster_cache.put(row[0], row[1], row[2])

    return result


def create_new_cluster(conn, source_system: str, source_key: str) -> int:
    """Create a new cluster for a record that has no matches."""
    with conn.cursor() as cur:
        cur.execute(CREATE_CLUSTER_SQL, {"source_system": source_system, "source_key": source_key})
        cluster_id = cur.fetchone()[0]
        cur.execute(UPSERT_XREF_SQL, {"source_system": source_system, "source_key": source_key, "customer_id": cluster_id})
    cluster_cache.put(source_system, source_key, cluster_id)
    return cluster_id


def assign_to_cluster(conn, source_system: str, source_key: str, cluster_id: int) -> None:
    """Assign a source record to an existing cluster."""
    with conn.cursor() as cur:
        cur.execute(ASSIGN_TO_CLUSTER_SQL, {"source_system": source_system, "source_key": source_key, "cluster_id": cluster_id})
        cur.execute(UPSERT_XREF_SQL, {"source_system": source_system, "source_key": source_key, "customer_id": cluster_id})
    cluster_cache.put(source_system, source_key, cluster_id)


def merge_clusters(conn, cluster_a: int, cluster_b: int) -> int:
    """Merge two clusters. Smaller cluster is absorbed into larger. Returns surviving cluster_id."""
    with conn.cursor() as cur:
        cur.execute(CLUSTER_SIZE_SQL, {"cluster_id": cluster_a})
        size_a = cur.fetchone()[0]
        cur.execute(CLUSTER_SIZE_SQL, {"cluster_id": cluster_b})
        size_b = cur.fetchone()[0]

    if size_a >= size_b:
        target, source = cluster_a, cluster_b
    else:
        target, source = cluster_b, cluster_a

    with conn.cursor() as cur:
        cur.execute(MERGE_CLUSTERS_SQL, {"target_id": target, "source_id": source})
        cur.execute(UPDATE_XREF_FOR_CLUSTER_SQL, {"new_id": target, "old_id": source})

    # Invalidate cache entries for merged cluster
    cluster_cache.invalidate_cluster(source, target)
    return target


# ---------------------------------------------------------------------------
# Resolution orchestration
# ---------------------------------------------------------------------------

def resolve(conn, record: SourceCustomer) -> tuple[int, bool]:
    """Resolve a record: find matches, assign/merge clusters.

    Returns (cluster_id, cluster_changed) where cluster_changed indicates
    whether a new cluster was created or an existing one was modified.
    """
    candidates = find_candidates(conn, record)

    # Score candidates and collect matching ones
    matched_candidates = [
        c for c in candidates
        if compute_match_score(record, c) >= MATCH_THRESHOLD
    ]

    # Batch lookup cluster_ids for all matched candidates
    if matched_candidates:
        keys = [(c.source_system, c.source_key) for c in matched_candidates]
        cluster_map = get_cluster_ids_batch(conn, keys)
        matched_clusters = set(cluster_map.values())
    else:
        matched_clusters = set()

    # Get current cluster of the incoming record (if it already exists)
    current_cluster = get_cluster_id(conn, record.source_system, record.source_key)
    if current_cluster is not None:
        matched_clusters.add(current_cluster)

    if not matched_clusters:
        # No matches found -- create new cluster
        cluster_id = create_new_cluster(conn, record.source_system, record.source_key)
        return cluster_id, True

    # Assign record to one of the matched clusters
    cluster_list = sorted(matched_clusters)
    primary_cluster = cluster_list[0]

    # Assign the record to the primary cluster
    assign_to_cluster(conn, record.source_system, record.source_key, primary_cluster)

    # Merge all other matched clusters into the primary
    cluster_changed = current_cluster is None or current_cluster != primary_cluster
    for other_cluster in cluster_list[1:]:
        if other_cluster != primary_cluster:
            merge_clusters(conn, primary_cluster, other_cluster)
            cluster_changed = True

    return primary_cluster, cluster_changed
