"""BIZ-15: Cluster Split / Unmatch with CDC fix-forward.

Splits specified source records out of a cluster into a new cluster,
recomputes golden records for both, and publishes CDC events.
"""

import logging

from nrt_mdm.audit import log_audit, publish_audit_async
from nrt_mdm.dq import compute_dq_score
from nrt_mdm.producer import publish_golden_if_changed, publish_xref_change, compute_row_hash
from nrt_mdm.resolver import cluster_cache
from nrt_mdm.survivorship import compute_golden

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

GET_CLUSTER_MEMBERS_SQL = """
    SELECT source_system, source_key
    FROM customer_clusters
    WHERE cluster_id = %(cluster_id)s
"""

REASSIGN_TO_NEW_CLUSTER_SQL = """
    UPDATE customer_clusters
    SET cluster_id = %(new_cluster_id)s
    WHERE source_system = %(source_system)s AND source_key = %(source_key)s
"""

REASSIGN_XREF_SQL = """
    UPDATE customer_xref
    SET customer_id = %(new_cluster_id)s, created_at = NOW()
    WHERE source_system = %(source_system)s AND source_key = %(source_key)s
"""

CREATE_NEW_CLUSTER_ID_SQL = "SELECT nextval('cluster_seq')"

INSERT_SUPPRESSION_SQL = """
    INSERT INTO match_suppressions (source_system_a, source_key_a, source_system_b, source_key_b, reason, created_by)
    VALUES (%(sys_a)s, %(key_a)s, %(sys_b)s, %(key_b)s, %(reason)s, %(actor)s)
    ON CONFLICT (source_system_a, source_key_a, source_system_b, source_key_b) DO NOTHING
"""


# ---------------------------------------------------------------------------
# Core unmatch function
# ---------------------------------------------------------------------------

def unmatch_records(
    pg_conn,
    producer,
    cluster_id: int,
    records_to_split: list[dict],
    reason: str,
    actor: str = "admin",
) -> dict:
    """Split records out of a cluster into a new cluster with CDC fix-forward.

    Args:
        pg_conn: Postgres connection (autocommit=False)
        producer: Kafka producer (or None for no CDC publish)
        cluster_id: The cluster to split from
        records_to_split: List of {"source_system": "CRM_B", "source_key": "B456"}
        reason: Human-readable reason for the split
        actor: Who triggered this (for audit)

    Returns:
        dict with results: original_cluster, new_cluster, split_records, suppressions_created
    """
    # 1. Get all members of the cluster
    with pg_conn.cursor() as cur:
        cur.execute(GET_CLUSTER_MEMBERS_SQL, {"cluster_id": cluster_id})
        all_members = [(row[0], row[1]) for row in cur.fetchall()]

    if not all_members:
        raise ValueError(f"Cluster {cluster_id} not found or empty")

    # 2. Validate split records exist in the cluster
    split_set = {(r["source_system"], r["source_key"]) for r in records_to_split}
    member_set = set(all_members)

    missing = split_set - member_set
    if missing:
        raise ValueError(f"Records not in cluster {cluster_id}: {missing}")

    remaining_set = member_set - split_set
    if not remaining_set:
        raise ValueError(f"Cannot split ALL records out of cluster {cluster_id}. At least one must remain.")

    # 3. Create new cluster_id
    with pg_conn.cursor() as cur:
        cur.execute(CREATE_NEW_CLUSTER_ID_SQL)
        new_cluster_id = cur.fetchone()[0]

    # 4. Reassign split records to new cluster + invalidate cache
    with pg_conn.cursor() as cur:
        for sys, key in split_set:
            cur.execute(REASSIGN_TO_NEW_CLUSTER_SQL, {
                "new_cluster_id": new_cluster_id,
                "source_system": sys,
                "source_key": key,
            })
            cur.execute(REASSIGN_XREF_SQL, {
                "new_cluster_id": new_cluster_id,
                "source_system": sys,
                "source_key": key,
            })
            # Invalidate cluster cache for reassigned record
            cluster_cache.put(sys, key, new_cluster_id)

    # 5. Create suppressions (each split record x each remaining record)
    suppressions_created = 0
    with pg_conn.cursor() as cur:
        for sys_split, key_split in split_set:
            for sys_remain, key_remain in remaining_set:
                # Normalize order (alphabetical) for consistent uniqueness
                a = (sys_split, key_split)
                b = (sys_remain, key_remain)
                if a > b:
                    a, b = b, a
                cur.execute(INSERT_SUPPRESSION_SQL, {
                    "sys_a": a[0], "key_a": a[1],
                    "sys_b": b[0], "key_b": b[1],
                    "reason": reason,
                    "actor": actor,
                })
                suppressions_created += 1

    # 6. Recompute golden for original cluster (fewer sources now)
    golden_original = compute_golden(pg_conn, cluster_id)
    if golden_original:
        golden_original.dq_score = compute_dq_score(golden_original)
        golden_original.row_hash = compute_row_hash(golden_original)
        publish_golden_if_changed(producer, pg_conn, golden_original)

    # 7. Recompute golden for new cluster
    golden_new = compute_golden(pg_conn, new_cluster_id)
    if golden_new:
        golden_new.dq_score = compute_dq_score(golden_new)
        golden_new.row_hash = compute_row_hash(golden_new)
        publish_golden_if_changed(producer, pg_conn, golden_new)

    # 8. Audit
    audit_rec = log_audit(
        pg_conn,
        event_type="ADMIN",
        actor=actor,
        cluster_id=cluster_id,
        action="UNMATCH",
        detail={
            "original_cluster_id": cluster_id,
            "new_cluster_id": new_cluster_id,
            "split_records": records_to_split,
            "remaining_count": len(remaining_set),
            "suppressions_created": suppressions_created,
            "reason": reason,
        },
    )

    # 9. Commit
    pg_conn.commit()

    # 10. Publish XREF REASSIGN events (after commit)
    for sys, key in split_set:
        if producer:
            publish_xref_change(producer, sys, key, new_cluster_id, previous_customer_id=cluster_id)

    publish_audit_async(producer, audit_rec)

    # Build response
    result = {
        "success": True,
        "original_cluster_id": cluster_id,
        "new_cluster_id": new_cluster_id,
        "split_records": records_to_split,
        "remaining_records": [{"source_system": s, "source_key": k} for s, k in remaining_set],
        "suppressions_created": suppressions_created,
        "original_golden": _golden_summary(golden_original) if golden_original else None,
        "new_golden": _golden_summary(golden_new) if golden_new else None,
    }

    logger.info(
        "Unmatch complete: cluster %d split -> %d (moved %d records, %d suppressions)",
        cluster_id, new_cluster_id, len(split_set), suppressions_created,
    )

    return result


def _golden_summary(golden) -> dict:
    """Summarize a golden record for API response."""
    return {
        "cluster_id": golden.cluster_id,
        "first_name": golden.first_name,
        "last_name": golden.last_name,
        "email": golden.email,
        "phone": golden.phone,
        "dq_score": golden.dq_score,
        "source_count": golden.source_count,
    }
