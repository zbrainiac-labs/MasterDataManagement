"""Survivorship engine: computes the golden record for a cluster.

Picks the best value per attribute using:
1. Completeness (non-null, valid format)
2. Source priority (CRM_A=1, CRM_B=2, CRM_C=3)
3. Recency (event_timestamp DESC)
"""

from nrt_mdm.models import GoldenCustomer

SOURCE_PRIORITY = {"CRM_A": 1, "CRM_B": 2, "CRM_C": 3}

GET_CLUSTER_RECORDS_SQL = """
SELECT sc.source_system, sc.source_key, sc.first_name, sc.last_name,
       sc.email, sc.phone, sc.event_timestamp
FROM source_customers sc
JOIN customer_clusters cc ON sc.source_system = cc.source_system AND sc.source_key = cc.source_key
WHERE cc.cluster_id = %(cluster_id)s
ORDER BY sc.event_timestamp DESC
"""


def _pick_best(records: list[dict], field: str, validity_fn) -> str | None:
    """Pick the best value for a field using survivorship rules.

    Order: completeness (valid first) -> source priority -> recency (already sorted DESC).
    """
    valid = [r for r in records if validity_fn(r.get(field))]
    if valid:
        # Sort by source priority (ascending = higher trust first)
        valid.sort(key=lambda r: SOURCE_PRIORITY.get(r["source_system"], 99))
        return valid[0][field]
    # Fallback: any non-null value
    for r in records:
        if r.get(field):
            return r[field]
    return None


def _is_valid_name(val: str | None) -> bool:
    return val is not None and len(val.strip()) > 1


def _is_valid_email(val: str | None) -> bool:
    return val is not None and "@" in val


def _is_valid_phone(val: str | None) -> bool:
    if not val:
        return False
    import re
    digits = re.sub(r"\D", "", val, flags=re.ASCII)
    return len(digits) >= 7


def compute_golden(conn, cluster_id: int) -> GoldenCustomer | None:
    """Compute the golden record for a cluster by applying survivorship rules.

    Returns None if the cluster has no source records.
    """
    with conn.cursor() as cur:
        cur.execute(GET_CLUSTER_RECORDS_SQL, {"cluster_id": cluster_id})
        rows = cur.fetchall()

    if not rows:
        return None

    records = []
    source_systems = set()
    for row in rows:
        records.append({
            "source_system": row[0],
            "source_key": row[1],
            "first_name": row[2],
            "last_name": row[3],
            "email": row[4],
            "phone": row[5],
            "event_timestamp": row[6],
        })
        source_systems.add(row[0])

    # Records are already sorted by event_timestamp DESC from SQL
    first_name = _pick_best(records, "first_name", _is_valid_name)
    last_name = _pick_best(records, "last_name", _is_valid_name)
    email = _pick_best(records, "email", _is_valid_email)
    phone = _pick_best(records, "phone", _is_valid_phone)

    return GoldenCustomer(
        cluster_id=cluster_id,
        first_name=first_name,
        last_name=last_name,
        email=email,
        phone=phone,
        dq_score=0,  # Will be computed by dq.py
        source_count=len(source_systems),
    )
