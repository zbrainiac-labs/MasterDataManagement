"""NRT MDM Golden Record Viewer — Single-page Streamlit app.

Browse and inspect golden records, source records, SCD2 history, and XREF
from the NRT MDM Postgres database.

Run:  streamlit run nrt/app/streamlit_app.py
"""

import os

import pandas as pd
import psycopg
import streamlit as st

POSTGRES_DSN = os.environ.get("POSTGRES_DSN", "postgresql://mdm:mdm@localhost:5432/mdm")


@st.cache_resource
def get_conn():
    return psycopg.connect(POSTGRES_DSN, autocommit=True)


def search_clusters(conn, query: str) -> pd.DataFrame:
    """Search golden records by customer_id, name, or email."""
    sql = """
        SELECT gc.cluster_id AS customer_id, gc.first_name, gc.last_name,
               gc.email, gc.phone, gc.dq_score, gc.source_count
        FROM golden_customers gc
        WHERE gc.is_current = TRUE
          AND (
              gc.cluster_id::text = %(q)s
              OR gc.first_name ILIKE %(like)s
              OR gc.last_name ILIKE %(like)s
              OR gc.email ILIKE %(like)s
          )
        ORDER BY gc.cluster_id
        LIMIT 20
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"q": query.strip(), "like": f"%{query.strip()}%"})
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def get_golden_record(conn, cluster_id: int) -> pd.DataFrame:
    sql = """
        SELECT cluster_id AS customer_id, first_name, last_name, email, phone,
               dq_score, source_count
        FROM golden_customers
        WHERE cluster_id = %s AND is_current = TRUE
    """
    with conn.cursor() as cur:
        cur.execute(sql, (cluster_id,))
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def get_source_records(conn, cluster_id: int) -> pd.DataFrame:
    sql = """
        SELECT cc.cluster_id AS customer_id, sc.source_system, sc.source_key,
               sc.first_name, sc.last_name, sc.email, sc.phone,
               sc.canonical_first_name, sc.block_soundex,
               sc.block_email_domain, sc.block_phone_suffix, sc.event_timestamp
        FROM source_customers sc
        JOIN customer_clusters cc ON sc.source_system = cc.source_system AND sc.source_key = cc.source_key
        WHERE cc.cluster_id = %s
        ORDER BY sc.source_system, sc.source_key
    """
    with conn.cursor() as cur:
        cur.execute(sql, (cluster_id,))
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def get_history(conn, cluster_id: int) -> pd.DataFrame:
    sql = """
        SELECT id, first_name, last_name, email, phone, dq_score, source_count,
               valid_from, valid_to, is_current
        FROM golden_customers
        WHERE cluster_id = %s
        ORDER BY valid_from DESC
    """
    with conn.cursor() as cur:
        cur.execute(sql, (cluster_id,))
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def get_xref(conn, cluster_id: int) -> pd.DataFrame:
    sql = """
        SELECT source_system, source_key, customer_id, created_at
        FROM customer_xref
        WHERE customer_id = %s
        ORDER BY source_system, source_key
    """
    with conn.cursor() as cur:
        cur.execute(sql, (cluster_id,))
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def get_stats(conn) -> dict:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM source_customers")
        sources = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM golden_customers WHERE is_current = TRUE")
        golden = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT cluster_id) FROM customer_clusters")
        clusters = cur.fetchone()[0]
    return {"source_records": sources, "golden_records": golden, "clusters": clusters}


# =============================================================================
# UI
# =============================================================================

st.set_page_config(page_title="NRT MDM Viewer", page_icon=":", layout="wide")
st.title("NRT MDM Golden Record Viewer")

conn = get_conn()

# Sidebar: stats
stats = get_stats(conn)
st.sidebar.metric("Source Records", stats["source_records"])
st.sidebar.metric("Golden Records", stats["golden_records"])
st.sidebar.metric("Clusters", stats["clusters"])

# Search
query = st.text_input("Search by customer_id, name, or email", placeholder="e.g. 42 or Smith or bill@acme.com")

if query:
    results = search_clusters(conn, query)
    if results.empty:
        st.warning("No results found.")
    else:
        st.subheader(f"Search Results ({len(results)} matches)")
        st.dataframe(results, width="stretch", hide_index=True)

        # Select a customer to drill into
        selected_id = st.selectbox(
            "Select customer_id to inspect",
            results["customer_id"].tolist(),
        )

        if selected_id:
            st.divider()

            # Golden Record
            st.subheader(f"Golden Record (customer_id={selected_id})")
            golden = get_golden_record(conn, selected_id)
            if not golden.empty:
                cols = st.columns(6)
                row = golden.iloc[0]
                cols[0].metric("First Name", row["first_name"] or "-")
                cols[1].metric("Last Name", row["last_name"] or "-")
                cols[2].metric("Email", row["email"] or "-")
                cols[3].metric("Phone", row["phone"] or "-")
                cols[4].metric("DQ Score", row["dq_score"])
                cols[5].metric("Sources", row["source_count"])

            # Source Records
            st.subheader("Source Records")
            sources = get_source_records(conn, selected_id)
            if sources.empty:
                st.info("No source records in this cluster.")
            else:
                st.dataframe(sources, width="stretch", hide_index=True)

            # XREF
            st.subheader("Cross-References (XREF)")
            st.info(
                "**What is XREF?** The cross-reference table links every source system record "
                "(e.g. CRM_A|A000123) to a single golden record ID (`customer_id`). "
                "It answers: *'Which source records were merged into this golden record?'*\n\n"
                f"**customer_id = {selected_id}** is the golden record ID (cluster_id) shown above. "
                "All source keys listed below were resolved as the same real-world person."
            )
            xref = get_xref(conn, selected_id)
            if xref.empty:
                st.info("No XREF entries.")
            else:
                st.dataframe(xref, width="stretch", hide_index=True)

            # SCD2 History
            st.subheader("SCD2 History")
            history = get_history(conn, selected_id)
            if history.empty:
                st.info("No history.")
            else:
                st.dataframe(history, width="stretch", hide_index=True)

            # ----- Admin: Unmatch (BIZ-15) -----
            if sources is not None and len(sources) > 1:
                st.divider()
                st.subheader("Admin: Unmatch / Split Cluster")
                st.caption(
                    "Select source records that were incorrectly merged into this cluster. "
                    "They will be moved to a new cluster with their own golden record."
                )

                # Checkboxes for each source record
                split_selections = []
                for i, row in sources.iterrows():
                    label = f"{row['source_system']}|{row['source_key']} ({row['first_name']} {row['last_name']}, {row['email']})"
                    if st.checkbox(label, key=f"unmatch_{i}"):
                        split_selections.append({
                            "source_system": row["source_system"],
                            "source_key": row["source_key"],
                        })

                reason = st.text_input("Reason for unmatch (required)", placeholder="e.g. False positive: father/son share phone")

                if st.button("Unmatch Selected", type="primary", disabled=not split_selections or not reason):
                    if len(split_selections) >= len(sources):
                        st.error("Cannot split ALL records out. At least one must remain in the original cluster.")
                    else:
                        import httpx
                        api_url = os.environ.get("MDM_API_URL", "http://localhost:8000")
                        try:
                            resp = httpx.post(
                                f"{api_url}/api/v1/admin/unmatch",
                                json={
                                    "cluster_id": selected_id,
                                    "source_records_to_split": split_selections,
                                    "reason": reason,
                                },
                                timeout=30.0,
                            )
                            if resp.status_code == 200:
                                result = resp.json()
                                st.success(
                                    f"Unmatch complete. New cluster: **{result['new_cluster_id']}**. "
                                    f"Suppressions created: {result['suppressions_created']}."
                                )
                                st.json(result)
                            else:
                                st.error(f"Error: {resp.json().get('detail', resp.text)}")
                        except Exception as e:
                            st.error(f"Request failed: {e}")
