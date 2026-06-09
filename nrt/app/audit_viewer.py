"""Streamlit Audit Log Viewer — live tail of audit_events table (SEC-04)."""

import os
import time

import pandas as pd
import psycopg
import streamlit as st

POSTGRES_DSN = os.environ.get("POSTGRES_DSN", "postgresql://mdm:mdm@localhost:5432/mdm")

st.set_page_config(page_title="MDM Audit Log", page_icon="📋", layout="wide")
st.title("MDM Audit Log")

# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------

col1, col2, col3, col4 = st.columns(4)

with col1:
    event_type_filter = st.selectbox(
        "Event Type",
        ["ALL", "INGEST", "GOLDEN_CHANGE", "BATCH_RESOLVE", "READ", "ADMIN"],
    )

with col2:
    source_filter = st.selectbox(
        "Source System",
        ["ALL", "CRM_A", "CRM_B", "CRM_C"],
    )

with col3:
    action_filter = st.selectbox(
        "Action",
        ["ALL", "INSERT", "UPDATE", "NO_CHANGE", "SKIPPED", "MERGE", "READ", "TRUNCATE_AND_REBUILD"],
    )

with col4:
    limit = st.slider("Rows", min_value=10, max_value=500, value=50, step=10)

# Auto-refresh toggle
auto_refresh = st.toggle("Auto-refresh (2s)", value=True)

# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------


@st.cache_resource
def get_connection():
    return psycopg.connect(POSTGRES_DSN, autocommit=True)


def load_audit_events():
    conn = get_connection()
    conditions = []
    params = {}

    if event_type_filter != "ALL":
        conditions.append("event_type = %(event_type)s")
        params["event_type"] = event_type_filter

    if source_filter != "ALL":
        conditions.append("source_system = %(source_system)s")
        params["source_system"] = source_filter

    if action_filter != "ALL":
        conditions.append("action = %(action)s")
        params["action"] = action_filter

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    query = f"""
        SELECT event_id, event_type, actor, source_system, source_key,
               cluster_id, action, detail, created_at
        FROM audit_events
        {where_clause}
        ORDER BY created_at DESC
        LIMIT %(limit)s
    """
    params["limit"] = limit

    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
            columns = ["event_id", "event_type", "actor", "source_system", "source_key",
                       "cluster_id", "action", "detail", "created_at"]
            return pd.DataFrame(rows, columns=columns)
    except Exception as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Display
# ---------------------------------------------------------------------------

df = load_audit_events()

if df.empty:
    st.info("No audit events found. Run the pipeline to generate events.")
else:
    # Summary metrics
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Events", len(df))
    m2.metric("Ingests", len(df[df["event_type"] == "INGEST"]))
    m3.metric("Reads", len(df[df["event_type"] == "READ"]))
    m4.metric("Batch Ops", len(df[df["event_type"] == "BATCH_RESOLVE"]))

    # Format for display
    display_df = df.copy()
    display_df["created_at"] = pd.to_datetime(display_df["created_at"]).dt.strftime("%H:%M:%S.%f").str[:-3]
    display_df["event_id"] = display_df["event_id"].astype(str).str[:8] + "..."
    display_df["detail"] = display_df["detail"].apply(
        lambda x: str(x)[:80] + "..." if x and len(str(x)) > 80 else str(x) if x else ""
    )

    st.dataframe(display_df, width=2000, height=500)

    # Expandable detail view
    st.subheader("Event Detail")
    if not df.empty:
        selected_idx = st.selectbox(
            "Select event to inspect",
            range(len(df)),
            format_func=lambda i: f"{df.iloc[i]['created_at']} | {df.iloc[i]['event_type']} | {df.iloc[i]['action']} | {df.iloc[i]['source_key'] or '-'}",
        )
        if selected_idx is not None:
            event = df.iloc[selected_idx]
            st.json({
                "event_id": str(event["event_id"]),
                "event_type": event["event_type"],
                "actor": event["actor"],
                "source_system": event["source_system"],
                "source_key": event["source_key"],
                "cluster_id": int(event["cluster_id"]) if pd.notna(event["cluster_id"]) else None,
                "action": event["action"],
                "detail": event["detail"],
                "created_at": str(event["created_at"]),
            })

# Auto-refresh
if auto_refresh:
    time.sleep(2)
    st.rerun()
