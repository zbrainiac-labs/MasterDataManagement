import streamlit as st
import pandas as pd
from datetime import timedelta

st.set_page_config(
    page_title="Customer 360 - MDM Dashboard",
    page_icon=":busts_in_silhouette:",
    layout="wide",
)

conn = st.connection("snowflake")

@st.cache_data(ttl=timedelta(minutes=5))
def load_customers():
    return conn.query("""
        SELECT customer_id, first_name, last_name,
               first_name || ' ' || last_name AS full_name,
               email, phone, dq_score, source_count, last_updated,
               CASE WHEN dq_score >= 90 THEN 'Excellent'
                    WHEN dq_score >= 70 THEN 'Good'
                    WHEN dq_score >= 50 THEN 'Fair'
                    ELSE 'Poor' END AS dq_tier
        FROM MDM_DEV.MDM_AGG_001.CRMA_AGG_DT_CUSTOMER
        ORDER BY customer_id
    """)

@st.cache_data(ttl=timedelta(minutes=5))
def load_addresses():
    return conn.query("""
        SELECT address_id, customer_id, address_type, street, city,
               postal_code, country, is_primary
        FROM MDM_DEV.MDM_AGG_001.CRMA_AGG_DT_ADDRESSES
        ORDER BY address_id
    """)

@st.cache_data(ttl=timedelta(minutes=5))
def load_customer_xref():
    return conn.query("""
        SELECT customer_id, source_system, source_key
        FROM MDM_DEV.MDM_AGG_001.CRMA_AGG_VW_CUSTOMER_XREF
    """)

@st.cache_data(ttl=timedelta(minutes=5))
def load_address_xref():
    return conn.query("""
        SELECT xref_id, address_id, source_system, source_key
        FROM MDM_DEV.MDM_AGG_001.CRMA_AGG_VW_ADDRESSES_XREF
    """)

@st.cache_data(ttl=timedelta(minutes=5))
def load_customer_xref_for_id(customer_id: int):
    return conn.query(f"""
        SELECT customer_id, source_system, source_key
        FROM MDM_DEV.MDM_AGG_001.CRMA_AGG_VW_CUSTOMER_XREF
        WHERE customer_id = {customer_id}
    """)

@st.cache_data(ttl=timedelta(minutes=5))
def load_customer_history():
    return conn.query("""
        SELECT customer_id, first_name, last_name, email, phone,
               dq_score, valid_from,
               CASE WHEN valid_to >= '2099-01-01' THEN NULL ELSE valid_to END AS valid_to,
               is_valid
        FROM MDM_DEV.MDM_AGG_001.CRMA_AGG_DT_CUSTOMER_HISTORY
        ORDER BY customer_id, valid_from
    """)

@st.cache_data(ttl=timedelta(minutes=5))
def load_address_history():
    return conn.query("""
        SELECT address_id, customer_id, address_type, street, city,
               postal_code, country, is_primary, valid_from,
               CASE WHEN valid_to >= '2099-01-01' THEN NULL ELSE valid_to END AS valid_to,
               is_valid
        FROM MDM_DEV.MDM_AGG_001.CRMA_AGG_DT_ADDRESSES_HISTORY
        ORDER BY address_id, valid_from
    """)

@st.cache_data(ttl=timedelta(minutes=5))
def load_customer_dq_distribution():
    return conn.query("""
        SELECT
            CASE WHEN dq_score >= 90 THEN 'Excellent'
                 WHEN dq_score >= 70 THEN 'Good'
                 WHEN dq_score >= 50 THEN 'Fair'
                 ELSE 'Poor' END AS dq_tier,
            COUNT(*) AS record_count,
            ROUND(AVG(dq_score), 1) AS avg_score
        FROM MDM_DEV.MDM_AGG_001.CRMA_AGG_DT_CUSTOMER
        GROUP BY dq_tier
        ORDER BY avg_score DESC
    """)

@st.cache_data(ttl=timedelta(minutes=5))
def load_customer_er_stats():
    return conn.query("""
        SELECT
            COUNT(DISTINCT customer_id) AS total_golden,
            SUM(CASE WHEN source_count >= 2 THEN 1 ELSE 0 END) AS merged_records,
            SUM(CASE WHEN source_count = 1 THEN 1 ELSE 0 END) AS unique_records,
            ROUND(AVG(dq_score), 1) AS avg_dq_score
        FROM MDM_DEV.MDM_AGG_001.CRMA_AGG_DT_CUSTOMER
    """)

@st.cache_data(ttl=timedelta(minutes=5))
def load_address_stats():
    return conn.query("""
        SELECT
            COUNT(*) AS total_addresses,
            COUNT(DISTINCT customer_id) AS customers_with_address,
            COUNT(DISTINCT country) AS country_count
        FROM MDM_DEV.MDM_AGG_001.CRMA_AGG_DT_ADDRESSES
    """)

@st.cache_data(ttl=timedelta(minutes=5))
def load_address_country_dist():
    return conn.query("""
        SELECT country, COUNT(*) AS address_count
        FROM MDM_DEV.MDM_AGG_001.CRMA_AGG_DT_ADDRESSES
        GROUP BY country
        ORDER BY address_count DESC
    """)

@st.cache_data(ttl=timedelta(minutes=5))
def load_customer_source_counts():
    return conn.query("""
        SELECT source_system, COUNT(*) AS record_count
        FROM MDM_DEV.MDM_AGG_001.CRMA_AGG_VW_CUSTOMER_UNION
        GROUP BY source_system ORDER BY source_system
    """)

@st.cache_data(ttl=timedelta(minutes=5))
def load_address_source_counts():
    return conn.query("""
        SELECT source_system, COUNT(*) AS record_count
        FROM MDM_DEV.MDM_AGG_001.CRMA_AGG_VW_ADDRESSES_UNION
        GROUP BY source_system ORDER BY source_system
    """)

customers_df = load_customers()
addresses_df = load_addresses()
cust_er_stats = load_customer_er_stats()
addr_stats = load_address_stats()
cust_dq_dist = load_customer_dq_distribution()
country_dist = load_address_country_dist()
cust_source_counts = load_customer_source_counts()
addr_source_counts = load_address_source_counts()

st.title(":busts_in_silhouette: Customer 360 — MDM Dashboard")

tab_overview, tab_search, tab_dq, tab_er, tab_history = st.tabs(
    [":bar_chart: Overview", ":mag: Customer Search", ":white_check_mark: Data Quality",
     ":link: Entity Resolution", ":clock3: SCD History"]
)

# --- OVERVIEW ---
with tab_overview:
    total_cust = int(cust_er_stats["TOTAL_GOLDEN"].iloc[0]) if len(cust_er_stats) and pd.notna(cust_er_stats["TOTAL_GOLDEN"].iloc[0]) else 0
    merged_cust = int(cust_er_stats["MERGED_RECORDS"].iloc[0]) if len(cust_er_stats) and pd.notna(cust_er_stats["MERGED_RECORDS"].iloc[0]) else 0
    unique_cust = int(cust_er_stats["UNIQUE_RECORDS"].iloc[0]) if len(cust_er_stats) and pd.notna(cust_er_stats["UNIQUE_RECORDS"].iloc[0]) else 0
    avg_dq = float(cust_er_stats["AVG_DQ_SCORE"].iloc[0]) if len(cust_er_stats) and pd.notna(cust_er_stats["AVG_DQ_SCORE"].iloc[0]) else 0
    total_addr = int(addr_stats["TOTAL_ADDRESSES"].iloc[0]) if len(addr_stats) and pd.notna(addr_stats["TOTAL_ADDRESSES"].iloc[0]) else 0
    cust_with_addr = int(addr_stats["CUSTOMERS_WITH_ADDRESS"].iloc[0]) if len(addr_stats) and pd.notna(addr_stats["CUSTOMERS_WITH_ADDRESS"].iloc[0]) else 0
    n_countries = int(addr_stats["COUNTRY_COUNT"].iloc[0]) if len(addr_stats) and pd.notna(addr_stats["COUNTRY_COUNT"].iloc[0]) else 0
    addr_coverage = round(cust_with_addr / total_cust * 100, 1) if total_cust else 0

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader(":bust_in_silhouette: Customers")
        with st.container(horizontal=True):
            st.metric("Golden Records", f"{total_cust:,}", border=True)
            st.metric("Merged (2+ sources)", f"{merged_cust:,}", border=True)
        with st.container(horizontal=True):
            st.metric("Unique (1 source)", f"{unique_cust:,}", border=True)
            st.metric("Avg DQ Score", f"{avg_dq:.1f}", border=True)
        with st.container(border=True):
            st.subheader("DQ Tier Distribution")
            if len(cust_dq_dist):
                st.bar_chart(cust_dq_dist, x="DQ_TIER", y="RECORD_COUNT")
        with st.container(border=True):
            st.subheader("Source Contribution")
            if len(cust_source_counts):
                st.bar_chart(cust_source_counts, x="SOURCE_SYSTEM", y="RECORD_COUNT")

    with col_right:
        st.subheader(":round_pushpin: Addresses")
        with st.container(horizontal=True):
            st.metric("Golden Addresses", f"{total_addr:,}", border=True)
            st.metric("Customers with Address", f"{cust_with_addr:,}", border=True)
        with st.container(horizontal=True):
            st.metric("Countries", f"{n_countries:,}", border=True)
            st.metric("Address Coverage", f"{addr_coverage}%", border=True)
        with st.container(border=True):
            st.subheader("Addresses by Country")
            if len(country_dist):
                st.bar_chart(country_dist, x="COUNTRY", y="ADDRESS_COUNT")
        with st.container(border=True):
            st.subheader("Source Contribution")
            if len(addr_source_counts):
                st.bar_chart(addr_source_counts, x="SOURCE_SYSTEM", y="RECORD_COUNT")

    with st.container(border=True):
        st.subheader("Top 20 Golden Records (Customers + Addresses)")
        top20 = customers_df.head(20).merge(
            addresses_df[["CUSTOMER_ID", "STREET", "CITY", "COUNTRY"]],
            on="CUSTOMER_ID", how="left",
        )
        st.dataframe(
            top20[["CUSTOMER_ID", "FULL_NAME", "EMAIL", "PHONE", "DQ_SCORE",
                   "DQ_TIER", "SOURCE_COUNT", "STREET", "CITY", "COUNTRY"]],
            hide_index=True, width="stretch",
        )

# --- CUSTOMER SEARCH ---
with tab_search:
    st.subheader("Customer & Address Lookup")
    search_col1, search_col2 = st.columns(2)
    with search_col1:
        search_term = st.text_input("Search by name, email, ID, city, or street")
    with search_col2:
        dq_filter = st.multiselect(
            "DQ Tier Filter",
            ["Excellent", "Good", "Fair", "Poor"],
            default=["Excellent", "Good", "Fair", "Poor"],
        )

    filtered = customers_df[customers_df["DQ_TIER"].isin(dq_filter)]
    if search_term:
        addr_match_ids = addresses_df[
            addresses_df["CITY"].str.contains(search_term, case=False, na=False)
            | addresses_df["STREET"].str.contains(search_term, case=False, na=False)
            | addresses_df["COUNTRY"].str.contains(search_term, case=False, na=False)
        ]["CUSTOMER_ID"].unique()
        mask = (
            filtered["FULL_NAME"].str.contains(search_term, case=False, na=False)
            | filtered["EMAIL"].str.contains(search_term, case=False, na=False)
            | filtered["CUSTOMER_ID"].astype(str).str.contains(search_term, na=False)
            | filtered["CUSTOMER_ID"].isin(addr_match_ids)
        )
        filtered = filtered[mask]

    st.write(f"Showing {len(filtered)} of {len(customers_df)} customers")
    st.dataframe(filtered, hide_index=True, width="stretch")

    if len(filtered) > 0:
        selected_id = st.selectbox(
            "Select a customer for detail view",
            filtered["CUSTOMER_ID"].tolist(),
            format_func=lambda x: f"#{x} — {filtered[filtered['CUSTOMER_ID']==x]['FULL_NAME'].iloc[0]}",
        )
        if selected_id:
            cust = filtered[filtered["CUSTOMER_ID"] == selected_id].iloc[0]
            addr = addresses_df[addresses_df["CUSTOMER_ID"] == selected_id]
            c_xref = load_customer_xref_for_id(selected_id)
            a_xref = pd.DataFrame()

            st.divider()
            detail_col1, detail_col2 = st.columns(2)
            with detail_col1:
                with st.container(border=True):
                    st.subheader(f":bust_in_silhouette: {cust['FULL_NAME']}")
                    st.write(f"**Email:** {cust['EMAIL']}")
                    st.write(f"**Phone:** {cust['PHONE']}")
                    st.write(f"**DQ Score:** {cust['DQ_SCORE']} ({cust['DQ_TIER']})")
                    st.write(f"**Sources:** {cust['SOURCE_COUNT']}")
            with detail_col2:
                with st.container(border=True):
                    st.subheader(":round_pushpin: Addresses")
                    if len(addr) > 0:
                        for _, a in addr.iterrows():
                            primary_tag = " :star:" if a["IS_PRIMARY"] else ""
                            st.write(f"**{a['ADDRESS_TYPE']}**{primary_tag}")
                            st.write(f"{a['STREET']}")
                            st.write(f"{a['CITY']}, {a['POSTAL_CODE']}, {a['COUNTRY']}")
                            st.write("---")
                    else:
                        st.write("No address on file")

            xref_col1, xref_col2 = st.columns(2)
            with xref_col1:
                with st.container(border=True):
                    st.subheader(":link: Customer XREF")
                    if len(c_xref):
                        st.dataframe(c_xref, hide_index=True, width="stretch")
                    else:
                        st.write("No customer XREF records")
            with xref_col2:
                with st.container(border=True):
                    st.subheader(":link: Address XREF")
                    if len(a_xref):
                        st.dataframe(a_xref, hide_index=True, width="stretch")
                    else:
                        st.write("No address XREF records")

# --- DATA QUALITY ---
with tab_dq:
    st.subheader("Data Quality Analysis")

    cust_tab, addr_tab = st.tabs(["Customer DQ", "Address DQ"])

    with cust_tab:
        with st.container(horizontal=True):
            for _, row in cust_dq_dist.iterrows():
                st.metric(
                    row["DQ_TIER"],
                    f"{int(row['RECORD_COUNT']):,}",
                    f"avg {row['AVG_SCORE']}",
                    border=True,
                )

        with st.container(border=True):
            st.subheader("Customer DQ Score Distribution")
            dq_hist = customers_df["DQ_SCORE"].value_counts().reset_index()
            dq_hist.columns = ["DQ_SCORE", "COUNT"]
            dq_hist = dq_hist.sort_values("DQ_SCORE")
            st.bar_chart(dq_hist, x="DQ_SCORE", y="COUNT")

        with st.container(border=True):
            st.subheader("Low Quality Customers (DQ < 50)")
            low_dq = customers_df[customers_df["DQ_SCORE"] < 50].sort_values("DQ_SCORE")
            st.write(f"{len(low_dq)} customers with DQ score below 50")
            st.dataframe(
                low_dq[["CUSTOMER_ID", "FULL_NAME", "EMAIL", "PHONE", "DQ_SCORE", "DQ_TIER"]],
                hide_index=True, width="stretch",
            )

    with addr_tab:
        with st.container(horizontal=True):
            st.metric("Total Addresses", f"{total_addr:,}", border=True)
            st.metric("Address Coverage", f"{addr_coverage}%", border=True)
            missing_addr = total_cust - cust_with_addr
            st.metric("Customers w/o Address", f"{missing_addr:,}", border=True)

        with st.container(border=True):
            st.subheader("Address Completeness")
            completeness_checks = addresses_df.copy()
            missing_street = completeness_checks["STREET"].isna().sum()
            missing_city = completeness_checks["CITY"].isna().sum()
            missing_postal = completeness_checks["POSTAL_CODE"].isna().sum()
            missing_country = completeness_checks["COUNTRY"].isna().sum()
            comp_df = pd.DataFrame({
                "Field": ["Street", "City", "Postal Code", "Country"],
                "Missing": [int(missing_street), int(missing_city), int(missing_postal), int(missing_country)],
                "Complete": [
                    int(total_addr - missing_street), int(total_addr - missing_city),
                    int(total_addr - missing_postal), int(total_addr - missing_country),
                ],
                "Completeness %": [
                    round((total_addr - missing_street) / total_addr * 100, 1) if total_addr else 0,
                    round((total_addr - missing_city) / total_addr * 100, 1) if total_addr else 0,
                    round((total_addr - missing_postal) / total_addr * 100, 1) if total_addr else 0,
                    round((total_addr - missing_country) / total_addr * 100, 1) if total_addr else 0,
                ],
            })
            st.dataframe(comp_df, hide_index=True, width="stretch")

        with st.container(border=True):
            st.subheader("Addresses by Country")
            st.bar_chart(country_dist, x="COUNTRY", y="ADDRESS_COUNT")

# --- ENTITY RESOLUTION ---
with tab_er:
    st.subheader("Entity Resolution & Cross-References")

    er_cust_tab, er_addr_tab = st.tabs(["Customer XREF", "Address XREF"])

    with er_cust_tab:
        total_source_keys = int(cust_source_counts["RECORD_COUNT"].sum())
        with st.container(horizontal=True):
            st.metric("Source Records", f"{total_source_keys:,}", border=True)
            for _, row in cust_source_counts.iterrows():
                st.metric(f"{row['SOURCE_SYSTEM']} Keys", f"{int(row['RECORD_COUNT']):,}", border=True)
            merge_rate = round(merged_cust / total_cust * 100, 1) if total_cust else 0
            st.metric("Merge Rate", f"{merge_rate}%", border=True)

        with st.container(border=True):
            st.subheader("Merged Customers (found in multiple sources)")
            merged_custs = customers_df[customers_df["SOURCE_COUNT"] >= 2]
            st.write(f"{len(merged_custs)} customers matched across source systems")
            st.dataframe(
                merged_custs[["CUSTOMER_ID", "FULL_NAME", "EMAIL", "DQ_SCORE", "SOURCE_COUNT"]].head(50),
                hide_index=True, width="stretch",
            )

        with st.container(border=True):
            st.subheader("Customer XREF Table (sample)")
            if st.button("Load XREF data (slow - uses Cortex AI)", key="load_cust_xref"):
                cust_xref_df = load_customer_xref()
                st.dataframe(cust_xref_df.head(100), hide_index=True, width="stretch")

    with er_addr_tab:
        total_addr_keys = int(addr_source_counts["RECORD_COUNT"].sum())
        with st.container(horizontal=True):
            st.metric("Source Records", f"{total_addr_keys:,}", border=True)
            for _, row in addr_source_counts.iterrows():
                st.metric(f"{row['SOURCE_SYSTEM']} Keys", f"{int(row['RECORD_COUNT']):,}", border=True)

        with st.container(border=True):
            st.subheader("Address XREF Table (sample)")
            if st.button("Load XREF data (slow - uses Cortex AI)", key="load_addr_xref"):
                addr_xref_df = load_address_xref()
                st.dataframe(addr_xref_df.head(100), hide_index=True, width="stretch")

        with st.container(border=True):
            st.subheader("Full Address List (sample)")
            st.dataframe(
                addresses_df.head(50)[["ADDRESS_ID", "CUSTOMER_ID", "STREET", "CITY", "POSTAL_CODE", "COUNTRY"]],
                hide_index=True, width="stretch",
            )

# --- SCD HISTORY ---
with tab_history:
    st.subheader("SCD Type 2 History")

    hist_cust_tab, hist_addr_tab = st.tabs(["Customer History", "Address History"])

    with hist_cust_tab:
        cust_history_df = load_customer_history()
        hist_customer = st.selectbox(
            "Select customer",
            sorted(cust_history_df["CUSTOMER_ID"].unique()),
            format_func=lambda x: f"Customer #{x}",
            key="hist_cust_select",
        )
        if hist_customer:
            cust_hist = cust_history_df[cust_history_df["CUSTOMER_ID"] == hist_customer].sort_values("VALID_FROM")
            st.write(f"{len(cust_hist)} version(s)")
            st.dataframe(cust_hist, hide_index=True, width="stretch")
            if len(cust_hist) > 1:
                dq_trend = cust_hist[["VALID_FROM", "DQ_SCORE"]].copy()
                dq_trend["VALID_FROM"] = pd.to_datetime(dq_trend["VALID_FROM"])
                st.line_chart(dq_trend, x="VALID_FROM", y="DQ_SCORE")

    with hist_addr_tab:
        addr_history_df = load_address_history()
        hist_address = st.selectbox(
            "Select address (by ID)",
            sorted(addr_history_df["ADDRESS_ID"].unique()),
            format_func=lambda x: f"Address #{x}",
            key="hist_addr_select",
        )
        if hist_address:
            addr_hist = addr_history_df[addr_history_df["ADDRESS_ID"] == hist_address].sort_values("VALID_FROM")
            st.write(f"{len(addr_hist)} version(s)")
            st.dataframe(addr_hist, hide_index=True, width="stretch")
