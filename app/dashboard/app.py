"""
Real-time Retail Analytics Dashboard

Features:
- Revenue metrics and time-series visualization
- Customer Segmentation (RFM + K-Means)  
- Product Recommendations (Association Rules)
- Recent transactions view
"""

import os
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
import streamlit as st
import altair as alt


def _env_first(names, default=None, cast=None):
    for n in names:
        v = os.getenv(n)
        if v not in (None, ""):
            return cast(v) if cast else v
    return default


# ----------------- DB config -----------------
PG_HOST = _env_first(["PG_HOST", "POSTGRES_HOST"], default="postgres")
PG_PORT = _env_first(["PG_PORT", "POSTGRES_PORT"], default=5432, cast=int)
PG_DB = _env_first(["PG_DB", "POSTGRES_DB"], default="retaildb")
PG_USER = _env_first(["PG_USER", "POSTGRES_USER"], default="postgres")
PG_PASSWORD = _env_first(["PG_PASSWORD", "POSTGRES_PASSWORD"], default="postgres")


def get_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def _coerce_dt(df: pd.DataFrame, col: str, label: str) -> pd.DataFrame:
    """Coerce a datetime-like column; drop rows that fail conversion."""
    if df.empty or col not in df.columns:
        return df
    raw = df[col].copy()
    df[col] = pd.to_datetime(df[col], errors="coerce")
    bad_mask = df[col].isna() & raw.notna()
    if bad_mask.any():
        df = df.loc[~bad_mask].copy()
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Data Fetching Functions
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=10)
def fetch_metrics(hours_back: int) -> pd.DataFrame:
    q = """
        SELECT
            pm.window_start,
            pm.window_end,
            pm.product_id,
            dp.product_name,
            dp.category,
            pm.total_revenue,
            pm.transaction_count
        FROM product_metrics_minute pm
        LEFT JOIN dim_products dp ON pm.product_id = dp.product_id
        WHERE pm.window_start >= (NOW()::timestamp - (%s || ' hours')::interval)
        ORDER BY pm.window_start ASC;
    """
    with get_conn() as conn:
        df = pd.read_sql(q, conn, params=(hours_back,))
    if df.empty:
        return df
    df = _coerce_dt(df, "window_start", "window_start")
    df = _coerce_dt(df, "window_end", "window_end")
    df["total_revenue"] = pd.to_numeric(df.get("total_revenue"), errors="coerce").fillna(0.0)
    df["transaction_count"] = pd.to_numeric(df.get("transaction_count"), errors="coerce").fillna(0).astype(int)
    return df


@st.cache_data(ttl=10)
def fetch_customer_segments() -> pd.DataFrame:
    """Fetch customer segmentation data."""
    q = """
        SELECT
            customer_id,
            recency_days,
            frequency,
            monetary,
            r_score,
            f_score,
            m_score,
            rfm_segment,
            cluster_id,
            updated_at
        FROM customer_segments
        ORDER BY monetary DESC;
    """
    try:
        with get_conn() as conn:
            df = pd.read_sql(q, conn)
        if not df.empty:
            df["monetary"] = pd.to_numeric(df["monetary"], errors="coerce").fillna(0.0)
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=10)
def fetch_segment_summary() -> pd.DataFrame:
    """Get aggregated segment statistics."""
    q = """
        SELECT
            rfm_segment,
            COUNT(*) as customer_count,
            ROUND(AVG(monetary)::numeric, 2) as avg_monetary,
            ROUND(AVG(frequency)::numeric, 1) as avg_frequency,
            ROUND(AVG(recency_days)::numeric, 1) as avg_recency
        FROM customer_segments
        GROUP BY rfm_segment
        ORDER BY avg_monetary DESC;
    """
    try:
        with get_conn() as conn:
            return pd.read_sql(q, conn)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=10)
def fetch_product_associations() -> pd.DataFrame:
    """Fetch product association rules."""
    q = """
        SELECT
            antecedent as "If Customer Buys",
            consequent as "They Also Buy",
            ROUND(support::numeric * 100, 2) as "Support %",
            ROUND(confidence::numeric * 100, 1) as "Confidence %",
            ROUND(lift::numeric, 2) as "Lift"
        FROM product_associations
        ORDER BY lift DESC
        LIMIT 20;
    """
    try:
        with get_conn() as conn:
            return pd.read_sql(q, conn)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=10)
def fetch_recent_transactions(limit: int = 50) -> pd.DataFrame:
    q = """
        SELECT
            t.ts,
            t.transaction_id,
            t.product_id,
            dp.product_name,
            dp.category,
            t.store_id,
            t.customer_id,
            t.amount
        FROM transactions t
        LEFT JOIN dim_products dp ON t.product_id = dp.product_id
        ORDER BY t.ts DESC
        LIMIT %s;
    """
    with get_conn() as conn:
        df = pd.read_sql(q, conn, params=(limit,))
    if df.empty:
        return df
    df = _coerce_dt(df, "ts", "ts")
    df["amount"] = pd.to_numeric(df.get("amount"), errors="coerce")
    return df


@st.cache_data(ttl=10)
def fetch_store_performance() -> pd.DataFrame:
    """Fetch store performance with store details from dim_stores."""
    q = """
        SELECT
            t.store_id,
            ds.store_name,
            ds.city,
            ds.region,
            COUNT(*) as transaction_count,
            SUM(t.amount) as total_revenue
        FROM transactions t
        LEFT JOIN dim_stores ds ON t.store_id = ds.store_id
        WHERE t.store_id IS NOT NULL
        GROUP BY t.store_id, ds.store_name, ds.city, ds.region
        ORDER BY total_revenue DESC;
    """
    try:
        with get_conn() as conn:
            df = pd.read_sql(q, conn)
        if not df.empty:
            df["total_revenue"] = pd.to_numeric(df["total_revenue"], errors="coerce").fillna(0.0)
            df["transaction_count"] = pd.to_numeric(df["transaction_count"], errors="coerce").fillna(0).astype(int)
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=10)
def fetch_refunds_summary() -> pd.DataFrame:
    """Fetch refunds summary with reason breakdown."""
    q = """
        SELECT
            r.reason,
            COUNT(*) as refund_count,
            SUM(r.refund_amount) as total_refunded
        FROM refunds r
        GROUP BY r.reason
        ORDER BY total_refunded DESC;
    """
    try:
        with get_conn() as conn:
            df = pd.read_sql(q, conn)
        if not df.empty:
            df["total_refunded"] = pd.to_numeric(df["total_refunded"], errors="coerce").fillna(0.0)
            df["refund_count"] = pd.to_numeric(df["refund_count"], errors="coerce").fillna(0).astype(int)
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=10)
def fetch_refunds_total() -> dict:
    """Fetch total refunds count and amount."""
    q = """
        SELECT
            COUNT(*) as total_count,
            COALESCE(SUM(refund_amount), 0) as total_amount
        FROM refunds;
    """
    try:
        with get_conn() as conn:
            df = pd.read_sql(q, conn)
        if not df.empty:
            return {
                "count": int(df["total_count"].iloc[0]),
                "amount": float(df["total_amount"].iloc[0])
            }
        return {"count": 0, "amount": 0.0}
    except Exception:
        return {"count": 0, "amount": 0.0}


# ─────────────────────────────────────────────────────────────────────────────
# Chart Functions
# ─────────────────────────────────────────────────────────────────────────────

def revenue_area_chart(df: pd.DataFrame) -> alt.Chart:
    """Create a stacked area chart for revenue over time - smoother than bar chart."""
    return (
        alt.Chart(df)
        .mark_area(opacity=0.7, interpolate="monotone")
        .encode(
            x=alt.X("window_start:T", title="Time", axis=alt.Axis(format="%H:%M")),
            y=alt.Y("total_revenue:Q", title="Revenue ($)", stack="zero"),
            color=alt.Color(
                "product_label:N", 
                title="Product",
                scale=alt.Scale(scheme="tableau10")
            ),
            tooltip=[
                alt.Tooltip("window_start:T", title="Time", format="%H:%M"),
                alt.Tooltip("product_label:N", title="Product"),
                alt.Tooltip("total_revenue:Q", title="Revenue", format="$,.2f"),
                alt.Tooltip("transaction_count:Q", title="Transactions"),
            ],
        )
        .properties(height=300)
    )


def category_pie_chart(df: pd.DataFrame) -> alt.Chart:
    """Create revenue breakdown by category pie chart."""
    category_data = df.groupby("category").agg({
        "total_revenue": "sum"
    }).reset_index()
    
    return (
        alt.Chart(category_data)
        .mark_arc(innerRadius=40, outerRadius=100)
        .encode(
            theta=alt.Theta("total_revenue:Q", title="Revenue"),
            color=alt.Color(
                "category:N", 
                title="Category",
                scale=alt.Scale(scheme="category10")
            ),
            tooltip=[
                alt.Tooltip("category:N", title="Category"),
                alt.Tooltip("total_revenue:Q", title="Revenue", format="$,.2f"),
            ],
        )
        .properties(height=250)
    )


def top_products_chart(df: pd.DataFrame) -> alt.Chart:
    """Create horizontal bar chart of top products by revenue."""
    product_data = df.groupby(["product_id", "product_label"]).agg({
        "total_revenue": "sum",
        "transaction_count": "sum"
    }).reset_index().sort_values("total_revenue", ascending=False).head(6)
    
    return (
        alt.Chart(product_data)
        .mark_bar(cornerRadiusEnd=4)
        .encode(
            x=alt.X("total_revenue:Q", title="Total Revenue ($)"),
            y=alt.Y("product_label:N", title="", sort="-x"),
            color=alt.Color(
                "product_label:N",
                legend=None,
                scale=alt.Scale(scheme="tableau10")
            ),
            tooltip=[
                alt.Tooltip("product_label:N", title="Product"),
                alt.Tooltip("total_revenue:Q", title="Revenue", format="$,.2f"),
                alt.Tooltip("transaction_count:Q", title="Transactions"),
            ],
        )
        .properties(height=250)
    )


def store_performance_chart(df: pd.DataFrame) -> alt.Chart:
    """Create grouped bar chart comparing store performance by region."""
    if df.empty:
        return alt.Chart(pd.DataFrame()).mark_text().encode()
    
    # Create label with store name and city
    df = df.copy()
    df["store_label"] = df["store_name"] + " (" + df["city"] + ")"
    
    return (
        alt.Chart(df)
        .mark_bar(cornerRadiusEnd=4)
        .encode(
            x=alt.X("total_revenue:Q", title="Total Revenue ($)"),
            y=alt.Y("store_label:N", title="", sort="-x"),
            color=alt.Color(
                "region:N",
                title="Region",
                scale=alt.Scale(scheme="tableau10")
            ),
            tooltip=[
                alt.Tooltip("store_name:N", title="Store"),
                alt.Tooltip("city:N", title="City"),
                alt.Tooltip("region:N", title="Region"),
                alt.Tooltip("total_revenue:Q", title="Revenue", format="$,.2f"),
                alt.Tooltip("transaction_count:Q", title="Transactions"),
            ],
        )
        .properties(height=200)
    )


def cumulative_revenue_chart(df: pd.DataFrame) -> alt.Chart:
    """Create cumulative revenue line chart - shows growth trajectory."""
    # Calculate cumulative revenue over time
    time_data = df.groupby("window_start").agg({
        "total_revenue": "sum"
    }).reset_index().sort_values("window_start")
    
    time_data["cumulative_revenue"] = time_data["total_revenue"].cumsum()
    
    line = (
        alt.Chart(time_data)
        .mark_line(color="#2ecc71", strokeWidth=3)
        .encode(
            x=alt.X("window_start:T", title="Time", axis=alt.Axis(format="%H:%M")),
            y=alt.Y("cumulative_revenue:Q", title="Cumulative Revenue ($)"),
            tooltip=[
                alt.Tooltip("window_start:T", title="Time", format="%H:%M"),
                alt.Tooltip("cumulative_revenue:Q", title="Cumulative", format="$,.2f"),
            ],
        )
    )
    
    area = (
        alt.Chart(time_data)
        .mark_area(opacity=0.3, color="#2ecc71")
        .encode(
            x=alt.X("window_start:T"),
            y=alt.Y("cumulative_revenue:Q"),
        )
    )
    
    return (area + line).properties(height=250)


def transaction_volume_chart(df: pd.DataFrame) -> alt.Chart:
    """Create transaction volume over time - number of transactions per window."""
    time_data = df.groupby("window_start").agg({
        "transaction_count": "sum"
    }).reset_index().sort_values("window_start")
    
    return (
        alt.Chart(time_data)
        .mark_bar(color="#3498db", opacity=0.8)
        .encode(
            x=alt.X("window_start:T", title="Time", axis=alt.Axis(format="%H:%M")),
            y=alt.Y("transaction_count:Q", title="Transaction Count"),
            tooltip=[
                alt.Tooltip("window_start:T", title="Time", format="%H:%M"),
                alt.Tooltip("transaction_count:Q", title="Transactions"),
            ],
        )
        .properties(height=200)
    )


def refunds_by_reason_chart(df: pd.DataFrame) -> alt.Chart:
    """Create horizontal bar chart showing refunds by reason."""
    if df.empty:
        return alt.Chart(pd.DataFrame()).mark_text().encode()
    
    return (
        alt.Chart(df)
        .mark_bar(cornerRadiusEnd=4, color="#e74c3c")
        .encode(
            x=alt.X("total_refunded:Q", title="Total Refunded ($)"),
            y=alt.Y("reason:N", title="", sort="-x"),
            tooltip=[
                alt.Tooltip("reason:N", title="Reason"),
                alt.Tooltip("total_refunded:Q", title="Amount", format="$,.2f"),
                alt.Tooltip("refund_count:Q", title="Count"),
            ],
        )
        .properties(height=200)
    )


def segment_pie_chart(df: pd.DataFrame) -> alt.Chart:
    """Create customer segment distribution pie chart."""
    return (
        alt.Chart(df)
        .mark_arc(innerRadius=50)
        .encode(
            theta=alt.Theta("customer_count:Q", title="Customers"),
            color=alt.Color(
                "rfm_segment:N",
                title="Segment",
                scale=alt.Scale(
                    domain=["Champions", "Loyal Customers", "Potential Loyalists", 
                            "New Customers", "At Risk", "Need Attention", "Hibernating"],
                    range=["#2ecc71", "#3498db", "#9b59b6", "#1abc9c", "#e74c3c", "#f39c12", "#95a5a6"]
                )
            ),
            tooltip=[
                alt.Tooltip("rfm_segment:N", title="Segment"),
                alt.Tooltip("customer_count:Q", title="Customers"),
                alt.Tooltip("avg_monetary:Q", title="Avg Spend", format="$,.2f"),
            ],
        )
        .properties(height=300)
    )


def cluster_scatter(df: pd.DataFrame) -> alt.Chart:
    """Create K-Means cluster scatter plot."""
    return (
        alt.Chart(df)
        .mark_circle(size=60, opacity=0.7)
        .encode(
            x=alt.X("recency_days:Q", title="Recency (days)", scale=alt.Scale(reverse=True)),
            y=alt.Y("monetary:Q", title="Total Spend ($)"),
            color=alt.Color("cluster_id:N", title="Cluster", scale=alt.Scale(scheme="category10")),
            tooltip=[
                alt.Tooltip("customer_id:N", title="Customer"),
                alt.Tooltip("recency_days:Q", title="Recency"),
                alt.Tooltip("frequency:Q", title="Frequency"),
                alt.Tooltip("monetary:Q", title="Monetary", format="$,.2f"),
                alt.Tooltip("rfm_segment:N", title="Segment"),
            ],
        )
        .properties(height=350)
    )


# ─────────────────────────────────────────────────────────────────────────────
# Main Dashboard
# ─────────────────────────────────────────────────────────────────────────────

def main():
    st.set_page_config(
        page_title="Retail Analytics Dashboard",
        page_icon="�",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # =========================================================================
    # HEADER
    # =========================================================================
    st.markdown("# Real-time Retail Analytics Dashboard")
    st.markdown("""
    This dashboard provides real-time insights into retail operations, combining **streaming data** 
    from Kafka with **batch data** from our data warehouse. Data refreshes automatically every 10 seconds.
    """)

    # =========================================================================
    # SIDEBAR
    # =========================================================================
    with st.sidebar:
        st.header("Dashboard Controls")
        
        hours_back = st.number_input(
            "Time Window (hours)",
            min_value=1,
            max_value=168,
            value=24,
            step=1,
            help="How far back to look for metrics data"
        )
        
        st.divider()
        
        st.markdown("### 📊 Data Sources")
        st.markdown("""
        | Source | Type |
        |--------|------|
        | Products | Batch |
        | Stores | Batch |
        | Transactions | Stream |
        | Refunds | Stream |
        """)
        
        st.divider()
        st.caption("💡 Dashboard auto-refreshes every 10 seconds")

    # =========================================================================
    # FETCH ALL DATA
    # =========================================================================
    metrics_df = fetch_metrics(int(hours_back))
    segments_df = fetch_customer_segments()
    segment_summary = fetch_segment_summary()
    associations_df = fetch_product_associations()
    recent_tx_df = fetch_recent_transactions(limit=50)
    store_perf_df = fetch_store_performance()
    refunds_summary_df = fetch_refunds_summary()
    refunds_total = fetch_refunds_total()

    # Calculate metrics
    total_revenue = float(metrics_df["total_revenue"].sum()) if not metrics_df.empty else 0.0
    total_tx = int(metrics_df["transaction_count"].sum()) if not metrics_df.empty else 0
    total_refunds = refunds_total["amount"]
    net_revenue = total_revenue - total_refunds
    total_customers = len(segments_df) if not segments_df.empty else 0
    refund_rate = (total_refunds / total_revenue * 100) if total_revenue > 0 else 0

    # =========================================================================
    # SECTION 1: KEY PERFORMANCE INDICATORS
    # =========================================================================
    st.markdown("## Key Performance Indicators")
    st.markdown("*Real-time business metrics at a glance*")
    
    kpi_col1, kpi_col2, kpi_col3, kpi_col4, kpi_col5 = st.columns(5)
    
    with kpi_col1:
        st.metric(
            label="Gross Revenue",
            value=f"${total_revenue:,.2f}",
            help="Total revenue from all transactions"
        )
    
    with kpi_col2:
        st.metric(
            label="Refunds",
            value=f"${total_refunds:,.2f}",
            delta=f"{refund_rate:.1f}% of sales",
            delta_color="inverse",
            help="Total refunds processed"
        )
    
    with kpi_col3:
        st.metric(
            label="Net Revenue",
            value=f"${net_revenue:,.2f}",
            help="Gross revenue minus refunds"
        )
    
    with kpi_col4:
        st.metric(
            label="Transactions",
            value=f"{total_tx:,}",
            help="Total number of transactions"
        )
    
    with kpi_col5:
        st.metric(
            label="Customers",
            value=f"{total_customers:,}",
            help="Customers analyzed by ML"
        )
    
    st.divider()

    # =========================================================================
    # SECTION 2: SALES ANALYTICS
    # =========================================================================
    st.markdown("## Sales Analytics")
    st.markdown("*Revenue trends and product performance from streaming transaction data*")
    
    if metrics_df.empty:
        st.info("Waiting for sales data... Ensure Spark is running and processing Kafka messages.")
    else:
        chart_df = metrics_df.copy()
        chart_df["product_label"] = chart_df["product_name"].fillna(chart_df["product_id"])
        
        # Main revenue chart
        st.markdown("### Revenue Over Time")
        st.altair_chart(revenue_area_chart(chart_df), use_container_width=True)
        
        # Three summary charts
        st.markdown("### Revenue Breakdown")
        chart_col1, chart_col2, chart_col3 = st.columns(3)
        
        with chart_col1:
            st.markdown("**By Category**")
            st.altair_chart(category_pie_chart(chart_df), use_container_width=True)
        
        with chart_col2:
            st.markdown("**Top Products**")
            st.altair_chart(top_products_chart(chart_df), use_container_width=True)
        
        with chart_col3:
            st.markdown("**Cumulative Growth**")
            st.altair_chart(cumulative_revenue_chart(chart_df), use_container_width=True)
        
        # Transaction volume
        with st.expander("View Transaction Volume Over Time", expanded=False):
            st.altair_chart(transaction_volume_chart(chart_df), use_container_width=True)
    
    st.divider()

    # =========================================================================
    # SECTION 3: REFUNDS & STORE PERFORMANCE (Side by Side)
    # =========================================================================
    st.markdown("## Operations Overview")
    st.markdown("*Refund patterns and store-level performance*")
    
    ops_col1, ops_col2 = st.columns(2)
    
    with ops_col1:
        st.markdown("### Refund Analysis")
        st.caption("Data source: Kafka `refunds` stream")
        
        if refunds_summary_df.empty:
            st.info("Waiting for refund data...")
        else:
            st.altair_chart(refunds_by_reason_chart(refunds_summary_df), use_container_width=True)
            
            with st.expander("View Refund Details"):
                st.dataframe(
                    refunds_summary_df.rename(columns={
                        "reason": "Reason",
                        "refund_count": "Count",
                        "total_refunded": "Amount ($)"
                    }),
                    hide_index=True,
                    use_container_width=True
                )
    
    with ops_col2:
        st.markdown("### Store Performance")
        st.caption("Data source: `store_locations.csv` (batch)")
        
        if store_perf_df.empty:
            st.info("Waiting for store data...")
        else:
            st.altair_chart(store_performance_chart(store_perf_df), use_container_width=True)
            
            with st.expander("View Store Details"):
                st.dataframe(
                    store_perf_df.rename(columns={
                        "store_name": "Store",
                        "city": "City",
                        "region": "Region",
                        "transaction_count": "Transactions",
                        "total_revenue": "Revenue ($)"
                    }),
                    hide_index=True,
                    use_container_width=True
                )
    
    st.divider()

    # =========================================================================
    # SECTION 4: ML INSIGHTS (Tabbed)
    # =========================================================================
    st.markdown("## Machine Learning Insights")
    st.markdown("*AI-powered customer segmentation and product recommendations*")
    
    ml_tab1, ml_tab2 = st.tabs(["Customer Segmentation", "Product Recommendations"])
    
    # --- Tab 1: Customer Segmentation ---
    with ml_tab1:
        st.markdown("""
        **RFM Analysis** groups customers based on:
        - **R**ecency: How recently they purchased
        - **F**requency: How often they purchase  
        - **M**onetary: How much they spend
        
        **K-Means Clustering** further groups customers into behavioral clusters.
        """)
        
        if segments_df.empty:
            st.info("Customer segmentation not yet available. The ML service needs more transaction data (minimum 10 customers).")
        else:
            seg_col1, seg_col2 = st.columns(2)
            
            with seg_col1:
                st.markdown("#### Segment Distribution")
                if not segment_summary.empty:
                    st.altair_chart(segment_pie_chart(segment_summary), use_container_width=True)
            
            with seg_col2:
                st.markdown("#### Customer Clusters")
                scatter_df = segments_df.head(100)
                st.altair_chart(cluster_scatter(scatter_df), use_container_width=True)
            
            # Segment details
            st.markdown("#### Segment Statistics")
            if not segment_summary.empty:
                display_summary = segment_summary.rename(columns={
                    "rfm_segment": "Segment",
                    "customer_count": "Customers",
                    "avg_monetary": "Avg Spend ($)",
                    "avg_frequency": "Avg Orders",
                    "avg_recency": "Avg Days Since Last"
                })
                st.dataframe(display_summary, use_container_width=True, hide_index=True)
            
            with st.expander("View Top 10 Customers by Spend"):
                top_customers = segments_df.head(10)[["customer_id", "rfm_segment", "monetary", "frequency", "recency_days"]]
                top_customers = top_customers.rename(columns={
                    "customer_id": "Customer ID",
                    "rfm_segment": "Segment",
                    "monetary": "Total Spend ($)",
                    "frequency": "Orders",
                    "recency_days": "Days Since Last"
                })
                st.dataframe(top_customers, use_container_width=True, hide_index=True)
    
    # --- Tab 2: Product Recommendations ---
    with ml_tab2:
        st.markdown("""
        **Market Basket Analysis** discovers products frequently purchased together using the Apriori algorithm.
        
        **How to interpret the metrics:**
        - **Support**: % of all transactions containing this product pair
        - **Confidence**: Probability customer buys product B given they bought A
        - **Lift**: How much more likely than random (>1 = positive association)
        """)
        
        if associations_df.empty:
            st.info("Product associations not yet available. The ML service needs more multi-product transactions to find patterns.")
        else:
            st.dataframe(associations_df, use_container_width=True, hide_index=True)
            
            if len(associations_df) > 0:
                best = associations_df.iloc[0]
                st.success(
                    f"**Top Recommendation:** Customers who buy **{best['If Customer Buys']}** "
                    f"are **{best['Lift']:.1f}x** more likely to also buy **{best['They Also Buy']}** "
                    f"(Confidence: {best['Confidence %']}%)"
                )
    
    st.divider()

    # =========================================================================
    # SECTION 5: RECENT TRANSACTIONS (Collapsible)
    # =========================================================================
    with st.expander("View Recent Transactions (Last 50)", expanded=False):
        st.markdown("*Raw transaction data for auditing and verification*")
        if recent_tx_df.empty:
            st.info("No transactions found. Confirm the producer and Spark are running.")
        else:
            st.dataframe(recent_tx_df, use_container_width=True, hide_index=True)
    
    # =========================================================================
    # FOOTER
    # =========================================================================
    st.divider()
    st.caption("Data refreshes automatically every 10 seconds | Built with Streamlit + Altair")


if __name__ == "__main__":
    main()