import os
import psycopg2
import pandas as pd
import streamlit as st

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

@st.cache_data(ttl=5)
def fetch_categories() -> list:
    """Fetch distinct categories for sidebar filters."""
    q = "SELECT DISTINCT category FROM dim_products ORDER BY category;"
    try:
        with get_conn() as conn:
            df = pd.read_sql(q, conn)
        return ["All"] + df["category"].tolist()
    except Exception:
        return ["All"]

@st.cache_data(ttl=5)
def fetch_stores() -> list:
    """Fetch store list for sidebar filters."""
    q = "SELECT store_id, store_name, city FROM dim_stores ORDER BY store_name;"
    try:
        with get_conn() as conn:
            df = pd.read_sql(q, conn)
        return [{"id": "All", "name": "All Stores"}] + [
            {"id": row["store_id"], "name": f"{row['store_name']} ({row['city']})"}
            for _, row in df.iterrows()
        ]
    except Exception:
        return [{"id": "All", "name": "All Stores"}]

@st.cache_data(ttl=10)
def fetch_metrics(hours_back: int, category: str = "All", store_id: str = "All") -> pd.DataFrame:
    """
    Fetch minute-by-minute product metrics.
    If filtered by store, dynamically aggregates raw transaction data.
    """
    if store_id != "All":
        # Dynamic aggregation from transactions table
        q = """
            SELECT
                date_trunc('minute', t.ts) as window_start,
                date_trunc('minute', t.ts) + interval '1 minute' as window_end,
                t.product_id,
                dp.product_name,
                dp.category,
                SUM(t.amount) as total_revenue,
                COUNT(t.transaction_id) as transaction_count
            FROM transactions t
            LEFT JOIN dim_products dp ON t.product_id = dp.product_id
            WHERE t.ts >= (NOW()::timestamp - (%s || ' hours')::interval)
              AND t.store_id = %s
        """
        params = [hours_back, store_id]
        if category != "All":
            q += " AND dp.category = %s"
            params.append(category)
        q += """
            GROUP BY 1, 2, 3, 4, 5
            ORDER BY window_start ASC;
        """
    else:
        # Pre-aggregated from metrics table (faster)
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
        """
        params = [hours_back]
        if category != "All":
            q += " AND dp.category = %s"
            params.append(category)
        q += " ORDER BY pm.window_start ASC;"

    with get_conn() as conn:
        df = pd.read_sql(q, conn, params=params)
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
def fetch_recent_transactions(limit: int = 50, category: str = "All", store_id: str = "All") -> pd.DataFrame:
    """Fetch recent transaction details with optional category and store filters."""
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
        WHERE 1=1
    """
    params = []
    if category != "All":
        q += " AND dp.category = %s"
        params.append(category)
    if store_id != "All":
        q += " AND t.store_id = %s"
        params.append(store_id)
        
    q += " ORDER BY t.ts DESC LIMIT %s;"
    params.append(limit)

    with get_conn() as conn:
        df = pd.read_sql(q, conn, params=params)
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
def fetch_refunds_summary(store_id: str = "All") -> pd.DataFrame:
    """Fetch refunds summary with reason breakdown and optional store filter."""
    q = """
        SELECT
            r.reason,
            COUNT(*) as refund_count,
            SUM(r.refund_amount) as total_refunded
        FROM refunds r
        WHERE 1=1
    """
    params = []
    if store_id != "All":
        q += " AND r.store_id = %s"
        params.append(store_id)
        
    q += " GROUP BY r.reason ORDER BY total_refunded DESC;"
    
    try:
        with get_conn() as conn:
            df = pd.read_sql(q, conn, params=params if params else None)
        if not df.empty:
            df["total_refunded"] = pd.to_numeric(df["total_refunded"], errors="coerce").fillna(0.0)
            df["refund_count"] = pd.to_numeric(df["refund_count"], errors="coerce").fillna(0).astype(int)
        return df
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=10)
def fetch_refunds_total(store_id: str = "All") -> dict:
    """Fetch total refunds count and amount with optional store filter."""
    q = """
        SELECT
            COUNT(*) as total_count,
            COALESCE(SUM(refund_amount), 0) as total_amount
        FROM refunds
        WHERE 1=1
    """
    params = []
    if store_id != "All":
        q += " AND store_id = %s"
        params.append(store_id)
        
    try:
        with get_conn() as conn:
            df = pd.read_sql(q, conn, params=params if params else None)
        if not df.empty:
            return {
                "count": int(df["total_count"].iloc[0]),
                "amount": float(df["total_amount"].iloc[0])
            }
        return {"count": 0, "amount": 0.0}
    except Exception:
        return {"count": 0, "amount": 0.0}
