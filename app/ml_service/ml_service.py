"""
ML Service: Customer Segmentation & Product Recommendations

This service provides two key ML features for the retail pipeline:
1. Customer Segmentation using RFM Analysis + K-Means Clustering
2. Product Recommendations using Market Basket Analysis (Apriori)
"""

import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

# Try to import mlxtend for association rules
try:
    from mlxtend.frequent_patterns import apriori, association_rules
    from mlxtend.preprocessing import TransactionEncoder
    MLXTEND_AVAILABLE = True
except ImportError:
    MLXTEND_AVAILABLE = False
    print("[ml_service] Warning: mlxtend not available, product associations disabled")


# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
DB_NAME = os.getenv("POSTGRES_DB", "retaildb")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

POLL_SECONDS = int(os.getenv("ML_POLL_SECONDS", "30"))
MIN_TRANSACTIONS = int(os.getenv("ML_MIN_TRANSACTIONS", "10"))
N_CLUSTERS = int(os.getenv("ML_N_CLUSTERS", "4"))


# ─────────────────────────────────────────────────────────────────────────────
# Database Connection
# ─────────────────────────────────────────────────────────────────────────────

def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def ensure_tables_exist(conn):
    """Create tables if they don't exist (for fresh databases)."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS customer_segments (
                customer_id     VARCHAR PRIMARY KEY,
                recency_days    INTEGER,
                frequency       INTEGER,
                monetary        NUMERIC(12, 2),
                r_score         INTEGER,
                f_score         INTEGER,
                m_score         INTEGER,
                rfm_segment     VARCHAR(50),
                cluster_id      INTEGER,
                updated_at      TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS product_associations (
                antecedent      VARCHAR NOT NULL,
                consequent      VARCHAR NOT NULL,
                support         NUMERIC(10, 6),
                confidence      NUMERIC(10, 6),
                lift            NUMERIC(10, 4),
                updated_at      TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (antecedent, consequent)
            );
        """)
    conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
# RFM Customer Segmentation
# ─────────────────────────────────────────────────────────────────────────────

def fetch_transactions_for_rfm(conn) -> pd.DataFrame:
    """Fetch transactions for RFM analysis."""
    sql = """
        SELECT 
            customer_id,
            ts,
            amount
        FROM transactions
        WHERE customer_id IS NOT NULL
          AND ts IS NOT NULL
          AND amount IS NOT NULL
        ORDER BY ts;
    """
    return pd.read_sql(sql, conn)


def calculate_rfm(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate RFM (Recency, Frequency, Monetary) metrics for each customer.
    """
    if df.empty:
        return pd.DataFrame()
    
    # Current timestamp for recency calculation
    now = datetime.utcnow()
    
    # Aggregate by customer
    rfm = df.groupby("customer_id").agg({
        "ts": lambda x: (now - x.max()).days,  # Recency: days since last purchase
        "amount": ["count", "sum"]              # Frequency & Monetary
    }).reset_index()
    
    # Flatten column names
    rfm.columns = ["customer_id", "recency_days", "frequency", "monetary"]
    
    # Convert monetary to float
    rfm["monetary"] = rfm["monetary"].astype(float)
    
    return rfm


def assign_rfm_scores(rfm: pd.DataFrame) -> pd.DataFrame:
    """
    Assign RFM scores (1-5) using quartile-based scoring.
    Handles edge cases where there aren't enough unique values for 5 bins.
    """
    if rfm.empty:
        return rfm
    
    def safe_qcut(series, q=5, ascending=True):
        """
        Safely assign quantile-based scores, falling back to simpler methods
        if there aren't enough unique values.
        """
        try:
            if ascending:
                labels = list(range(1, q + 1))
            else:
                labels = list(range(q, 0, -1))
            return pd.qcut(series.rank(method="first"), q=q, labels=labels, duplicates="drop").astype(int)
        except ValueError:
            # Not enough unique values for q bins, try fewer bins
            n_unique = series.nunique()
            if n_unique <= 1:
                # All same value, assign middle score
                return pd.Series([3] * len(series), index=series.index)
            
            # Try with fewer bins
            effective_q = min(q, n_unique)
            try:
                if ascending:
                    labels = [int(1 + (i * 4 / (effective_q - 1))) for i in range(effective_q)]
                else:
                    labels = [int(5 - (i * 4 / (effective_q - 1))) for i in range(effective_q)]
                return pd.qcut(series.rank(method="first"), q=effective_q, labels=labels, duplicates="drop").astype(int)
            except ValueError:
                # Last resort: use simple percentile-based scoring
                percentiles = series.rank(pct=True)
                if ascending:
                    scores = (percentiles * 4 + 1).round().astype(int).clip(1, 5)
                else:
                    scores = (5 - percentiles * 4).round().astype(int).clip(1, 5)
                return scores
    
    # For recency, lower is better (score reversed - low recency = high score)
    rfm["r_score"] = safe_qcut(rfm["recency_days"], q=5, ascending=False)
    
    # For frequency and monetary, higher is better
    rfm["f_score"] = safe_qcut(rfm["frequency"], q=5, ascending=True)
    rfm["m_score"] = safe_qcut(rfm["monetary"], q=5, ascending=True)
    
    return rfm


def assign_segments(rfm: pd.DataFrame) -> pd.DataFrame:
    """
    Assign customer segment labels based on RFM scores.
    """
    if rfm.empty:
        return rfm
    
    def get_segment(row):
        r, f, m = row["r_score"], row["f_score"], row["m_score"]
        
        # Champions: High R, F, M
        if r >= 4 and f >= 4 and m >= 4:
            return "Champions"
        # Loyal Customers: High F and M
        elif f >= 4 and m >= 4:
            return "Loyal Customers"
        # Potential Loyalists: Recent + moderate frequency
        elif r >= 4 and f >= 2:
            return "Potential Loyalists"
        # New Customers: Very recent, low frequency
        elif r >= 4 and f <= 2:
            return "New Customers"
        # At Risk: Used to be good, not recent
        elif r <= 2 and f >= 3:
            return "At Risk"
        # Hibernating: Low R, F, M
        elif r <= 2 and f <= 2:
            return "Hibernating"
        # Need Attention: Middle ground
        else:
            return "Need Attention"
    
    rfm["rfm_segment"] = rfm.apply(get_segment, axis=1)
    return rfm


def apply_kmeans_clustering(rfm: pd.DataFrame, n_clusters: int = 4) -> pd.DataFrame:
    """
    Apply K-Means clustering on RFM features for additional segmentation.
    """
    if rfm.empty or len(rfm) < n_clusters:
        rfm["cluster_id"] = 0
        return rfm
    
    # Features for clustering
    features = rfm[["recency_days", "frequency", "monetary"]].copy()
    
    # Standardize features
    scaler = StandardScaler()
    features_scaled = scaler.fit_transform(features)
    
    # Apply K-Means
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    rfm["cluster_id"] = kmeans.fit_predict(features_scaled)
    
    return rfm


def upsert_customer_segments(conn, rfm: pd.DataFrame) -> int:
    """Upsert customer segments into the database."""
    if rfm.empty:
        return 0
    
    rows = [
        (
            row["customer_id"],
            int(row["recency_days"]),
            int(row["frequency"]),
            float(row["monetary"]),
            int(row["r_score"]),
            int(row["f_score"]),
            int(row["m_score"]),
            row["rfm_segment"],
            int(row["cluster_id"]),
        )
        for _, row in rfm.iterrows()
    ]
    
    sql = """
        INSERT INTO customer_segments 
            (customer_id, recency_days, frequency, monetary, r_score, f_score, m_score, rfm_segment, cluster_id, updated_at)
        VALUES %s
        ON CONFLICT (customer_id) DO UPDATE SET
            recency_days = EXCLUDED.recency_days,
            frequency = EXCLUDED.frequency,
            monetary = EXCLUDED.monetary,
            r_score = EXCLUDED.r_score,
            f_score = EXCLUDED.f_score,
            m_score = EXCLUDED.m_score,
            rfm_segment = EXCLUDED.rfm_segment,
            cluster_id = EXCLUDED.cluster_id,
            updated_at = CURRENT_TIMESTAMP;
    """
    
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)", page_size=500)
    conn.commit()
    
    return len(rows)


def run_customer_segmentation(conn) -> int:
    """
    Main function to run customer segmentation pipeline.
    Returns the number of customers segmented.
    """
    # Fetch data
    df = fetch_transactions_for_rfm(conn)
    
    if df.empty or len(df) < MIN_TRANSACTIONS:
        print(f"[ml_service] Not enough transactions for segmentation ({len(df)} < {MIN_TRANSACTIONS})")
        return 0
    
    # Calculate RFM
    rfm = calculate_rfm(df)
    
    if rfm.empty or len(rfm) < 5:
        print("[ml_service] Not enough unique customers for segmentation")
        return 0
    
    # Assign scores and segments
    rfm = assign_rfm_scores(rfm)
    rfm = assign_segments(rfm)
    
    # Apply K-Means clustering
    rfm = apply_kmeans_clustering(rfm, n_clusters=min(N_CLUSTERS, len(rfm)))
    
    # Save to database
    count = upsert_customer_segments(conn, rfm)
    
    return count


# ─────────────────────────────────────────────────────────────────────────────
# Product Associations (Market Basket Analysis)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_transactions_for_basket(conn) -> pd.DataFrame:
    """Fetch transactions grouped by customer for basket analysis."""
    sql = """
        SELECT 
            customer_id,
            product_id
        FROM transactions
        WHERE customer_id IS NOT NULL
          AND product_id IS NOT NULL
        ORDER BY customer_id, ts;
    """
    return pd.read_sql(sql, conn)


def prepare_basket_data(df: pd.DataFrame) -> List[List[str]]:
    """
    Prepare transaction data for Apriori algorithm.
    Group products by customer to form "baskets".
    """
    if df.empty:
        return []
    
    # Group products by customer
    baskets = df.groupby("customer_id")["product_id"].apply(list).tolist()
    
    # Keep only unique products per basket
    baskets = [list(set(basket)) for basket in baskets]
    
    # Filter out single-item baskets (no associations possible)
    baskets = [b for b in baskets if len(b) >= 2]
    
    return baskets


def find_associations(baskets: List[List[str]], min_support: float = 0.01, min_confidence: float = 0.1) -> pd.DataFrame:
    """
    Find product associations using Apriori algorithm.
    """
    if not baskets or len(baskets) < 5:
        return pd.DataFrame()
    
    if not MLXTEND_AVAILABLE:
        return pd.DataFrame()
    
    try:
        # Encode transactions
        te = TransactionEncoder()
        te_array = te.fit(baskets).transform(baskets)
        df = pd.DataFrame(te_array, columns=te.columns_)
        
        # Find frequent itemsets
        frequent_itemsets = apriori(df, min_support=min_support, use_colnames=True)
        
        if frequent_itemsets.empty:
            return pd.DataFrame()
        
        # Generate association rules
        rules = association_rules(frequent_itemsets, metric="confidence", min_threshold=min_confidence)
        
        if rules.empty:
            return pd.DataFrame()
        
        # Convert frozensets to strings
        rules["antecedent"] = rules["antecedents"].apply(lambda x: ", ".join(sorted(x)))
        rules["consequent"] = rules["consequents"].apply(lambda x: ", ".join(sorted(x)))
        
        # Select relevant columns
        result = rules[["antecedent", "consequent", "support", "confidence", "lift"]].copy()
        
        # Sort by lift (most interesting associations first)
        result = result.sort_values("lift", ascending=False).head(50)
        
        return result
        
    except Exception as e:
        print(f"[ml_service] Error in association rules: {e}")
        return pd.DataFrame()


def upsert_product_associations(conn, rules: pd.DataFrame) -> int:
    """Upsert product associations into the database."""
    if rules.empty:
        return 0
    
    # Clear existing associations and insert new ones
    with conn.cursor() as cur:
        cur.execute("DELETE FROM product_associations;")
    
    rows = [
        (
            row["antecedent"],
            row["consequent"],
            float(row["support"]),
            float(row["confidence"]),
            float(row["lift"]),
        )
        for _, row in rules.iterrows()
    ]
    
    sql = """
        INSERT INTO product_associations 
            (antecedent, consequent, support, confidence, lift, updated_at)
        VALUES %s
        ON CONFLICT (antecedent, consequent) DO UPDATE SET
            support = EXCLUDED.support,
            confidence = EXCLUDED.confidence,
            lift = EXCLUDED.lift,
            updated_at = CURRENT_TIMESTAMP;
    """
    
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, template="(%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)", page_size=100)
    conn.commit()
    
    return len(rows)


def run_product_associations(conn) -> int:
    """
    Main function to run product association mining.
    Returns the number of associations found.
    """
    if not MLXTEND_AVAILABLE:
        return 0
    
    # Fetch data
    df = fetch_transactions_for_basket(conn)
    
    if df.empty:
        print("[ml_service] No transactions for basket analysis")
        return 0
    
    # Prepare baskets
    baskets = prepare_basket_data(df)
    
    if len(baskets) < 5:
        print(f"[ml_service] Not enough multi-item baskets ({len(baskets)})")
        return 0
    
    # Find associations
    rules = find_associations(baskets)
    
    if rules.empty:
        print("[ml_service] No significant product associations found")
        return 0
    
    # Save to database
    count = upsert_product_associations(conn, rules)
    
    return count


# ─────────────────────────────────────────────────────────────────────────────
# Main Loop
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print(
        f"[ml_service] Starting ML service:\n"
        f"  - Poll interval: {POLL_SECONDS}s\n"
        f"  - Min transactions: {MIN_TRANSACTIONS}\n"
        f"  - K-Means clusters: {N_CLUSTERS}\n"
        f"  - Mlxtend available: {MLXTEND_AVAILABLE}"
    )
    
    while True:
        try:
            conn = get_connection()
            ensure_tables_exist(conn)
            
            # Run Customer Segmentation
            seg_count = run_customer_segmentation(conn)
            if seg_count > 0:
                print(f"[ml_service] ✓ Segmented {seg_count} customers into RFM segments + {N_CLUSTERS} clusters")
            
            # Run Product Associations
            assoc_count = run_product_associations(conn)
            if assoc_count > 0:
                print(f"[ml_service] ✓ Found {assoc_count} product associations")
            
            if seg_count == 0 and assoc_count == 0:
                print("[ml_service] Waiting for more data...")
            
            conn.close()
            
        except Exception as e:
            print(f"[ml_service] Error: {e}")
        
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
