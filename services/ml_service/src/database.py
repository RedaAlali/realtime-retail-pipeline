import os
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values

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
