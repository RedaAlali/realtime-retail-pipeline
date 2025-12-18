-- 01_schema.sql
-- Create tables for dimension, facts, metrics, and ML outputs.

CREATE TABLE IF NOT EXISTS dim_products (
    product_id      VARCHAR PRIMARY KEY,
    product_name    VARCHAR NOT NULL,
    category        VARCHAR NOT NULL,
    base_price      NUMERIC(10, 2) NOT NULL
);

-- Store dimension table (batch data source #2)
CREATE TABLE IF NOT EXISTS dim_stores (
    store_id        VARCHAR PRIMARY KEY,
    store_name      VARCHAR NOT NULL,
    city            VARCHAR NOT NULL,
    region          VARCHAR NOT NULL,
    size_sqft       INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id  BIGINT,
    product_id      VARCHAR NOT NULL REFERENCES dim_products(product_id),
    store_id        VARCHAR REFERENCES dim_stores(store_id),
    customer_id     VARCHAR,
    amount          NUMERIC(10, 2),
    ts              TIMESTAMP WITHOUT TIME ZONE,
    PRIMARY KEY (transaction_id, ts)
);

-- Refunds table (streaming data source #2)
CREATE TABLE IF NOT EXISTS refunds (
    refund_id                   BIGINT,
    original_transaction_id     BIGINT,
    product_id                  VARCHAR NOT NULL REFERENCES dim_products(product_id),
    store_id                    VARCHAR REFERENCES dim_stores(store_id),
    customer_id                 VARCHAR,
    refund_amount               NUMERIC(10, 2),
    reason                      VARCHAR(50),
    ts                          TIMESTAMP WITHOUT TIME ZONE,
    PRIMARY KEY (refund_id, ts)
);

CREATE TABLE IF NOT EXISTS product_metrics_minute (
    window_start        TIMESTAMP WITHOUT TIME ZONE,
    window_end          TIMESTAMP WITHOUT TIME ZONE,
    product_id          VARCHAR NOT NULL REFERENCES dim_products(product_id),
    total_revenue       NUMERIC(12, 2),
    transaction_count   BIGINT,
    PRIMARY KEY (window_start, product_id)
);

-- Customer Segmentation (RFM + K-Means)
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

-- Product Associations (Market Basket Analysis)
CREATE TABLE IF NOT EXISTS product_associations (
    antecedent      VARCHAR NOT NULL,
    consequent      VARCHAR NOT NULL,
    support         NUMERIC(10, 6),
    confidence      NUMERIC(10, 6),
    lift            NUMERIC(10, 4),
    updated_at      TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (antecedent, consequent)
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_transactions_ts
    ON transactions(ts);

CREATE INDEX IF NOT EXISTS idx_transactions_customer
    ON transactions(customer_id);

CREATE INDEX IF NOT EXISTS idx_transactions_store
    ON transactions(store_id);

CREATE INDEX IF NOT EXISTS idx_refunds_ts
    ON refunds(ts);

CREATE INDEX IF NOT EXISTS idx_refunds_reason
    ON refunds(reason);

CREATE INDEX IF NOT EXISTS idx_metrics_product_ts
    ON product_metrics_minute(product_id, window_start);

CREATE INDEX IF NOT EXISTS idx_segments_cluster
    ON customer_segments(cluster_id);

CREATE INDEX IF NOT EXISTS idx_associations_lift
    ON product_associations(lift DESC);
