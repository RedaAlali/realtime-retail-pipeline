# Real-time Retail Analytics & ML Insights Pipeline

This project implements a real-time retail analytics pipeline using:

- **Kafka + Zookeeper** for event streaming (2 topics: transactions + refunds)
- **Spark Structured Streaming** for real-time multi-stream processing
- **PostgreSQL** as an operational and analytics store
- **Python producer** to simulate POS transactions and refunds
- **ML service** for customer segmentation and product recommendations
- **Streamlit dashboard** for real-time visualization and analytics

## Data Sources (4 Total)

| Type | Source | Description |
|------|--------|-------------|
| **Batch #1** | `product_catalog.csv` | Product dimension table (6 products) |
| **Batch #2** | `store_locations.csv` | Store dimension table (3 stores) |
| **Streaming #1** | Kafka `transactions` topic | Real-time sales events |
| **Streaming #2** | Kafka `refunds` topic | Real-time refund events (~10% of sales) |

All components run locally via Docker Compose.

---

## 1. High-level architecture

```text
     ┌─────────────────────────────────────────────────────────────┐
     │                    BATCH DATA SOURCES                       │
     │  product_catalog.csv          store_locations.csv           │
     │         │                            │                      │
     │         └────────────┬───────────────┘                      │
     │                      ▼                                      │
     │            +-------------------+                            │
     │            |   DB Init Seeds   |                            │
     │            +-------------------+                            │
     └─────────────────────────────────────────────────────────────┘
                            │
                            ▼
                   +------------------+
                   |   PostgreSQL     |
                   |  ┌────────────┐  |
                   |  │dim_products│  |
                   |  │dim_stores  │  |
                   |  │transactions│  |
                   |  │refunds     │  |
                   |  │metrics     │  |
                   |  │segments    │  |
                   |  │associations│  |
                   |  └────────────┘  |
                   +--------▲---------+
                            │
     ┌──────────────────────┼──────────────────────┐
     │                      │                      │
     │    STREAMING DATA SOURCES                   │
     │                      │                      │
     │  +----------------+  │  +----------------+  │
     │  |   Producer     |  │  |   Spark        |  │
     │  | (transactions  |  │  | Structured     |  │
     │  |  + refunds)    |  │  | Streaming      |  │
     │  +-------┬--------+  │  | (3 queries)    |  │
     │          │           │  +--------┬-------+  │
     │          ▼           │           │          │
     │  +----------------+  │           │          │
     │  |  Kafka Broker  |──┼───────────┘          │
     │  | transactions   |  │                      │
     │  | refunds        |  │                      │
     │  +----------------+  │                      │
     └──────────────────────┴──────────────────────┘
                            │
              ┌─────────────┼─────────────┐
              │             │             │
              ▼             ▼             ▼
     +-------------+  +------------+  +-----------+
     | ML Service  |  | Dashboard  |  | PostgreSQL|
     | - RFM       |  | - KPIs     |  | (queries) |
     | - K-Means   |  | - Charts   |  |           |
     | - Apriori   |  | - Tables   |  |           |
     +-------------+  +------------+  +-----------+
```

### Component roles

- **Kafka + Zookeeper**
  - Durable event log for incoming retail transactions and refunds.
  - Two topics: `transactions` and `refunds`.

- **Python Producer**
  - Simulates point-of-sale (POS) transactions.
  - Publishes to both Kafka topics (~10% of transactions have matching refunds).

- **Spark Structured Streaming**
  - Runs **3 concurrent streaming queries**:
    1. Raw transactions → `transactions` table
    2. 1-minute windowed aggregations → `product_metrics_minute`
    3. Raw refunds → `refunds` table

- **PostgreSQL**
  - Holds dimension tables (batch-loaded) and fact tables (streaming):
    - `dim_products` - Product catalog (batch)
    - `dim_stores` - Store locations (batch)
    - `transactions` - Sales events (streaming)
    - `refunds` - Refund events (streaming)
    - `product_metrics_minute` - Aggregated metrics
    - `customer_segments` - RFM + K-Means output
    - `product_associations` - Market basket analysis

- **ML Service (Python)**
  - **Customer Segmentation**: RFM scores + K-Means clustering
  - **Product Recommendations**: Apriori association rules

- **Streamlit Dashboard**
  - Real-time KPIs: Gross Revenue, Refunds, Net Revenue, Transactions, Customers
  - Sales analytics with multiple chart types
  - Refunds analytics by reason
  - Store performance by region
  - Customer segmentation visualizations
  - Product recommendation rules

---

## 2. Data flow and schema

### 2.1 Event schemas (Kafka topics)

**Transaction Event** (`transactions` topic):

```json
{
  "transaction_id": 12345,
  "product_id": "LAPTOP-001",
  "store_id": "STORE-001",
  "customer_id": "CUST-0042",
  "amount": 1299.99,
  "timestamp": "2025-01-01 12:34:56"
}
```

**Refund Event** (`refunds` topic):

```json
{
  "refund_id": 1001,
  "original_transaction_id": 12345,
  "product_id": "LAPTOP-001",
  "store_id": "STORE-001",
  "customer_id": "CUST-0042",
  "refund_amount": 1299.99,
  "reason": "defective",
  "timestamp": "2025-01-01 14:00:00"
}
```

Refund reasons: `defective`, `wrong_item`, `not_as_described`, `changed_mind`, `damaged_shipping`

### 2.2 Database tables

**`dim_products`** (batch source #1)

| Column | Type | Description |
|--------|------|-------------|
| `product_id` | VARCHAR (PK) | Product identifier |
| `product_name` | VARCHAR | Display name |
| `category` | VARCHAR | Electronics or Accessories |
| `base_price` | NUMERIC | Suggested retail price |

Loaded from `db/data/product_catalog.csv` at container startup.

---

**`dim_stores`** (batch source #2)

| Column | Type | Description |
|--------|------|-------------|
| `store_id` | VARCHAR (PK) | Store identifier |
| `store_name` | VARCHAR | Store display name |
| `city` | VARCHAR | City (Riyadh, Jeddah, Dammam) |
| `region` | VARCHAR | Region (Central, Western, Eastern) |
| `size_sqft` | INTEGER | Store size in square feet |

Loaded from `db/data/store_locations.csv` at container startup.

---

**`transactions`** (streaming source #1)

| Column | Type | Description |
|--------|------|-------------|
| `transaction_id` | BIGINT | Transaction ID |
| `product_id` | VARCHAR (FK) | References dim_products |
| `store_id` | VARCHAR (FK) | References dim_stores |
| `customer_id` | VARCHAR | Customer identifier |
| `amount` | NUMERIC | Transaction amount |
| `ts` | TIMESTAMP | Event timestamp |

PK: `(transaction_id, ts)`. Populated by Spark from Kafka.

---

**`refunds`** (streaming source #2)

| Column | Type | Description |
|--------|------|-------------|
| `refund_id` | BIGINT | Refund ID |
| `original_transaction_id` | BIGINT | Original transaction |
| `product_id` | VARCHAR (FK) | References dim_products |
| `store_id` | VARCHAR (FK) | References dim_stores |
| `customer_id` | VARCHAR | Customer identifier |
| `refund_amount` | NUMERIC | Amount refunded |
| `reason` | VARCHAR | Refund reason |
| `ts` | TIMESTAMP | Refund timestamp |

PK: `(refund_id, ts)`. Populated by Spark from Kafka.

---

**`product_metrics_minute`** (aggregated metrics)

| Column | Type | Description |
|--------|------|-------------|
| `window_start` | TIMESTAMP | Window start time |
| `window_end` | TIMESTAMP | Window end time |
| `product_id` | VARCHAR (FK) | Product ID |
| `total_revenue` | NUMERIC | Sum of revenue in window |
| `transaction_count` | BIGINT | Number of transactions |

PK: `(window_start, product_id)`. 1-minute tumbling windows.

---

**`customer_segments`** (ML output)

| Column | Description |
|--------|-------------|
| `customer_id` | Customer ID (PK) |
| `recency_days` | Days since last purchase |
| `frequency` | Number of purchases |
| `monetary` | Total spend |
| `r_score`, `f_score`, `m_score` | RFM scores (1-5) |
| `rfm_segment` | Segment label |
| `cluster_id` | K-Means cluster |

---

**`product_associations`** (ML output)

| Column | Description |
|--------|-------------|
| `antecedent` | Product(s) bought |
| `consequent` | Product(s) likely also bought |
| `support` | % of transactions with both |
| `confidence` | P(consequent \| antecedent) |
| `lift` | Association strength (>1 = positive) |

---

## 3. Running the project locally

### 3.1 Prerequisites

- macOS with Apple Silicon (M1) or later.
- Docker Desktop installed and running.
- `docker compose` command available.

No local Python or Spark installation is required; everything runs in containers.

---

### 3.2 Steps

From the project root:

1. **Build containers**

   ```bash
   docker compose build
   ```

2. **Make the Spark submit script executable (once)**

   ```bash
   chmod +x app/spark/submit.sh
   ```

3. **Start the full stack**

   ```bash
   docker compose up
   ```

   - Zookeeper, Kafka, Postgres, Spark, producer, ML service, and dashboard will start.
   - Spark waits ~30 seconds and then submits the streaming job.
   - The producer starts sending transactions to Kafka.

4. **Open the dashboard**

   Visit:

   ```text
   http://localhost:8501
   ```

   Within a minute or two you should see:

   - KPIs updating (revenue, transaction counts).
   - Time-series chart of revenue by product.
   - Anomaly table populated as the ML service finds spikes/drops.
   - Recent raw transactions with product names/categories from the dimension table.

5. **Connect to Postgres (optional)**

   ```bash
   psql -h localhost -p 5433 -U postgres -d retaildb
   ```

   Example queries:

   ```sql
   SELECT * FROM dim_products;
   SELECT * FROM transactions ORDER BY ts DESC LIMIT 10;
   SELECT * FROM product_metrics_minute ORDER BY window_start DESC LIMIT 10;
   SELECT * FROM anomalies ORDER BY created_at DESC LIMIT 10;
   ```

---

## 4. Spark Structured Streaming logic

Spark job (`app/spark/streaming_job.py`) implements:

1. **Kafka source**

   ```python
   raw_df = (
       spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", kafka_bootstrap)
       .option("subscribe", kafka_topic)
       .option("startingOffsets", "earliest")
       .load()
   )
   ```

2. **JSON parsing & typing**

   - JSON payload is parsed using a predefined `StructType`.
   - `timestamp` string is converted to `TimestampType` column `ts`.
   - Simple filters enforce non-null product, amount, and timestamp.

3. **Raw write (append)**

   - Uses `foreachBatch` + JDBC to append each micro-batch to `transactions`.

4. **Windowed aggregations**

   - 1-minute tumbling windows with a 5-minute watermark:

     ```python
     metrics_df = (
         cleaned_df.withWatermark("ts", "5 minutes")
         .groupBy(window(col("ts"), "1 minute").alias("time_window"),
                  col("product_id"))
         .agg(
             _sum("amount").alias("total_revenue"),
             _count("*").alias("transaction_count"),
         )
     )
     ```

   - Writes each batch into `product_metrics_minute` via JDBC.

5. **Checkpointing**

   - Checkpoints for both queries are written under `spark-checkpoints/`, which is volume-mounted so state is preserved across restarts.

This uses Spark’s stateful streaming model and demonstrates modern big data concepts like watermarks and time-window aggregations.

---

## 5. Machine Learning Components

The ML microservice (`app/ml_service/ml_service.py`) implements two advanced ML features:

### 5.1 Customer Segmentation (RFM + K-Means)

**RFM Analysis** calculates three metrics per customer:
- **Recency**: Days since their last purchase (lower = better)
- **Frequency**: Total number of purchases (higher = better)
- **Monetary**: Total amount spent (higher = better)

Each metric is scored 1-5 using quintiles, then customers are assigned to segments:

| Segment | Description | Characteristics |
|---------|-------------|----------------|
| Champions | Best customers | High R, F, M scores |
| Loyal Customers | Frequent buyers | High F and M |
| Potential Loyalists | Recent with moderate activity | High R, moderate F |
| New Customers | Just started buying | High R, low F |
| At Risk | Were good, now inactive | Low R, high F |
| Need Attention | Middle ground | Mixed scores |
| Hibernating | Inactive, low value | Low R, F, M |

**K-Means Clustering** (scikit-learn) provides an additional unsupervised grouping based on RFM features.

### 5.2 Product Recommendations (Apriori Algorithm)

**Market Basket Analysis** finds products frequently purchased together:

1. Groups transactions by customer to form "baskets"
2. Uses **Apriori algorithm** (mlxtend) to find frequent itemsets
3. Generates **association rules** with metrics:
   - **Support**: How often the product pair appears together
   - **Confidence**: P(consequent | antecedent)
   - **Lift**: How much more likely than random (>1 = positive association)

Example output: *"Customers who buy LAPTOP-001 are 2.5x more likely to also buy MOUSE-001"*

---

## 6. Streamlit dashboard

The dashboard (`app/dashboard/app.py`) provides comprehensive analytics:

### KPIs (Top Row)
- **Gross Revenue**: Total sales amount
- **Refunds**: Total refund amount with count
- **Net Revenue**: Gross - Refunds
- **Transactions**: Total transaction count
- **Customers**: Number of segmented customers

### Sales Analytics Section
- **Revenue by Product Over Time**: Stacked area chart showing revenue flow
- **Revenue by Category**: Pie chart (Electronics vs Accessories)
- **Top Products**: Horizontal bar chart of best-selling products
- **Cumulative Revenue**: Line chart showing growth trajectory
- **Transaction Volume**: Bar chart of transaction counts over time

### Refunds Analytics Section
- **Refunds by Reason**: Horizontal bar chart showing refund amounts by reason
- **Refund Statistics**: Table with reason, count, and amount

### Store Performance Section
- **Revenue by Store Location**: Bar chart colored by region
- Uses batch data from `store_locations.csv` to enrich transaction data

### Customer Segmentation Section
- **Segment Distribution**: Pie chart of customer segments
- **K-Means Clusters**: Scatter plot (Recency vs Spend)
- **Segment Statistics**: Summary table

### Product Recommendations Section
- **Association Rules Table**: Product pairs with support, confidence, lift

### Recent Transactions
- Last 50 transactions with product and store details

---