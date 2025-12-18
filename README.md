# Real-time Retail Analytics Pipeline

A real-time retail analytics platform using **Apache Kafka**, **Spark Structured Streaming**, **PostgreSQL**, and **Streamlit**. Features ML-powered customer segmentation (RFM + K-Means) and product recommendations (Apriori).

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Batch: product_catalog.csv, store_locations.csv → PostgreSQL
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│  Producer → Kafka (transactions, refunds) → Spark Streaming │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│  PostgreSQL ← ML Service (RFM, K-Means, Apriori)            │
│       ↓                                                      │
│  Streamlit Dashboard (KPIs, Charts, ML Insights)            │
└─────────────────────────────────────────────────────────────┘
```

## Data Sources

| Type | Source | Description |
|------|--------|-------------|
| Batch | `product_catalog.csv` | 6 products |
| Batch | `store_locations.csv` | 3 stores |
| Stream | Kafka `transactions` | Real-time sales |
| Stream | Kafka `refunds` | ~10% of sales |

## Tech Stack

- **Kafka 3.7.1** + Zookeeper — Event streaming
- **Spark 3.5.1** — Structured Streaming with 3 concurrent queries
- **PostgreSQL 15** — Star schema storage
- **Streamlit** — Real-time dashboard
- **scikit-learn / mlxtend** — ML (RFM, K-Means, Apriori)
- **Docker Compose** — Container orchestration

## Quick Start

### Prerequisites

- Docker Desktop installed and running
- `docker compose` command available

### Running the Project

**macOS / Linux:**
```bash
# Build containers
docker compose build

# Make Spark script executable (first time only)
chmod +x app/spark/submit.sh

# Start all services
docker compose up
```

**Windows (PowerShell):**
```powershell
# Build containers
docker compose build

# Start all services
docker compose up
```

### Access

- **Dashboard:** http://localhost:8501
- **PostgreSQL:** `localhost:5433` (user: `postgres`, db: `retaildb`)

## Database Schema

| Table | Type | Description |
|-------|------|-------------|
| `dim_products` | Dimension | Product catalog |
| `dim_stores` | Dimension | Store locations |
| `transactions` | Fact | Sales events |
| `refunds` | Fact | Refund events |
| `product_metrics_minute` | Aggregate | 1-min windowed metrics |
| `customer_segments` | ML Output | RFM + K-Means results |
| `product_associations` | ML Output | Apriori rules |

## Features

### Spark Streaming
- 3 concurrent queries (transactions, aggregations, refunds)
- 1-minute tumbling windows with 5-minute watermark
- Checkpointing for exactly-once semantics

### ML Service
- **Customer Segmentation:** RFM scoring + K-Means clustering
- **Product Recommendations:** Apriori association rules

### Dashboard
- Real-time KPIs (Revenue, Refunds, Transactions)
- Sales analytics by product, category, time
- Refund analysis by reason
- Store performance by region
- Customer segment visualization
- Product recommendation rules

## Project Structure

```
├── app/
│   ├── dashboard/      # Streamlit app
│   ├── ml_service/     # RFM, K-Means, Apriori
│   ├── producer/       # Kafka event generator
│   └── spark/          # Streaming job
├── db/
│   ├── data/           # CSV seed files
│   └── init/           # SQL schema & seeds
├── docker-compose.yml
└── README.md
```

## Sample Queries

```sql
-- Recent transactions
SELECT * FROM transactions ORDER BY ts DESC LIMIT 10;

-- Windowed metrics
SELECT * FROM product_metrics_minute ORDER BY window_start DESC LIMIT 10;

-- Customer segments
SELECT rfm_segment, COUNT(*) FROM customer_segments GROUP BY rfm_segment;

-- Product associations
SELECT * FROM product_associations ORDER BY lift DESC;
```