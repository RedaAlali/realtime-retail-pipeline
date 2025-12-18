# Dashboard Service (`app/dashboard/`)

A real-time analytics dashboard built with Streamlit that visualizes retail transaction data and ML insights.

## Files

| File | Description |
|------|-------------|
| `app.py` | Main Streamlit application with all dashboard logic |
| `Dockerfile` | Container configuration for the dashboard service |
| `requirements.txt` | Python dependencies (streamlit, pandas, altair, psycopg2) |

## Features

### KPIs (Top Row)
- **Gross Revenue**: Total sales amount
- **Refunds**: Total refund amount with count
- **Net Revenue**: Gross - Refunds
- **Transactions**: Total transaction count
- **Customers**: Number of segmented customers

### Sales Analytics
- **Stacked Area Chart**: Revenue by product over time
- **Category Pie Chart**: Revenue breakdown by category (Electronics, Accessories)
- **Top Products Bar Chart**: Best-selling products ranked by revenue
- **Cumulative Revenue Line**: Total revenue growth trajectory
- **Transaction Volume**: Number of transactions per time window

### Refunds Analytics
- **Refunds by Reason Chart**: Horizontal bar chart of refund amounts
- **Refund Statistics Table**: Count and amount by reason
- *Data source: Kafka refunds topic (streaming)*

### Store Performance
- **Revenue by Store Location**: Bar chart colored by region
- *Data source: store_locations.csv (batch)*

### Customer Segmentation (from ML Service)
- **Segment Pie Chart**: Distribution of customer segments
- **K-Means Scatter Plot**: Customer clusters by recency vs spend

### Product Recommendations (from ML Service)
- **Association Rules Table**: Product pairs frequently bought together
- **Lift Scores**: How much more likely products are bought together

## How It Works

1. Connects to PostgreSQL database
2. Queries data from multiple tables:
   - `product_metrics_minute` - aggregated sales data
   - `refunds` - refund events with reasons
   - `dim_stores` - store information (batch)
   - `customer_segments` - ML segmentation results
   - `product_associations` - ML recommendation results
   - `transactions` - raw transaction data
3. Renders interactive charts using Altair
4. Auto-refreshes data every 10 seconds

## Interactions with Other Components

```
PostgreSQL ←──reads── Dashboard
     ↑
     │
Spark writes: transactions, refunds, metrics
ML Service writes: segments, associations
Batch seeds: dim_products, dim_stores
```

## Running Locally

The dashboard runs on port **8501**:

```bash
# Via Docker
docker compose up dashboard

# Access at
http://localhost:8501
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_HOST` | postgres | Database host |
| `POSTGRES_PORT` | 5432 | Database port |
| `POSTGRES_DB` | retaildb | Database name |
| `POSTGRES_USER` | postgres | Database user |
| `POSTGRES_PASSWORD` | postgres | Database password |
