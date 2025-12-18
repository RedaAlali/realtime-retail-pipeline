# ML Service (`app/ml_service/`)

A machine learning microservice that provides customer segmentation and product recommendations for the retail pipeline.

## Files

| File | Description |
|------|-------------|
| `ml_service.py` | Main ML service with RFM analysis, K-Means, and Apriori |
| `Dockerfile` | Container configuration for the ML service |
| `requirements.txt` | Python dependencies (pandas, scikit-learn, mlxtend, psycopg2) |

## ML Features

### 1. Customer Segmentation (RFM + K-Means)

**RFM Analysis** calculates three metrics per customer:
- **Recency**: Days since their last purchase
- **Frequency**: Total number of purchases
- **Monetary**: Total amount spent

Customers are scored 1-5 on each metric and assigned to segments:

| Segment | Description |
|---------|-------------|
| Champions | Best customers - high R, F, M |
| Loyal Customers | Frequent buyers - high F and M |
| Potential Loyalists | Recent with moderate activity |
| New Customers | Just started buying |
| At Risk | Were good, now inactive |
| Need Attention | Middle ground |
| Hibernating | Inactive, low value |

**K-Means Clustering** provides additional grouping based on RFM features.

### 2. Product Recommendations (Apriori Algorithm)

Uses **Market Basket Analysis** to find products frequently bought together:

1. Groups transactions by customer to form "baskets"
2. Applies Apriori algorithm to find frequent itemsets
3. Generates association rules with:
   - **Support**: How often the pair appears together
   - **Confidence**: P(consequent | antecedent)
   - **Lift**: Strength of association (>1 = positive)

## How It Works

1. Runs in a continuous loop (every 30 seconds by default)
2. Reads transaction data from PostgreSQL
3. Calculates RFM scores and applies K-Means clustering
4. Runs Apriori algorithm on customer purchase history
5. Writes results to `customer_segments` and `product_associations` tables

## Interactions with Other Components

```
PostgreSQL ←──reads/writes──→ ML Service
    ↑
Spark writes raw transactions
    ↓
Dashboard reads ML results
```

## Database Tables

**Writes to:**
- `customer_segments` - RFM scores, segment labels, cluster IDs
- `product_associations` - product pairs with support, confidence, lift

**Reads from:**
- `transactions` - raw transaction data

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_HOST` | postgres | Database host |
| `POSTGRES_PORT` | 5432 | Database port |
| `POSTGRES_DB` | retaildb | Database name |
| `ML_POLL_SECONDS` | 30 | How often to run ML algorithms |
| `ML_MIN_TRANSACTIONS` | 10 | Minimum transactions needed |
| `ML_N_CLUSTERS` | 4 | Number of K-Means clusters |
