# ML Service (`app/ml_service/`)

Customer segmentation and product recommendation service.

## Files

| File | Description |
|------|-------------|
| `ml_service.py` | RFM analysis, K-Means, Apriori |
| `Dockerfile` | Container configuration |
| `requirements.txt` | Python dependencies |

## Algorithms

### Customer Segmentation (RFM + K-Means)
- **Recency:** Days since last purchase
- **Frequency:** Number of purchases  
- **Monetary:** Total spend
- Segments: Champions, Loyal, At Risk, etc.

### Product Recommendations (Apriori)
- Market basket analysis
- Association rules with support, confidence, lift

## Output Tables

| Table | Description |
|-------|-------------|
| `customer_segments` | RFM scores + cluster IDs |
| `product_associations` | Product pairs + metrics |

## Environment Variables

| Variable | Default |
|----------|---------|
| `ML_POLL_SECONDS` | 30 |
| `ML_N_CLUSTERS` | 4 |
