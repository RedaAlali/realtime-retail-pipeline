# Dashboard (`app/dashboard/`)

Real-time analytics dashboard built with Streamlit.

## Files

| File | Description |
|------|-------------|
| `app.py` | Main Streamlit application |
| `Dockerfile` | Container configuration |
| `requirements.txt` | Python dependencies |

## Features

- **KPIs:** Gross Revenue, Refunds, Net Revenue, Transactions, Customers
- **Sales Analytics:** Revenue by product/category, top products, growth trends
- **Refunds:** Analysis by reason
- **Store Performance:** Revenue by region
- **ML Insights:** Customer segments, product recommendations

## Access

```
http://localhost:8501
```

## Environment Variables

| Variable | Default |
|----------|---------|
| `POSTGRES_HOST` | postgres |
| `POSTGRES_PORT` | 5432 |
| `POSTGRES_DB` | retaildb |
