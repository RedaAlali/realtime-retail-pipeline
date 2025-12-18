# Application Components (`app/`)

Microservices for the real-time retail analytics pipeline.

## Structure

| Component | Purpose | Tech |
|-----------|---------|------|
| `dashboard/` | Real-time analytics UI | Streamlit + Altair |
| `ml_service/` | Customer segmentation & recommendations | scikit-learn + mlxtend |
| `producer/` | Transaction & refund simulator | Python + kafka-python |
| `spark/` | Multi-stream processing (3 queries) | PySpark Structured Streaming |

## Data Flow

```
producer → Kafka (transactions, refunds) → Spark → PostgreSQL → ML Service + Dashboard
```

## Run

```bash
docker compose up
```
