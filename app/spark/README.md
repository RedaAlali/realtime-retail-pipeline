# Spark Streaming (`app/spark/`)

PySpark Structured Streaming job processing Kafka events.

## Files

| File | Description |
|------|-------------|
| `streaming_job.py` | Main Spark application |
| `submit.sh` | Job submission script |

## Streaming Queries

| Query | Source | Target |
|-------|--------|--------|
| Transactions | `transactions` topic | `transactions` table |
| Metrics | 1-min windows | `product_metrics_minute` |
| Refunds | `refunds` topic | `refunds` table |

## Key Features

- **Multi-stream:** 2 Kafka topics, 3 concurrent queries
- **Idempotent writes:** `ON CONFLICT DO NOTHING`
- **Checkpointing:** Exactly-once semantics
- **Watermarking:** 2-min late data tolerance

## Environment Variables

| Variable | Default |
|----------|---------|
| `KAFKA_BOOTSTRAP` | kafka:9092 |
| `PG_HOST` | postgres |
| `SPARK_CHECKPOINT_BASE` | /opt/spark-checkpoints |
