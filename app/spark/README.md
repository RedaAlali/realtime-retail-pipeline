# Spark Streaming Job (`app/spark/`)

A PySpark Structured Streaming job that processes multiple Kafka topics and writes to PostgreSQL.

## Files

| File | Description |
|------|-------------|
| `streaming_job.py` | Main Spark streaming application |
| `submit.sh` | Shell script to submit the Spark job |

## What It Does

The Spark job runs **3 concurrent streaming queries**:

### Query 1: Transaction Ingestion
- Reads JSON events from Kafka topic `transactions`
- Parses and validates the data
- Writes raw transactions to PostgreSQL `transactions` table

### Query 2: Windowed Aggregations
- Groups transactions into 1-minute tumbling windows
- Calculates per-product metrics:
  - `total_revenue` - sum of amounts
  - `transaction_count` - number of transactions
- Writes to PostgreSQL `product_metrics_minute` table

### Query 3: Refund Ingestion
- Reads JSON events from Kafka topic `refunds`
- Parses refund data including reason
- Writes raw refunds to PostgreSQL `refunds` table

## How It Works

```python
# Stream 1: Transactions
raw_tx = spark.readStream.format("kafka")
    .option("subscribe", "transactions")
    .load()
parsed_tx.writeStream.foreachBatch(upsert_transactions)

# Stream 2: Metrics (from transactions)
metrics = parsed_tx.withWatermark("ts", "2 minutes")
    .groupBy(window("ts", "1 minute"), "product_id")
    .agg(sum("amount"), count("*"))
metrics.writeStream.foreachBatch(upsert_metrics)

# Stream 3: Refunds
raw_refunds = spark.readStream.format("kafka")
    .option("subscribe", "refunds")
    .load()
parsed_refunds.writeStream.foreachBatch(upsert_refunds)
```

## Key Features

- **Multi-stream processing**: Consumes from 2 Kafka topics simultaneously
- **Idempotent writes**: Uses `ON CONFLICT DO NOTHING/UPDATE` to handle duplicates
- **Checkpointing**: State preserved in `/opt/spark-checkpoints` for fault tolerance
- **Watermarking**: 2-minute watermark handles late-arriving data

## Interactions with Other Components

```
Kafka (transactions) ──┬──> Spark ──writes──> PostgreSQL
                       │                            │
Kafka (refunds) ───────┘                            ↓
                                         Dashboard & ML Service
```

## Database Tables

**Writes to:**
- `transactions` - raw transaction events
- `refunds` - raw refund events
- `product_metrics_minute` - aggregated metrics per product per minute

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP` | kafka:9092 | Kafka broker address |
| `KAFKA_TOPIC` | transactions | Transactions topic |
| `KAFKA_TOPIC_REFUNDS` | refunds | Refunds topic |
| `PG_HOST` | postgres | PostgreSQL host |
| `PG_PORT` | 5432 | PostgreSQL port |
| `PG_DB` | retaildb | Database name |
| `SPARK_CHECKPOINT_BASE` | /opt/spark-checkpoints | Checkpoint directory |

## Running

The Spark job starts automatically via `docker compose up`. It uses the `spark:3.5.1-python3` Docker image and submits the job via `submit.sh`.

On startup, you'll see:
```
[spark] Started 3 streaming queries:
  1. Transactions -> PostgreSQL
  2. Metrics (1-min aggregations) -> PostgreSQL
  3. Refunds -> PostgreSQL
```

