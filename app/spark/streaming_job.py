import os
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Permanent fix goals:
# 1) Make DB writes idempotent (no duplicate-key crashes on restart/replay)
# 2) Keep checkpoints so Spark does NOT replay old offsets unnecessarily
# 3) Be tolerant to multiple timestamp string formats

# ---------- Env / config ----------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC_TRANSACTIONS = os.getenv("KAFKA_TOPIC", "transactions")
KAFKA_TOPIC_REFUNDS = os.getenv("KAFKA_TOPIC_REFUNDS", "refunds")
KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "latest")
CHECKPOINT_BASE = os.getenv("SPARK_CHECKPOINT_BASE", "/opt/spark-checkpoints")

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "retaildb")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")

try:
    import psycopg2
    from psycopg2.extras import execute_values
except Exception as e:
    raise RuntimeError(
        "Missing dependency psycopg2-binary inside the Spark container."
    ) from e


def pg_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def parse_ts(col: F.Column) -> F.Column:
    """Parse common timestamp formats into Spark TimestampType."""
    return F.coalesce(
        F.to_timestamp(col),
        F.to_timestamp(col, "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(col, "yyyy-MM-dd HH:mm:ss.SSS"),
        F.to_timestamp(col, "yyyy-MM-dd'T'HH:mm:ss"),
        F.to_timestamp(col, "yyyy-MM-dd'T'HH:mm:ss.SSS"),
        F.to_timestamp(col, "yyyy-MM-dd'T'HH:mm:ssXXX"),
    )


def upsert_transactions(batch_df, batch_id: int):
    batch_df = batch_df.select(
        F.col("transaction_id").cast("long"),
        F.col("product_id").cast("string"),
        F.col("store_id").cast("string"),
        F.col("customer_id").cast("string"),
        F.col("amount").cast("double"),
        F.col("ts").cast("timestamp"),
    ).where(F.col("transaction_id").isNotNull() & F.col("product_id").isNotNull() & F.col("ts").isNotNull())

    if batch_df.rdd.isEmpty():
        return

    rows = [(int(r[0]), r[1], r[2], r[3], float(r[4]) if r[4] else None, r[5]) 
            for r in batch_df.toLocalIterator()]

    if not rows:
        return

    sql = """
        INSERT INTO transactions (transaction_id, product_id, store_id, customer_id, amount, ts)
        VALUES %s
        ON CONFLICT (transaction_id, ts) DO NOTHING
    """

    with pg_conn() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=1000)
        conn.commit()


def upsert_refunds(batch_df, batch_id: int):
    """Upsert refunds from the refunds Kafka topic."""
    batch_df = batch_df.select(
        F.col("refund_id").cast("long"),
        F.col("original_transaction_id").cast("long"),
        F.col("product_id").cast("string"),
        F.col("store_id").cast("string"),
        F.col("customer_id").cast("string"),
        F.col("refund_amount").cast("double"),
        F.col("reason").cast("string"),
        F.col("ts").cast("timestamp"),
    ).where(F.col("refund_id").isNotNull() & F.col("product_id").isNotNull() & F.col("ts").isNotNull())

    if batch_df.rdd.isEmpty():
        return

    rows = [(int(r[0]), int(r[1]) if r[1] else None, r[2], r[3], r[4], 
             float(r[5]) if r[5] else None, r[6], r[7]) 
            for r in batch_df.toLocalIterator()]

    if not rows:
        return

    sql = """
        INSERT INTO refunds (refund_id, original_transaction_id, product_id, store_id, customer_id, refund_amount, reason, ts)
        VALUES %s
        ON CONFLICT (refund_id, ts) DO NOTHING
    """

    with pg_conn() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=1000)
        conn.commit()


def upsert_metrics(batch_df, batch_id: int):
    batch_df = batch_df.select(
        F.col("window_start").cast("timestamp"),
        F.col("window_end").cast("timestamp"),
        F.col("product_id").cast("string"),
        F.col("total_revenue").cast("double"),
        F.col("transaction_count").cast("long"),
    ).where(F.col("window_start").isNotNull() & F.col("product_id").isNotNull())

    if batch_df.rdd.isEmpty():
        return

    rows = [(r[0], r[1], r[2], float(r[3]) if r[3] else 0.0, int(r[4]) if r[4] else 0) 
            for r in batch_df.toLocalIterator()]

    if not rows:
        return

    sql = """
        INSERT INTO product_metrics_minute (window_start, window_end, product_id, total_revenue, transaction_count)
        VALUES %s
        ON CONFLICT (window_start, product_id)
        DO UPDATE SET
            window_end = EXCLUDED.window_end,
            total_revenue = EXCLUDED.total_revenue,
            transaction_count = EXCLUDED.transaction_count
    """

    with pg_conn() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=1000)
        conn.commit()


def main():
    spark = (
        SparkSession.builder
        .appName("RetailStreaming")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # ---------- Transactions Schema ----------
    tx_schema = T.StructType([
        T.StructField("transaction_id", T.LongType(), True),
        T.StructField("product_id", T.StringType(), True),
        T.StructField("store_id", T.StringType(), True),
        T.StructField("customer_id", T.StringType(), True),
        T.StructField("amount", T.DoubleType(), True),
        T.StructField("timestamp", T.StringType(), True),
    ])

    # ---------- Refunds Schema ----------
    refund_schema = T.StructType([
        T.StructField("refund_id", T.LongType(), True),
        T.StructField("original_transaction_id", T.LongType(), True),
        T.StructField("product_id", T.StringType(), True),
        T.StructField("store_id", T.StringType(), True),
        T.StructField("customer_id", T.StringType(), True),
        T.StructField("refund_amount", T.DoubleType(), True),
        T.StructField("reason", T.StringType(), True),
        T.StructField("timestamp", T.StringType(), True),
    ])

    # ========== STREAM 1: Transactions ==========
    raw_tx = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC_TRANSACTIONS)
        .option("startingOffsets", KAFKA_STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_tx = (
        raw_tx.select(F.col("value").cast("string").alias("json"))
        .select(F.from_json("json", tx_schema).alias("data"))
        .select("data.*")
        .withColumn("ts", parse_ts(F.col("timestamp")))
        .drop("timestamp")
    )

    # Transaction sink
    tx_checkpoint = os.path.join(CHECKPOINT_BASE, "transactions_sink")
    tx_query = (
        parsed_tx.writeStream
        .foreachBatch(upsert_transactions)
        .option("checkpointLocation", tx_checkpoint)
        .outputMode("update")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # Per-minute metrics sink
    metrics = (
        parsed_tx
        .withWatermark("ts", "2 minutes")
        .groupBy(
            F.window("ts", "1 minute").alias("w"),
            F.col("product_id"),
        )
        .agg(
            F.sum("amount").alias("total_revenue"),
            F.count("transaction_id").alias("transaction_count"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "product_id",
            "total_revenue",
            "transaction_count",
        )
    )

    metrics_checkpoint = os.path.join(CHECKPOINT_BASE, "metrics_sink")
    metrics_query = (
        metrics.writeStream
        .foreachBatch(upsert_metrics)
        .option("checkpointLocation", metrics_checkpoint)
        .outputMode("update")
        .trigger(processingTime="15 seconds")
        .start()
    )

    # ========== STREAM 2: Refunds ==========
    raw_refunds = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC_REFUNDS)
        .option("startingOffsets", KAFKA_STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_refunds = (
        raw_refunds.select(F.col("value").cast("string").alias("json"))
        .select(F.from_json("json", refund_schema).alias("data"))
        .select("data.*")
        .withColumn("ts", parse_ts(F.col("timestamp")))
        .drop("timestamp")
    )

    # Refunds sink
    refunds_checkpoint = os.path.join(CHECKPOINT_BASE, "refunds_sink")
    refunds_query = (
        parsed_refunds.writeStream
        .foreachBatch(upsert_refunds)
        .option("checkpointLocation", refunds_checkpoint)
        .outputMode("update")
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("[spark] Started 3 streaming queries:")
    print(f"  1. Transactions -> PostgreSQL")
    print(f"  2. Metrics (1-min aggregations) -> PostgreSQL")
    print(f"  3. Refunds -> PostgreSQL")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()

