#!/usr/bin/env bash
set -e

echo "[spark] Waiting 30 seconds for Kafka and Postgres to be ready..."
sleep 30

# Choose spark-submit
SPARK_SUBMIT="$(command -v spark-submit || true)"
if [ -z "$SPARK_SUBMIT" ] && [ -x /opt/spark/bin/spark-submit ]; then
  SPARK_SUBMIT="/opt/spark/bin/spark-submit"
fi
if [ -z "$SPARK_SUBMIT" ] && [ -x /opt/bitnami/spark/bin/spark-submit ]; then
  SPARK_SUBMIT="/opt/bitnami/spark/bin/spark-submit"
fi

if [ -z "$SPARK_SUBMIT" ]; then
  echo "ERROR: spark-submit not found"
  exit 1
fi

# Choose Python (some Spark images provide python3 but not python)
PYTHON_BIN="$(command -v python3 || true)"
if [ -z "$PYTHON_BIN" ]; then
  PYTHON_BIN="$(command -v python || true)"
fi
if [ -z "$PYTHON_BIN" ]; then
  echo "ERROR: python/python3 not found in Spark container"
  exit 1
fi

# --- Permanent fix: install psycopg2-binary for Postgres UPSERT writes ---
# Spark image does not ship with psycopg2; install it at container startup.
# Use a writable HOME so pip --user works inside the container.
export HOME=/tmp
export PIP_DISABLE_PIP_VERSION_CHECK=1
echo "[spark] Installing Python dependency: psycopg2-binary (user site) using $PYTHON_BIN ..."
"$PYTHON_BIN" -m pip install --no-cache-dir --user psycopg2-binary

# Make user site-packages visible to the Spark driver + Python workers
USER_SITE="$("$PYTHON_BIN" -c 'import site; print(site.getusersitepackages())')"
export PYTHONPATH="${USER_SITE}:${PYTHONPATH:-}"
# ----------------------------------------------------------------------

# Fix Ivy cache location (writable)
IVY_DIR=/tmp/.ivy2
mkdir -p "$IVY_DIR"
chmod -R 777 "$IVY_DIR" || true

echo "[spark] Using: $SPARK_SUBMIT"
echo "[spark] Submitting Spark Structured Streaming job..."

exec "$SPARK_SUBMIT" \
  --master local[2] \
  --conf spark.jars.ivy="$IVY_DIR" \
  --conf spark.driverEnv.PYTHONPATH="$PYTHONPATH" \
  --conf spark.executorEnv.PYTHONPATH="$PYTHONPATH" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.2 \
  /opt/app/spark/streaming_job.py
