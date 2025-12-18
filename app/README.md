# Application Components (`app/`)

This directory contains all the application microservices that make up the real-time retail analytics pipeline.

## Directory Structure

```
app/
├── dashboard/     # Streamlit web dashboard for visualization
├── ml_service/    # Machine learning service for analytics
├── producer/      # Kafka message producer (transaction simulator)
└── spark/         # Spark Structured Streaming job
```

## Component Overview

| Component | Purpose | Technology |
|-----------|---------|------------|
| **dashboard** | Real-time analytics dashboard | Streamlit + Altair |
| **ml_service** | Customer segmentation & product recommendations | scikit-learn + mlxtend |
| **producer** | Simulates transactions + refunds | Python + kafka-python |
| **spark** | Multi-stream processing (3 queries) | PySpark Structured Streaming |

## Data Flow

```
producer ──┬──> Kafka (transactions) ──┬──> Spark ──> PostgreSQL
           │                           │                  │
           └──> Kafka (refunds) ───────┘                  │
                                                          ↓
                                            ┌─────────────┴─────────────┐
                                            │                           │
                                            ▼                           ▼
                                       ML Service                  Dashboard
```

1. **Producer** generates transactions and refunds (~10% refund rate) to 2 Kafka topics
2. **Spark** runs 3 streaming queries: transactions, metrics, and refunds
3. **ML Service** reads from PostgreSQL, runs ML algorithms, writes results back
4. **Dashboard** reads all data from PostgreSQL and displays visualizations

## How to Run

All components are containerized. From the project root:

```bash
docker compose up
```

Each subdirectory contains its own README with component-specific details.
