# Real-time Retail Analytics Pipeline

A modern, containerized, real-time retail analytics platform that simulates high-volume retail transactions and refunds, processes them via event streaming and microservice pipelines, analyzes them using machine learning models, and visualizes KPIs in a Streamlit dashboard.

Built using **Apache Kafka**, **Apache Spark Structured Streaming**, **PostgreSQL**, and **Streamlit**.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Features](#features)
- [Project Directory Structure](#project-directory-structure)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation & Quick Start](#installation--quick-start)
  - [Accessing the Dashboard](#accessing-the-dashboard)
- [Usage & Sample Queries](#usage--sample-queries)
- [Contributing](#contributing)
- [License](#license)

---

## Project Overview

This repository demonstrates an end-to-end real-time data engineering and machine learning pipeline. It handles both streaming transaction feeds (Kafka + Spark) and analytical batches (PostgreSQL) to power live metrics, customer RFM segmentation, and product recommendation features.

## Architecture

For a deep dive into schemas, tables, and exact data-flow mechanics, check out our [docs/architecture.md](file:///c:/Users/redab/Downloads/CV%20Projects/realtime-retail-pipeline/docs/architecture.md).

```text
┌─────────────────────────────────────────────────────────────┐
│  Batch: product_catalog.csv, store_locations.csv → PostgreSQL
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│  Producer → Kafka (transactions, refunds) → Spark Streaming │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│  PostgreSQL ← ML Service (RFM, K-Means, Apriori)            │
│       ↓                                                      │
│  Streamlit Dashboard (KPIs, Charts, ML Insights)            │
└─────────────────────────────────────────────────────────────┘
```

## Features

###  Real-time Data Streaming

- **Mock Transaction & Refund Generator**: A Python producer generates continuous transaction payloads and simulated refunds (approx. 10% rate) to independent Kafka topics.
- **Spark Structured Streaming**: PySpark jobs consume from Kafka, parse and validate JSON schemas, handle watermark-supported late events (5-minute watermark), and write transaction logs to PostgreSQL.
- **1-Minute Aggregations**: Spark groups and sums transactional revenue metrics into rolling 1-minute tumbling windows.

###  Machine Learning Engine

- **Customer RFM Segmentation**: Calculates Recency (days since purchase), Frequency (purchase volume), and Monetary (total spend) metrics to classify shoppers (e.g., Champions, Loyal, Hibernating).
- **K-Means Clustering**: Clusters customers based on scaled RFM vectors into `N` behavioral segments.
- **Apriori Association Mining**: Analyzes transaction baskets to identify product pairs frequently bought together, deriving confidence and lift numbers to generate recommendations.

###  Streamlit Frontend Dashboard

- **KPI Metrics**: Real-time Gross Revenue, Refunds, Net Revenue, Transactions, and Customer counts.
- **Visual Analytics**: Interactive area and pie charts representing sales over time, category percentages, store performance, and transaction volumes.
- **ML Visualization**: Pie charts representing RFM segments and scatter plots indicating K-Means cluster groupings.

---

## Project Directory Structure

Following software engineering best practices, this repository separates infrastructure configurations, microservice codes, database schemas, and documentation:

```text
realtime-retail-pipeline/
├── .github/                       # GitHub templates
│   ├── ISSUE_TEMPLATE/
│   │   ├── bug_report.md
│   │   └── feature_request.md
│   └── PULL_REQUEST_TEMPLATE.md
├── docs/                          # In-depth architectural documentation
│   └── architecture.md
├── services/                      # Microservices source directory
│   ├── dashboard/                 # Streamlit UI service
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── src/ (app.py, components.py, utils.py)
│   ├── ml_service/                # Machine learning daemon
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── src/ (main.py, algorithms.py, database.py)
│   ├── producer/                  # Kafka event simulator
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── src/ (main.py, generator.py)
│   └── spark/                     # PySpark structured streaming job
│       ├── submit.sh
│       └── src/ (streaming_job.py)
├── db/                            # PostgreSQL warehouse setup
│   ├── data/                      # Batch seed files (.csv)
│   └── init/                      # Schema definition and seeding SQL scripts
├── .env.example                   # Template environment variables
├── .gitignore                     # Tech-stack specific gitignore configuration
├── docker-compose.yml             # Local orchestrator compose file
├── LICENSE                        # Project MIT License
├── CONTRIBUTING.md                # Development guides for collaborators
└── README.md                      # Project documentation (this file)
```

---

## Getting Started

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running.
- `docker compose` command line tools configured.

### Installation & Quick Start

1. **Clone the Repository**:

   ```bash
   git clone https://github.com/RedaAlali/realtime-retail-pipeline.git
   cd realtime-retail-pipeline
   ```

2. **Configure Environment Variables**:
   Create a local `.env` file using the configuration template:

   ```bash
   cp .env.example .env
   ```

3. **Build the Containers**:

   ```bash
   docker compose build
   ```

4. **Launch All Services**:

   ```bash
   docker compose up
   ```

To run services in the background, append the `-d` flag: `docker compose up -d`.

### Accessing the Dashboard

Once the docker containers are active:

- **Streamlit Dashboard**: [http://localhost:8501](http://localhost:8501)
- **PostgreSQL Database**: `localhost:5433` (Username: `postgres`, Password: `postgres`, Database: `retaildb`)

---

## Usage & Sample Queries

You can connect directly to PostgreSQL on port `5433` to inspect the warehouse. Here are some useful SQL queries to analyze the data state:

```sql
-- Retrieve the latest 10 transactions recorded by Spark
SELECT * FROM transactions ORDER BY ts DESC LIMIT 10;

-- Query the 1-minute windowed metrics
SELECT * FROM product_metrics_minute ORDER BY window_start DESC LIMIT 10;

-- Display customer segment sizes
SELECT rfm_segment, COUNT(*) FROM customer_segments GROUP BY rfm_segment;

-- Retrieve high-lift product recommendations
SELECT * FROM product_associations ORDER BY lift DESC LIMIT 10;
```

---

## Contributing

We welcome contributions! Please review our [CONTRIBUTING.md](file:///c:/Users/redab/Downloads/CV%20Projects/realtime-retail-pipeline/CONTRIBUTING.md) guide before submitting pull requests.

## License

Distributed under the MIT License. See [LICENSE](file:///c:/Users/redab/Downloads/CV%20Projects/realtime-retail-pipeline/LICENSE) for more details.

