# Database Configuration (`db/`)

This directory contains all database-related files for PostgreSQL initialization and seed data.

## Directory Structure

```
db/
├── init/                         # SQL scripts run on container startup
│   ├── 01_schema.sql             # Table definitions and indexes
│   ├── 02_seed_dim_products.sql  # Product catalog seed
│   └── 03_seed_dim_stores.sql    # Store locations seed
└── data/                         # Static data files
    ├── product_catalog.csv       # Product catalog (batch source #1)
    └── store_locations.csv       # Store locations (batch source #2)
```

## How It Works

When the PostgreSQL container starts, it automatically runs all `.sql` files in the `init/` directory in alphabetical order:

1. `01_schema.sql` - Creates all database tables and indexes
2. `02_seed_dim_products.sql` - Loads the product catalog
3. `03_seed_dim_stores.sql` - Loads the store locations

This is handled by the official PostgreSQL Docker image's entrypoint.

## Tables Created

| Table | Purpose | Populated By |
|-------|---------|--------------|
| `dim_products` | Product catalog (6 products) | Seed script (batch) |
| `dim_stores` | Store locations (3 stores) | Seed script (batch) |
| `transactions` | Raw transaction events | Spark (streaming) |
| `refunds` | Raw refund events | Spark (streaming) |
| `product_metrics_minute` | Aggregated metrics | Spark (streaming) |
| `customer_segments` | RFM + K-Means results | ML Service |
| `product_associations` | Association rules | ML Service |

## Interactions with Other Components

```
PostgreSQL Container
    ↑
    │ init scripts run on startup
    │
db/init/*.sql + db/data/*.csv

All services read/write to PostgreSQL:
- Spark writes transactions, refunds & metrics
- ML Service reads transactions, writes segments & associations
- Dashboard reads all tables for visualization
```

## Connection Details

| Setting | Value |
|---------|-------|
| Host | localhost (external) or postgres (internal) |
| Port | 5433 (external) or 5432 (internal) |
| Database | retaildb |
| User | postgres |
| Password | postgres |

## Manual Access

```bash
# Connect from host machine
psql -h localhost -p 5433 -U postgres -d retaildb

# Or via Docker
docker exec -it postgres psql -U postgres -d retaildb
```

