# Database (`db/`)

PostgreSQL initialization and seed data.

## Structure

```
db/
├── init/           # SQL scripts (run on startup)
│   ├── 01_schema.sql
│   ├── 02_seed_dim_products.sql
│   └── 03_seed_dim_stores.sql
└── data/           # CSV seed files
    ├── product_catalog.csv
    └── store_locations.csv
```

## Tables

| Table | Type | Populated By |
|-------|------|--------------|
| `dim_products` | Dimension | Seed script |
| `dim_stores` | Dimension | Seed script |
| `transactions` | Fact | Spark |
| `refunds` | Fact | Spark |
| `product_metrics_minute` | Aggregate | Spark |
| `customer_segments` | ML Output | ML Service |
| `product_associations` | ML Output | ML Service |

## Connection

| Setting | Value |
|---------|-------|
| Host | localhost:5433 |
| Database | retaildb |
| User | postgres |

## Access

```bash
psql -h localhost -p 5433 -U postgres -d retaildb
```
