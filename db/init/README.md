# Database Initialization Scripts (`db/init/`)

SQL scripts that run automatically when the PostgreSQL container starts for the first time.

## Files

| File | Description |
|------|-------------|
| `01_schema.sql` | Creates all database tables and indexes |
| `02_seed_dim_products.sql` | Loads product catalog from CSV |
| `03_seed_dim_stores.sql` | Loads store locations from CSV |

## Execution Order

Scripts run in **alphabetical order**:
1. `01_schema.sql` - Creates schema
2. `02_seed_dim_products.sql` - Seeds products
3. `03_seed_dim_stores.sql` - Seeds stores

## 01_schema.sql

Creates the following tables:

### Dimension Tables (Batch)
- **`dim_products`** - Product catalog (6 products)
  - Loaded from: `product_catalog.csv`
  
- **`dim_stores`** - Store locations (3 stores)
  - Loaded from: `store_locations.csv`

### Fact Tables (Streaming)
- **`transactions`** - Raw transaction events
  - PK: (transaction_id, ts)
  - FK: product_id → dim_products, store_id → dim_stores
  
- **`refunds`** - Raw refund events
  - PK: (refund_id, ts)
  - FK: product_id → dim_products, store_id → dim_stores
  
- **`product_metrics_minute`** - Aggregated metrics per product per minute
  - PK: (window_start, product_id)

### ML Output Tables
- **`customer_segments`** - RFM scores and K-Means clusters
  - PK: customer_id
  
- **`product_associations`** - Association rules from Apriori
  - PK: (antecedent, consequent)

### Indexes
| Index | Table | Purpose |
|-------|-------|---------|
| `idx_transactions_ts` | transactions | Time-based queries |
| `idx_transactions_customer` | transactions | Customer lookups |
| `idx_transactions_store` | transactions | Store-based queries |
| `idx_refunds_ts` | refunds | Time-based queries |
| `idx_refunds_reason` | refunds | Reason-based aggregations |
| `idx_metrics_product_ts` | product_metrics_minute | Metrics queries |
| `idx_segments_cluster` | customer_segments | Cluster lookups |
| `idx_associations_lift` | product_associations | Sort by lift |

## Seed Scripts

### 02_seed_dim_products.sql
Loads product catalog:
```sql
COPY dim_products FROM '/docker-entrypoint-initdb.d/data/product_catalog.csv' WITH CSV HEADER;
```

### 03_seed_dim_stores.sql
Loads store locations:
```sql
COPY dim_stores FROM '/docker-entrypoint-initdb.d/data/store_locations.csv' WITH CSV HEADER;
```

## Interactions with Other Components

```
db/init/*.sql → PostgreSQL → Spark, ML Service, Dashboard
db/data/*.csv
```

## Modifying the Schema

1. Edit `01_schema.sql`
2. Delete the existing database volume:
   ```bash
   docker compose down -v
   ```
3. Restart:
   ```bash
   docker compose up
   ```

