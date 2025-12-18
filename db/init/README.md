# Init Scripts (`db/init/`)

SQL scripts executed on PostgreSQL container startup (alphabetical order).

## Files

| File | Purpose |
|------|---------|
| `01_schema.sql` | Create tables and indexes |
| `02_seed_dim_products.sql` | Load product catalog |
| `03_seed_dim_stores.sql` | Load store locations |

## Tables Created

**Dimensions:** `dim_products`, `dim_stores`  
**Facts:** `transactions`, `refunds`, `product_metrics_minute`  
**ML Output:** `customer_segments`, `product_associations`

## Reset Database

```bash
docker compose down -v
docker compose up
```
