# Static Data (`db/data/`)

Batch data files loaded into PostgreSQL on startup.

## Files

| File | Target Table |
|------|--------------|
| `product_catalog.csv` | `dim_products` |
| `store_locations.csv` | `dim_stores` |

## product_catalog.csv

```csv
product_id,product_name,category,base_price
LAPTOP-001,Laptop Pro 15,Electronics,1299.99
PHONE-001,Smartphone X,Electronics,899.99
HEADPHONES-001,Wireless Headphones,Accessories,199.99
KEYBOARD-001,Mechanical KB,Accessories,149.99
MOUSE-001,Gaming Mouse,Accessories,79.99
MONITOR-001,27" 4K Monitor,Electronics,549.99
```

## store_locations.csv

```csv
store_id,store_name,city,region,size_sqft
STORE-001,Downtown Mall,Riyadh,Central,5000
STORE-002,Westside Plaza,Jeddah,Western,3500
STORE-003,Tech Hub,Dammam,Eastern,4200
```
