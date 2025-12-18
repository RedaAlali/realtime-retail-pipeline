# Static Data Files (`db/data/`)

Contains static batch data files that are loaded into the database on startup.

## Files

| File | Description | Target Table |
|------|-------------|--------------|
| `product_catalog.csv` | Product catalog with 6 retail products | `dim_products` |
| `store_locations.csv` | Store information with 3 locations | `dim_stores` |

---

## product_catalog.csv (Batch Source #1)

The product catalog defines all products in the retail system:

```csv
product_id,product_name,category,base_price
LAPTOP-001,Laptop Pro 15,Electronics,1299.99
PHONE-001,Smartphone X,Electronics,899.99
HEADPHONES-001,Wireless Headphones,Accessories,199.99
KEYBOARD-001,Mechanical KB,Accessories,149.99
MOUSE-001,Gaming Mouse,Accessories,79.99
MONITOR-001,27" 4K Monitor,Electronics,549.99
```

### Fields

| Field | Description |
|-------|-------------|
| `product_id` | Unique product identifier |
| `product_name` | Human-readable product name |
| `category` | Product category (Electronics or Accessories) |
| `base_price` | Suggested retail price |

---

## store_locations.csv (Batch Source #2)

Store information for geographic analysis:

```csv
store_id,store_name,city,region,size_sqft
STORE-001,Downtown Mall,Riyadh,Central,5000
STORE-002,Westside Plaza,Jeddah,Western,3500
STORE-003,Tech Hub,Dammam,Eastern,4200
```

### Fields

| Field | Description |
|-------|-------------|
| `store_id` | Unique store identifier (matches transactions) |
| `store_name` | Store display name |
| `city` | City where store is located |
| `region` | Geographic region (Central, Western, Eastern) |
| `size_sqft` | Store size in square feet |

---

## How They're Used

1. PostgreSQL container starts
2. `02_seed_dim_products.sql` loads product_catalog.csv → `dim_products`
3. `03_seed_dim_stores.sql` loads store_locations.csv → `dim_stores`
4. Producer generates transactions referencing these IDs
5. Dashboard joins transactions with both dimension tables for analytics

## Data Source Summary

This project demonstrates **4 data sources**:

| Type | Source | Description |
|------|--------|-------------|
| **Batch #1** | `product_catalog.csv` | Product dimension table |
| **Batch #2** | `store_locations.csv` | Store dimension table |
| **Streaming #1** | Kafka `transactions` topic | Real-time sales events |
| **Streaming #2** | Kafka `refunds` topic | Real-time refund events |
