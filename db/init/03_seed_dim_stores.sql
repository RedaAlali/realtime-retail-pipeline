-- 03_seed_dim_stores.sql
-- Load dim_stores from CSV (batch data source #2).

COPY dim_stores (store_id, store_name, city, region, size_sqft)
FROM '/docker-entrypoint-initdb.d/data/store_locations.csv'
WITH (FORMAT csv, HEADER true);
