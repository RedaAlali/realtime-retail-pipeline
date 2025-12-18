-- 02_seed_dim_products.sql
-- Load dim_products from CSV (batch data source).

COPY dim_products (product_id, product_name, category, base_price)
FROM '/docker-entrypoint-initdb.d/data/product_catalog.csv'
WITH (FORMAT csv, HEADER true);
