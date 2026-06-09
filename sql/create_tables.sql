-- Medallion Architecture Table Definitions
-- Replace `my_catalog` and `my_schema` with your Unity Catalog names.

-- ---------------------------------------------------------------------------
-- Bronze Layer — raw ingested data, schema mirrors the source CSV
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS my_catalog.my_schema.bronze_orders (
    order_id      STRING,
    customer_id   STRING,
    customer_name STRING,
    product_id    STRING,
    product_name  STRING,
    category      STRING,
    quantity      STRING,   -- kept as STRING to preserve raw values
    unit_price    STRING,
    order_date    STRING,
    status        STRING,
    region        STRING,
    _ingested_at  TIMESTAMP,
    _source_file  STRING
)
USING DELTA
COMMENT 'Raw orders data ingested from source files without transformation.';

-- ---------------------------------------------------------------------------
-- Silver Layer — cleaned and validated data
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS my_catalog.my_schema.silver_orders (
    order_id      STRING        NOT NULL,
    customer_id   STRING        NOT NULL,
    customer_name STRING,
    product_id    STRING        NOT NULL,
    product_name  STRING,
    category      STRING,
    quantity      INT,
    unit_price    DECIMAL(10,2),
    order_date    DATE,
    status        STRING,
    region        STRING,
    order_total   DECIMAL(10,2),
    _ingested_at  TIMESTAMP,
    _processed_at TIMESTAMP
)
USING DELTA
COMMENT 'Cleaned and validated orders with correct data types and derived fields.';

-- ---------------------------------------------------------------------------
-- Gold Layer — aggregated, analytics-ready data
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS my_catalog.my_schema.gold_sales_summary (
    summary_date    DATE          NOT NULL,
    region          STRING        NOT NULL,
    category        STRING        NOT NULL,
    total_orders    BIGINT,
    total_quantity  BIGINT,
    total_revenue   DECIMAL(14,2),
    avg_order_value DECIMAL(10,2),
    _processed_at   TIMESTAMP
)
USING DELTA
COMMENT 'Daily sales summary aggregated by region and product category.';

CREATE TABLE IF NOT EXISTS my_catalog.my_schema.gold_customer_summary (
    customer_id       STRING        NOT NULL,
    customer_name     STRING,
    total_orders      BIGINT,
    completed_orders  BIGINT,
    total_spend       DECIMAL(14,2),
    avg_order_value   DECIMAL(10,2),
    first_order_date  DATE,
    last_order_date   DATE,
    _processed_at     TIMESTAMP
)
USING DELTA
COMMENT 'Lifetime customer metrics aggregated from silver orders.';
