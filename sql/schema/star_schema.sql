-- =============================================================================
-- star_schema.sql
-- Retail Demand Forecasting — Data Warehouse Star Schema
-- =============================================================================
-- Schema:
--
--              dim_time ─────────────────────┐
--                                            │
--   dim_product ──────────── fact_sales ─────┤
--                                            │
--              dim_store ────────────────────┘
--
-- =============================================================================

-- ─── DATABASE SETUP ──────────────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS retail_dw;
\c retail_dw;

CREATE SCHEMA IF NOT EXISTS warehouse;
SET search_path = warehouse;

-- ─── DIMENSION: TIME ─────────────────────────────────────────────────────────
-- Pre-populated date dimension for fast temporal joins & filtering
CREATE TABLE IF NOT EXISTS dim_time (
    time_key          SERIAL PRIMARY KEY,
    full_date         DATE        NOT NULL UNIQUE,
    day_of_week       SMALLINT    NOT NULL,           -- 1=Mon … 7=Sun
    day_name          VARCHAR(10) NOT NULL,
    day_of_month      SMALLINT    NOT NULL,
    day_of_year       SMALLINT    NOT NULL,
    week_of_year      SMALLINT    NOT NULL,
    month_num         SMALLINT    NOT NULL,
    month_name        VARCHAR(10) NOT NULL,
    quarter           SMALLINT    NOT NULL,           -- 1-4
    year              SMALLINT    NOT NULL,
    is_weekend        BOOLEAN     NOT NULL DEFAULT FALSE,
    is_holiday        BOOLEAN     NOT NULL DEFAULT FALSE,
    holiday_name      VARCHAR(100),
    is_month_start    BOOLEAN     NOT NULL DEFAULT FALSE,
    is_month_end      BOOLEAN     NOT NULL DEFAULT FALSE,
    fiscal_year       SMALLINT,                       -- optional: set per business calendar
    fiscal_quarter    SMALLINT,
    created_at        TIMESTAMP   DEFAULT CURRENT_TIMESTAMP
);

-- Populate dim_time for 2020-2030
INSERT INTO dim_time (
    full_date, day_of_week, day_name, day_of_month, day_of_year,
    week_of_year, month_num, month_name, quarter, year,
    is_weekend, is_month_start, is_month_end
)
SELECT
    d::DATE                                                     AS full_date,
    EXTRACT(ISODOW FROM d)::SMALLINT                           AS day_of_week,
    TO_CHAR(d, 'Day')                                          AS day_name,
    EXTRACT(DAY   FROM d)::SMALLINT                            AS day_of_month,
    EXTRACT(DOY   FROM d)::SMALLINT                            AS day_of_year,
    EXTRACT(WEEK  FROM d)::SMALLINT                            AS week_of_year,
    EXTRACT(MONTH FROM d)::SMALLINT                            AS month_num,
    TO_CHAR(d, 'Month')                                        AS month_name,
    EXTRACT(QUARTER FROM d)::SMALLINT                          AS quarter,
    EXTRACT(YEAR  FROM d)::SMALLINT                            AS year,
    EXTRACT(ISODOW FROM d) IN (6, 7)                           AS is_weekend,
    EXTRACT(DAY FROM d) = 1                                    AS is_month_start,
    d = (DATE_TRUNC('MONTH', d) + INTERVAL '1 month - 1 day') AS is_month_end
FROM generate_series('2020-01-01'::DATE, '2030-12-31'::DATE, '1 day'::INTERVAL) AS t(d)
ON CONFLICT (full_date) DO NOTHING;

-- ─── DIMENSION: PRODUCT ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_product (
    product_key        SERIAL PRIMARY KEY,
    product_id         VARCHAR(20) NOT NULL UNIQUE,  -- natural key from source
    product_name       VARCHAR(255) NOT NULL,
    category           VARCHAR(100),
    sub_category       VARCHAR(100),
    brand              VARCHAR(100),
    unit_price         NUMERIC(10,2),
    cost_price         NUMERIC(10,2),
    weight_kg          NUMERIC(8,3),
    is_perishable      BOOLEAN DEFAULT FALSE,
    is_active          BOOLEAN DEFAULT TRUE,
    -- SCD Type 2 columns
    effective_from     DATE NOT NULL DEFAULT CURRENT_DATE,
    effective_to       DATE,
    is_current_record  BOOLEAN DEFAULT TRUE,
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_product_id ON dim_product(product_id);
CREATE INDEX idx_dim_product_category ON dim_product(category);

-- ─── DIMENSION: STORE ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_store (
    store_key      SERIAL PRIMARY KEY,
    store_id       VARCHAR(20) NOT NULL UNIQUE,      -- natural key from source
    store_name     VARCHAR(255) NOT NULL,
    store_type     VARCHAR(50),                      -- supermarket, express, online
    city           VARCHAR(100),
    state          VARCHAR(50),
    state_code     VARCHAR(5),
    region         VARCHAR(50),
    zip_code       VARCHAR(10),
    latitude       NUMERIC(9,6),
    longitude      NUMERIC(9,6),
    floor_area_sqft INTEGER,
    opening_date   DATE,
    is_active      BOOLEAN DEFAULT TRUE,
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_store_id ON dim_store(store_id);
CREATE INDEX idx_dim_store_region ON dim_store(region);

-- ─── FACT TABLE: SALES ────────────────────────────────────────────────────────
-- Grain: one row per product per transaction
-- Partitioned by transaction year+month for performance
CREATE TABLE IF NOT EXISTS fact_sales (
    sales_key        BIGSERIAL,
    -- Foreign keys to dimensions
    time_key         INT    NOT NULL REFERENCES dim_time(time_key),
    product_key      INT    NOT NULL REFERENCES dim_product(product_key),
    store_key        INT    NOT NULL REFERENCES dim_store(store_key),
    -- Degenerate dimensions (no separate table needed)
    transaction_id   VARCHAR(50) NOT NULL,
    customer_id      VARCHAR(20),                   -- NULL for anonymous
    payment_method   VARCHAR(20),
    channel          VARCHAR(20) DEFAULT 'store',   -- store | online
    -- Measures (additive facts)
    quantity_sold    INT          NOT NULL,
    unit_price       NUMERIC(10,2) NOT NULL,
    discount_amount  NUMERIC(10,2) DEFAULT 0,
    gross_revenue    NUMERIC(12,2) NOT NULL,
    net_revenue      NUMERIC(12,2) NOT NULL,        -- after discount
    cost_of_goods    NUMERIC(12,2),
    gross_profit     NUMERIC(12,2),
    -- Metadata
    source_system    VARCHAR(30) DEFAULT 'pos',     -- pos | ecommerce
    load_timestamp   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id         VARCHAR(50),                   -- Airflow run_id

    PRIMARY KEY (sales_key, time_key)               -- composite for partitioning
) PARTITION BY RANGE (time_key);

-- Create monthly partitions (2023)
CREATE TABLE fact_sales_2023_q1 PARTITION OF fact_sales
    FOR VALUES FROM (1) TO (92);   -- time_keys for Q1 2023
CREATE TABLE fact_sales_2023_q2 PARTITION OF fact_sales
    FOR VALUES FROM (92) TO (183);
CREATE TABLE fact_sales_2023_q3 PARTITION OF fact_sales
    FOR VALUES FROM (183) TO (275);
CREATE TABLE fact_sales_2023_q4 PARTITION OF fact_sales
    FOR VALUES FROM (275) TO (366);

-- Indexes on fact table
CREATE INDEX idx_fact_sales_time    ON fact_sales(time_key);
CREATE INDEX idx_fact_sales_product ON fact_sales(product_key);
CREATE INDEX idx_fact_sales_store   ON fact_sales(store_key);
CREATE INDEX idx_fact_sales_txn     ON fact_sales(transaction_id);

-- ─── AGGREGATION TABLES ───────────────────────────────────────────────────────
-- Pre-computed for dashboard performance
CREATE TABLE IF NOT EXISTS agg_daily_demand (
    agg_key          BIGSERIAL PRIMARY KEY,
    agg_date         DATE    NOT NULL,
    product_id       VARCHAR(20) NOT NULL,
    store_id         VARCHAR(20) NOT NULL,
    category         VARCHAR(100),
    region           VARCHAR(50),
    total_quantity   INT     NOT NULL,
    total_revenue    NUMERIC(14,2) NOT NULL,
    total_orders     INT     NOT NULL,
    avg_order_value  NUMERIC(10,2),
    discount_rate    NUMERIC(5,4),
    -- Weather enrichment (joined from external)
    temp_high_f      NUMERIC(5,1),
    precipitation_in NUMERIC(5,2),
    is_holiday       BOOLEAN DEFAULT FALSE,
    holiday_name     VARCHAR(100),
    -- ML features
    day_of_week      SMALLINT,
    week_of_year     SMALLINT,
    month_num        SMALLINT,
    quarter          SMALLINT,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (agg_date, product_id, store_id)
);

CREATE INDEX idx_agg_daily_date    ON agg_daily_demand(agg_date);
CREATE INDEX idx_agg_daily_product ON agg_daily_demand(product_id);
CREATE INDEX idx_agg_daily_store   ON agg_daily_demand(store_id);

CREATE TABLE IF NOT EXISTS agg_weekly_demand (
    agg_key          BIGSERIAL PRIMARY KEY,
    year             SMALLINT NOT NULL,
    week_of_year     SMALLINT NOT NULL,
    product_id       VARCHAR(20) NOT NULL,
    store_id         VARCHAR(20) NOT NULL,
    category         VARCHAR(100),
    region           VARCHAR(50),
    total_quantity   INT     NOT NULL,
    total_revenue    NUMERIC(14,2) NOT NULL,
    total_orders     INT     NOT NULL,
    avg_daily_qty    NUMERIC(10,2),
    -- Week-over-week comparison
    prev_week_qty    INT,
    wow_change_pct   NUMERIC(8,4),
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (year, week_of_year, product_id, store_id)
);

-- ─── STREAMING EVENTS TABLE ───────────────────────────────────────────────────
-- Stores near-real-time e-commerce events from Kafka consumer
CREATE TABLE IF NOT EXISTS stream_ecommerce_events (
    event_key        BIGSERIAL PRIMARY KEY,
    event_id         VARCHAR(50) NOT NULL UNIQUE,
    event_type       VARCHAR(30) NOT NULL,
    event_timestamp  TIMESTAMP  NOT NULL,
    session_id       VARCHAR(50),
    user_id          VARCHAR(20),
    product_id       VARCHAR(20),
    category         VARCHAR(100),
    price            NUMERIC(10,2),
    quantity         INT,
    device           VARCHAR(20),
    platform         VARCHAR(20),
    referrer         VARCHAR(50),
    state            VARCHAR(5),
    load_timestamp   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_stream_event_ts   ON stream_ecommerce_events(event_timestamp);
CREATE INDEX idx_stream_event_type ON stream_ecommerce_events(event_type);
CREATE INDEX idx_stream_product    ON stream_ecommerce_events(product_id);

-- ─── DATA QUALITY LOG ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dq_check_log (
    log_id          BIGSERIAL PRIMARY KEY,
    check_name      VARCHAR(100) NOT NULL,
    table_name      VARCHAR(100) NOT NULL,
    column_name     VARCHAR(100),
    check_type      VARCHAR(50),          -- null_check, duplicate_check, range_check, etc.
    total_records   BIGINT,
    passed_records  BIGINT,
    failed_records  BIGINT,
    pass_rate       NUMERIC(5,4),
    status          VARCHAR(10),          -- PASS | WARN | FAIL
    threshold       NUMERIC(5,4),         -- e.g., 0.99 = 99% must pass
    run_timestamp   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    pipeline_run_id VARCHAR(100),
    details         TEXT
);
