-- =============================================================================
-- star_schema.sql
-- Retail Demand Forecasting — Data Warehouse Star Schema
-- =============================================================================
-- NOTE: This file is mounted as a PostgreSQL init script.
--       It runs inside the POSTGRES_DB database (airflow).
--       The warehouse schema is created here — no separate DB needed.
--
-- Schema layout:
--              dim_time ─────────────────────┐
--   dim_product ──────────── fact_sales ─────┤
--              dim_store ────────────────────┘
-- =============================================================================

-- Create the warehouse schema inside the airflow database
CREATE SCHEMA IF NOT EXISTS warehouse;
SET search_path = warehouse;

-- ─── DIMENSION: TIME ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_time (
    time_key          SERIAL PRIMARY KEY,
    full_date         DATE        NOT NULL UNIQUE,
    day_of_week       SMALLINT    NOT NULL,
    day_name          VARCHAR(10) NOT NULL,
    day_of_month      SMALLINT    NOT NULL,
    day_of_year       SMALLINT    NOT NULL,
    week_of_year      SMALLINT    NOT NULL,
    month_num         SMALLINT    NOT NULL,
    month_name        VARCHAR(10) NOT NULL,
    quarter           SMALLINT    NOT NULL,
    year              SMALLINT    NOT NULL,
    is_weekend        BOOLEAN     NOT NULL DEFAULT FALSE,
    is_holiday        BOOLEAN     NOT NULL DEFAULT FALSE,
    holiday_name      VARCHAR(100),
    is_month_start    BOOLEAN     NOT NULL DEFAULT FALSE,
    is_month_end      BOOLEAN     NOT NULL DEFAULT FALSE,
    fiscal_year       SMALLINT,
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
    d::DATE,
    EXTRACT(ISODOW FROM d)::SMALLINT,
    TO_CHAR(d, 'Day'),
    EXTRACT(DAY     FROM d)::SMALLINT,
    EXTRACT(DOY     FROM d)::SMALLINT,
    EXTRACT(WEEK    FROM d)::SMALLINT,
    EXTRACT(MONTH   FROM d)::SMALLINT,
    TO_CHAR(d, 'Month'),
    EXTRACT(QUARTER FROM d)::SMALLINT,
    EXTRACT(YEAR    FROM d)::SMALLINT,
    EXTRACT(ISODOW  FROM d) IN (6, 7),
    EXTRACT(DAY FROM d) = 1,
    d = (DATE_TRUNC('MONTH', d) + INTERVAL '1 month - 1 day')
FROM generate_series(
    '2020-01-01'::DATE,
    '2030-12-31'::DATE,
    '1 day'::INTERVAL
) AS t(d)
ON CONFLICT (full_date) DO NOTHING;

-- ─── DIMENSION: PRODUCT ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_product (
    product_key        SERIAL PRIMARY KEY,
    product_id         VARCHAR(20)  NOT NULL UNIQUE,
    product_name       VARCHAR(255) NOT NULL,
    category           VARCHAR(100),
    sub_category       VARCHAR(100),
    brand              VARCHAR(100),
    unit_price         NUMERIC(10,2),
    cost_price         NUMERIC(10,2),
    weight_kg          NUMERIC(8,3),
    is_perishable      BOOLEAN DEFAULT FALSE,
    is_active          BOOLEAN DEFAULT TRUE,
    effective_from     DATE NOT NULL DEFAULT CURRENT_DATE,
    effective_to       DATE,
    is_current_record  BOOLEAN DEFAULT TRUE,
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_product_id       ON dim_product(product_id);
CREATE INDEX IF NOT EXISTS idx_dim_product_category ON dim_product(category);

-- ─── DIMENSION: STORE ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_store (
    store_key       SERIAL PRIMARY KEY,
    store_id        VARCHAR(20)  NOT NULL UNIQUE,
    store_name      VARCHAR(255) NOT NULL,
    store_type      VARCHAR(50),
    city            VARCHAR(100),
    state           VARCHAR(50),
    state_code      VARCHAR(5),
    region          VARCHAR(50),
    zip_code        VARCHAR(10),
    latitude        NUMERIC(9,6),
    longitude       NUMERIC(9,6),
    floor_area_sqft INTEGER,
    opening_date    DATE,
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_store_id     ON dim_store(store_id);
CREATE INDEX IF NOT EXISTS idx_dim_store_region ON dim_store(region);

-- ─── FACT TABLE: SALES ────────────────────────────────────────────────────────
-- NOTE: Using simple (non-partitioned) table for Docker compatibility.
--       For production, enable table partitioning separately.
CREATE TABLE IF NOT EXISTS fact_sales (
    sales_key        BIGSERIAL PRIMARY KEY,
    time_key         INT    NOT NULL REFERENCES dim_time(time_key),
    product_key      INT    NOT NULL REFERENCES dim_product(product_key),
    store_key        INT    NOT NULL REFERENCES dim_store(store_key),
    transaction_id   VARCHAR(50) NOT NULL,
    customer_id      VARCHAR(20),
    payment_method   VARCHAR(20),
    channel          VARCHAR(20) DEFAULT 'store',
    quantity_sold    INT           NOT NULL,
    unit_price       NUMERIC(10,2) NOT NULL,
    discount_amount  NUMERIC(10,2) DEFAULT 0,
    gross_revenue    NUMERIC(12,2) NOT NULL,
    net_revenue      NUMERIC(12,2) NOT NULL,
    cost_of_goods    NUMERIC(12,2),
    gross_profit     NUMERIC(12,2),
    source_system    VARCHAR(30) DEFAULT 'pos',
    load_timestamp   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id         VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_fact_sales_time    ON fact_sales(time_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product ON fact_sales(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_store   ON fact_sales(store_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_txn     ON fact_sales(transaction_id);

-- ─── AGGREGATION: DAILY DEMAND ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS agg_daily_demand (
    agg_key          BIGSERIAL PRIMARY KEY,
    agg_date         DATE        NOT NULL,
    product_id       VARCHAR(20) NOT NULL,
    store_id         VARCHAR(20) NOT NULL,
    category         VARCHAR(100),
    region           VARCHAR(50),
    total_quantity   INT           NOT NULL,
    total_revenue    NUMERIC(14,2) NOT NULL,
    total_orders     INT           NOT NULL,
    avg_order_value  NUMERIC(10,2),
    discount_rate    NUMERIC(5,4),
    temp_high_f      NUMERIC(5,1),
    precipitation_in NUMERIC(5,2),
    is_holiday       BOOLEAN DEFAULT FALSE,
    holiday_name     VARCHAR(100),
    day_of_week      SMALLINT,
    week_of_year     SMALLINT,
    month_num        SMALLINT,
    quarter          SMALLINT,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (agg_date, product_id, store_id)
);

CREATE INDEX IF NOT EXISTS idx_agg_daily_date    ON agg_daily_demand(agg_date);
CREATE INDEX IF NOT EXISTS idx_agg_daily_product ON agg_daily_demand(product_id);
CREATE INDEX IF NOT EXISTS idx_agg_daily_store   ON agg_daily_demand(store_id);

-- ─── AGGREGATION: WEEKLY DEMAND ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS agg_weekly_demand (
    agg_key        BIGSERIAL PRIMARY KEY,
    year           SMALLINT    NOT NULL,
    week_of_year   SMALLINT    NOT NULL,
    product_id     VARCHAR(20) NOT NULL,
    store_id       VARCHAR(20) NOT NULL,
    category       VARCHAR(100),
    region         VARCHAR(50),
    total_quantity INT           NOT NULL,
    total_revenue  NUMERIC(14,2) NOT NULL,
    total_orders   INT           NOT NULL,
    avg_daily_qty  NUMERIC(10,2),
    prev_week_qty  INT,
    wow_change_pct NUMERIC(8,4),
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (year, week_of_year, product_id, store_id)
);

-- ─── STREAMING EVENTS ─────────────────────────────────────────────────────────
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

CREATE INDEX IF NOT EXISTS idx_stream_event_ts   ON stream_ecommerce_events(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_stream_event_type ON stream_ecommerce_events(event_type);
CREATE INDEX IF NOT EXISTS idx_stream_product    ON stream_ecommerce_events(product_id);

-- ─── DATA QUALITY LOG ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dq_check_log (
    log_id          BIGSERIAL PRIMARY KEY,
    check_name      VARCHAR(100) NOT NULL,
    table_name      VARCHAR(100) NOT NULL,
    column_name     VARCHAR(100),
    check_type      VARCHAR(50),
    total_records   BIGINT,
    passed_records  BIGINT,
    failed_records  BIGINT,
    pass_rate       NUMERIC(5,4),
    status          VARCHAR(10),
    threshold       NUMERIC(5,4),
    run_timestamp   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    pipeline_run_id VARCHAR(100),
    details         TEXT
);