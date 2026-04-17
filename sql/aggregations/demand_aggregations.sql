-- =============================================================================
-- demand_aggregations.sql
-- SQL queries to compute daily and weekly demand aggregations
-- Run by Airflow after PySpark loads fact_sales
-- =============================================================================

SET search_path = warehouse;

-- ─── 1. POPULATE agg_daily_demand ────────────────────────────────────────────
-- Aggregates fact_sales to product × store × date grain
-- Enriched with dim_time (holiday flags), external weather data

INSERT INTO agg_daily_demand (
    agg_date, product_id, store_id, category, region,
    total_quantity, total_revenue, total_orders, avg_order_value, discount_rate,
    is_holiday, holiday_name, day_of_week, week_of_year, month_num, quarter
)
SELECT
    t.full_date                                         AS agg_date,
    p.product_id,
    s.store_id,
    p.category,
    s.region,
    SUM(fs.quantity_sold)                               AS total_quantity,
    SUM(fs.net_revenue)                                 AS total_revenue,
    COUNT(DISTINCT fs.transaction_id)                   AS total_orders,
    ROUND(SUM(fs.net_revenue) / NULLIF(COUNT(DISTINCT fs.transaction_id), 0), 2)
                                                        AS avg_order_value,
    ROUND(SUM(fs.discount_amount) / NULLIF(SUM(fs.gross_revenue), 0), 4)
                                                        AS discount_rate,
    t.is_holiday,
    t.holiday_name,
    t.day_of_week,
    t.week_of_year,
    t.month_num,
    t.quarter
FROM fact_sales fs
JOIN dim_time    t ON t.time_key    = fs.time_key
JOIN dim_product p ON p.product_key = fs.product_key
JOIN dim_store   s ON s.store_key   = fs.store_key
WHERE
    -- Incremental load: only process yesterday's data
    t.full_date = CURRENT_DATE - INTERVAL '1 day'
GROUP BY
    t.full_date, p.product_id, s.store_id, p.category, s.region,
    t.is_holiday, t.holiday_name, t.day_of_week, t.week_of_year, t.month_num, t.quarter
ON CONFLICT (agg_date, product_id, store_id)
DO UPDATE SET
    total_quantity  = EXCLUDED.total_quantity,
    total_revenue   = EXCLUDED.total_revenue,
    total_orders    = EXCLUDED.total_orders,
    avg_order_value = EXCLUDED.avg_order_value,
    discount_rate   = EXCLUDED.discount_rate,
    created_at      = CURRENT_TIMESTAMP;


-- ─── 2. POPULATE agg_weekly_demand ───────────────────────────────────────────
-- Week-over-week demand with lag comparison for trend detection

INSERT INTO agg_weekly_demand (
    year, week_of_year, product_id, store_id, category, region,
    total_quantity, total_revenue, total_orders, avg_daily_qty,
    prev_week_qty, wow_change_pct
)
WITH weekly_base AS (
    SELECT
        t.year,
        t.week_of_year,
        p.product_id,
        s.store_id,
        p.category,
        s.region,
        SUM(fs.quantity_sold)                   AS total_quantity,
        SUM(fs.net_revenue)                     AS total_revenue,
        COUNT(DISTINCT fs.transaction_id)       AS total_orders,
        ROUND(SUM(fs.quantity_sold)::NUMERIC / 7, 2) AS avg_daily_qty
    FROM fact_sales fs
    JOIN dim_time    t ON t.time_key    = fs.time_key
    JOIN dim_product p ON p.product_key = fs.product_key
    JOIN dim_store   s ON s.store_key   = fs.store_key
    WHERE t.year = EXTRACT(YEAR  FROM CURRENT_DATE)
      AND t.week_of_year = EXTRACT(WEEK FROM CURRENT_DATE) - 1
    GROUP BY t.year, t.week_of_year, p.product_id, s.store_id, p.category, s.region
),
prev_week AS (
    SELECT product_id, store_id, total_quantity AS prev_qty
    FROM agg_weekly_demand
    WHERE year = EXTRACT(YEAR FROM CURRENT_DATE)
      AND week_of_year = EXTRACT(WEEK FROM CURRENT_DATE) - 2
)
SELECT
    wb.year, wb.week_of_year,
    wb.product_id, wb.store_id, wb.category, wb.region,
    wb.total_quantity, wb.total_revenue, wb.total_orders, wb.avg_daily_qty,
    pw.prev_qty,
    ROUND(
        (wb.total_quantity - pw.prev_qty)::NUMERIC / NULLIF(pw.prev_qty, 0) * 100, 4
    )                                           AS wow_change_pct
FROM weekly_base wb
LEFT JOIN prev_week pw USING (product_id, store_id)
ON CONFLICT (year, week_of_year, product_id, store_id)
DO UPDATE SET
    total_quantity  = EXCLUDED.total_quantity,
    total_revenue   = EXCLUDED.total_revenue,
    avg_daily_qty   = EXCLUDED.avg_daily_qty,
    prev_week_qty   = EXCLUDED.prev_week_qty,
    wow_change_pct  = EXCLUDED.wow_change_pct,
    created_at      = CURRENT_TIMESTAMP;


-- ─── 3. ANALYTICAL VIEWS ─────────────────────────────────────────────────────

-- Top products by revenue this month
CREATE OR REPLACE VIEW v_top_products_monthly AS
SELECT
    p.product_id,
    p.product_name,
    p.category,
    t.month_num,
    t.year,
    SUM(fs.quantity_sold)  AS units_sold,
    SUM(fs.net_revenue)    AS revenue,
    RANK() OVER (
        PARTITION BY t.month_num, t.year
        ORDER BY SUM(fs.net_revenue) DESC
    )                      AS revenue_rank
FROM fact_sales fs
JOIN dim_product p ON p.product_key = fs.product_key
JOIN dim_time    t ON t.time_key    = fs.time_key
GROUP BY p.product_id, p.product_name, p.category, t.month_num, t.year;

-- Store performance summary
CREATE OR REPLACE VIEW v_store_performance AS
SELECT
    s.store_id,
    s.store_name,
    s.region,
    s.city,
    t.year,
    t.quarter,
    SUM(fs.quantity_sold)     AS units_sold,
    SUM(fs.net_revenue)       AS total_revenue,
    SUM(fs.gross_profit)      AS gross_profit,
    ROUND(
        SUM(fs.gross_profit) / NULLIF(SUM(fs.net_revenue), 0) * 100, 2
    )                         AS gross_margin_pct,
    COUNT(DISTINCT fs.transaction_id) AS num_transactions,
    COUNT(DISTINCT fs.customer_id)    AS unique_customers
FROM fact_sales fs
JOIN dim_store s ON s.store_key = fs.store_key
JOIN dim_time  t ON t.time_key  = fs.time_key
GROUP BY s.store_id, s.store_name, s.region, s.city, t.year, t.quarter;

-- Category demand trend (good for forecasting)
CREATE OR REPLACE VIEW v_category_demand_trend AS
SELECT
    t.full_date,
    p.category,
    SUM(fs.quantity_sold)  AS daily_units,
    SUM(fs.net_revenue)    AS daily_revenue,
    AVG(SUM(fs.quantity_sold)) OVER (
        PARTITION BY p.category
        ORDER BY t.full_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )                      AS rolling_7d_avg_units,
    AVG(SUM(fs.quantity_sold)) OVER (
        PARTITION BY p.category
        ORDER BY t.full_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    )                      AS rolling_30d_avg_units
FROM fact_sales fs
JOIN dim_product p ON p.product_key = fs.product_key
JOIN dim_time    t ON t.time_key    = fs.time_key
GROUP BY t.full_date, p.category
ORDER BY t.full_date, p.category;

-- ML-ready export: flat table with all features for forecasting models
CREATE OR REPLACE VIEW v_ml_ready_dataset AS
SELECT
    a.agg_date,
    a.product_id,
    a.store_id,
    a.category,
    a.region,
    a.total_quantity          AS target_demand,
    a.total_revenue,
    a.total_orders,
    a.avg_order_value,
    a.discount_rate,
    -- Time features
    a.day_of_week,
    a.week_of_year,
    a.month_num,
    a.quarter,
    a.is_holiday,
    CASE WHEN a.is_holiday THEN 1 ELSE 0 END AS is_holiday_flag,
    -- Rolling demand features (lag features for ML)
    LAG(a.total_quantity, 7)  OVER w AS demand_lag_7d,
    LAG(a.total_quantity, 14) OVER w AS demand_lag_14d,
    LAG(a.total_quantity, 28) OVER w AS demand_lag_28d,
    AVG(a.total_quantity) OVER (
        PARTITION BY a.product_id, a.store_id
        ORDER BY a.agg_date
        ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
    )                         AS rolling_avg_7d,
    AVG(a.total_quantity) OVER (
        PARTITION BY a.product_id, a.store_id
        ORDER BY a.agg_date
        ROWS BETWEEN 28 PRECEDING AND 1 PRECEDING
    )                         AS rolling_avg_28d,
    -- Weather features
    a.temp_high_f,
    a.precipitation_in
FROM agg_daily_demand a
WINDOW w AS (PARTITION BY a.product_id, a.store_id ORDER BY a.agg_date);
