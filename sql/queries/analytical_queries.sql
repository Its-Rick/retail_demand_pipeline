-- =============================================================================
-- analytical_queries.sql
-- Business-facing SQL queries for the Retail Demand Forecasting project.
-- Run against the warehouse schema in PostgreSQL.
-- =============================================================================

SET search_path = warehouse;

-- ─── 1. REVENUE TREND: MONTH-OVER-MONTH ──────────────────────────────────────
-- Shows monthly revenue with absolute and percentage change vs prior month.

WITH monthly AS (
    SELECT
        t.year,
        t.month_num,
        TO_CHAR(t.full_date, 'Mon YYYY')        AS month_label,
        SUM(fs.net_revenue)                      AS revenue,
        SUM(fs.quantity_sold)                    AS units_sold,
        COUNT(DISTINCT fs.transaction_id)        AS transactions
    FROM fact_sales fs
    JOIN dim_time t ON t.time_key = fs.time_key
    GROUP BY t.year, t.month_num, TO_CHAR(t.full_date, 'Mon YYYY')
)
SELECT
    month_label,
    revenue,
    units_sold,
    transactions,
    LAG(revenue) OVER (ORDER BY year, month_num)         AS prev_month_revenue,
    ROUND(
        (revenue - LAG(revenue) OVER (ORDER BY year, month_num))
        / NULLIF(LAG(revenue) OVER (ORDER BY year, month_num), 0) * 100, 2
    )                                                    AS mom_change_pct
FROM monthly
ORDER BY year, month_num;


-- ─── 2. TOP 10 PRODUCTS BY CATEGORY (LAST 30 DAYS) ───────────────────────────

SELECT
    p.category,
    p.product_id,
    p.product_name,
    SUM(fs.quantity_sold)  AS units_sold_30d,
    SUM(fs.net_revenue)    AS revenue_30d,
    RANK() OVER (
        PARTITION BY p.category
        ORDER BY SUM(fs.net_revenue) DESC
    )                      AS rank_in_category
FROM fact_sales fs
JOIN dim_product p ON p.product_key = fs.product_key
JOIN dim_time    t ON t.time_key    = fs.time_key
WHERE t.full_date >= CURRENT_DATE - 30
GROUP BY p.category, p.product_id, p.product_name
QUALIFY rank_in_category <= 10   -- use HAVING rank_in_category <= 10 for older PG
ORDER BY p.category, rank_in_category;


-- ─── 3. STORE PERFORMANCE SCORECARD ──────────────────────────────────────────
-- Quarter-to-date vs same quarter last year

WITH qtd_current AS (
    SELECT
        s.store_id, s.store_name, s.region,
        SUM(fs.net_revenue)    AS revenue_qtd,
        SUM(fs.quantity_sold)  AS units_qtd,
        COUNT(DISTINCT fs.customer_id) AS customers_qtd
    FROM fact_sales fs
    JOIN dim_store s ON s.store_key = fs.store_key
    JOIN dim_time  t ON t.time_key  = fs.time_key
    WHERE t.year    = EXTRACT(YEAR    FROM CURRENT_DATE)
      AND t.quarter = EXTRACT(QUARTER FROM CURRENT_DATE)
    GROUP BY s.store_id, s.store_name, s.region
),
qtd_prior AS (
    SELECT
        s.store_id,
        SUM(fs.net_revenue) AS revenue_prior_qtd
    FROM fact_sales fs
    JOIN dim_store s ON s.store_key = fs.store_key
    JOIN dim_time  t ON t.time_key  = fs.time_key
    WHERE t.year    = EXTRACT(YEAR    FROM CURRENT_DATE) - 1
      AND t.quarter = EXTRACT(QUARTER FROM CURRENT_DATE)
    GROUP BY s.store_id
)
SELECT
    c.store_id, c.store_name, c.region,
    c.revenue_qtd,
    c.units_qtd,
    c.customers_qtd,
    p.revenue_prior_qtd,
    ROUND(
        (c.revenue_qtd - p.revenue_prior_qtd)
        / NULLIF(p.revenue_prior_qtd, 0) * 100, 2
    )                          AS yoy_growth_pct
FROM qtd_current c
LEFT JOIN qtd_prior p USING (store_id)
ORDER BY c.revenue_qtd DESC;


-- ─── 4. DEMAND SPIKE DETECTION ────────────────────────────────────────────────
-- Identifies products where today's demand is > 2 std devs above the 30-day mean.
-- Useful for early detection of viral products or supply chain risk.

WITH stats AS (
    SELECT
        product_id,
        store_id,
        AVG(total_quantity)    AS avg_30d,
        STDDEV(total_quantity) AS std_30d
    FROM agg_daily_demand
    WHERE agg_date BETWEEN CURRENT_DATE - 31 AND CURRENT_DATE - 1
    GROUP BY product_id, store_id
),
today AS (
    SELECT product_id, store_id, total_quantity AS today_qty
    FROM agg_daily_demand
    WHERE agg_date = CURRENT_DATE - 1
)
SELECT
    t.product_id,
    t.store_id,
    t.today_qty,
    ROUND(s.avg_30d, 1)   AS avg_30d,
    ROUND(s.std_30d, 1)   AS std_30d,
    ROUND((t.today_qty - s.avg_30d) / NULLIF(s.std_30d, 0), 2) AS z_score,
    CASE
        WHEN (t.today_qty - s.avg_30d) / NULLIF(s.std_30d, 0) > 3 THEN 'EXTREME_SPIKE'
        WHEN (t.today_qty - s.avg_30d) / NULLIF(s.std_30d, 0) > 2 THEN 'SPIKE'
        WHEN (t.today_qty - s.avg_30d) / NULLIF(s.std_30d, 0) < -2 THEN 'DIP'
        ELSE 'NORMAL'
    END                   AS demand_signal
FROM today t
JOIN stats s USING (product_id, store_id)
WHERE ABS((t.today_qty - s.avg_30d) / NULLIF(s.std_30d, 0)) > 2
ORDER BY z_score DESC;


-- ─── 5. HOLIDAY IMPACT ANALYSIS ───────────────────────────────────────────────
-- Compares average daily demand on holidays vs non-holidays per category.

SELECT
    p.category,
    t.is_holiday,
    t.holiday_name,
    ROUND(AVG(fs.quantity_sold), 2)   AS avg_daily_units,
    ROUND(AVG(fs.net_revenue),   2)   AS avg_daily_revenue,
    COUNT(DISTINCT t.full_date)       AS num_days
FROM fact_sales fs
JOIN dim_product p ON p.product_key = fs.product_key
JOIN dim_time    t ON t.time_key    = fs.time_key
GROUP BY p.category, t.is_holiday, t.holiday_name
ORDER BY p.category, t.is_holiday DESC;


-- ─── 6. CUSTOMER COHORT ANALYSIS (REPEAT PURCHASERS) ─────────────────────────

WITH first_purchase AS (
    SELECT
        customer_id,
        MIN(t.full_date) AS first_purchase_date,
        DATE_TRUNC('month', MIN(t.full_date)) AS cohort_month
    FROM fact_sales fs
    JOIN dim_time t ON t.time_key = fs.time_key
    WHERE customer_id IS NOT NULL
    GROUP BY customer_id
),
purchases AS (
    SELECT
        fs.customer_id,
        t.full_date,
        fp.cohort_month,
        EXTRACT(MONTH FROM AGE(t.full_date, fp.cohort_month)) AS months_since_first
    FROM fact_sales fs
    JOIN dim_time    t  ON t.time_key = fs.time_key
    JOIN first_purchase fp ON fp.customer_id = fs.customer_id
    WHERE fs.customer_id IS NOT NULL
)
SELECT
    TO_CHAR(cohort_month, 'YYYY-MM')   AS cohort,
    months_since_first                  AS month_number,
    COUNT(DISTINCT customer_id)         AS active_customers
FROM purchases
GROUP BY cohort_month, months_since_first
ORDER BY cohort_month, months_since_first;


-- ─── 7. PRODUCT ABC CLASSIFICATION ───────────────────────────────────────────
-- Classifies products into A (top 80% revenue), B (next 15%), C (bottom 5%).
-- Used for inventory prioritization and forecasting granularity decisions.

WITH product_revenue AS (
    SELECT
        p.product_id,
        p.product_name,
        p.category,
        SUM(fs.net_revenue)                        AS total_revenue,
        SUM(SUM(fs.net_revenue)) OVER ()           AS grand_total,
        SUM(SUM(fs.net_revenue)) OVER (
            ORDER BY SUM(fs.net_revenue) DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )                                          AS running_total
    FROM fact_sales fs
    JOIN dim_product p ON p.product_key = fs.product_key
    GROUP BY p.product_id, p.product_name, p.category
)
SELECT
    product_id,
    product_name,
    category,
    ROUND(total_revenue, 2)                        AS total_revenue,
    ROUND(total_revenue / grand_total * 100, 2)    AS revenue_pct,
    ROUND(running_total / grand_total * 100, 2)    AS cumulative_pct,
    CASE
        WHEN running_total / grand_total <= 0.80 THEN 'A'
        WHEN running_total / grand_total <= 0.95 THEN 'B'
        ELSE 'C'
    END                                            AS abc_class
FROM product_revenue
ORDER BY total_revenue DESC;


-- ─── 8. CHANNEL MIX ANALYSIS (STORE vs ONLINE) ────────────────────────────────

SELECT
    t.year,
    t.month_num,
    fs.channel,
    SUM(fs.net_revenue)                    AS revenue,
    SUM(fs.quantity_sold)                  AS units,
    ROUND(
        SUM(fs.net_revenue) * 100.0 /
        SUM(SUM(fs.net_revenue)) OVER (PARTITION BY t.year, t.month_num),
        2
    )                                      AS channel_revenue_pct
FROM fact_sales fs
JOIN dim_time t ON t.time_key = fs.time_key
GROUP BY t.year, t.month_num, fs.channel
ORDER BY t.year, t.month_num, fs.channel;


-- ─── 9. DISCOUNT EFFECTIVENESS ────────────────────────────────────────────────
-- Checks if discounted items drive proportionally more volume.

SELECT
    p.category,
    CASE
        WHEN fs.discount_amount = 0             THEN 'No discount'
        WHEN fs.discount_amount / NULLIF(fs.gross_revenue, 0) < 0.10 THEN '<10%'
        WHEN fs.discount_amount / NULLIF(fs.gross_revenue, 0) < 0.20 THEN '10-20%'
        ELSE '>20%'
    END                                      AS discount_band,
    COUNT(*)                                 AS num_transactions,
    ROUND(AVG(fs.quantity_sold), 2)          AS avg_quantity,
    ROUND(AVG(fs.net_revenue), 2)            AS avg_net_revenue,
    ROUND(AVG(fs.gross_profit), 2)           AS avg_gross_profit
FROM fact_sales fs
JOIN dim_product p ON p.product_key = fs.product_key
GROUP BY p.category, discount_band
ORDER BY p.category, discount_band;


-- ─── 10. SLOW MOVERS (30-DAY ZERO SALES) ─────────────────────────────────────
-- Products in the warehouse with no sales in the last 30 days.
-- Flag for markdown or clearance decisions.

SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.unit_price,
    MAX(t.full_date) AS last_sale_date,
    CURRENT_DATE - MAX(t.full_date) AS days_since_last_sale
FROM dim_product p
LEFT JOIN fact_sales fs ON fs.product_key = p.product_key
LEFT JOIN dim_time t    ON t.time_key     = fs.time_key
WHERE p.is_active = TRUE
GROUP BY p.product_id, p.product_name, p.category, p.unit_price
HAVING MAX(t.full_date) < CURRENT_DATE - 30
    OR MAX(t.full_date) IS NULL
ORDER BY days_since_last_sale DESC NULLS FIRST;
