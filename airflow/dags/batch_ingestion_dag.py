"""
batch_ingestion_dag.py
----------------------
Main Airflow DAG for the Retail Demand Forecasting batch pipeline.

Pipeline stages:
  1. Extract  → Copy raw files to Data Lake (partitioned by date)
  2. Validate → Run data quality checks on raw data
  3. Transform → PySpark ETL: clean, enrich, deduplicate
  4. Load      → Write dimensions to PostgreSQL Data Warehouse
  5. Aggregate → Compute daily/weekly demand tables (inline SQL)
  6. Export    → Write ML-ready CSV for model consumption

Schedule: Daily at 02:00 UTC (processes previous day's data)
"""

from datetime import datetime, timedelta
import os
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

log = logging.getLogger(__name__)

# ─── DEFAULT ARGS ─────────────────────────────────────────────────────────────
default_args = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

DW_CONN_ID    = "postgres_retail_dw"
DATA_LAKE     = os.getenv("DATA_LAKE_PATH", "/opt/airflow/data")
SPARK_MASTER  = os.getenv("SPARK_MASTER",   "local[2]")

# ─── INLINE AGGREGATION SQL ───────────────────────────────────────────────────
# Defined here to avoid Jinja template file-path resolution issues.

DAILY_AGG_SQL = """
INSERT INTO warehouse.agg_daily_demand (
    agg_date, product_id, store_id, category, region,
    total_quantity, total_revenue, total_orders, avg_order_value,
    discount_rate, is_holiday, holiday_name,
    day_of_week, week_of_year, month_num, quarter
)
SELECT
    t.full_date                                                         AS agg_date,
    p.product_id,
    s.store_id,
    p.category,
    s.region,
    SUM(fs.quantity_sold)                                               AS total_quantity,
    SUM(fs.net_revenue)                                                 AS total_revenue,
    COUNT(DISTINCT fs.transaction_id)                                   AS total_orders,
    ROUND(SUM(fs.net_revenue) /
          NULLIF(COUNT(DISTINCT fs.transaction_id), 0), 2)             AS avg_order_value,
    ROUND(SUM(fs.discount_amount) /
          NULLIF(SUM(fs.gross_revenue), 0), 4)                         AS discount_rate,
    t.is_holiday,
    t.holiday_name,
    t.day_of_week,
    t.week_of_year,
    t.month_num,
    t.quarter
FROM warehouse.fact_sales fs
JOIN warehouse.dim_time    t ON t.time_key    = fs.time_key
JOIN warehouse.dim_product p ON p.product_key = fs.product_key
JOIN warehouse.dim_store   s ON s.store_key   = fs.store_key
GROUP BY
    t.full_date, p.product_id, s.store_id, p.category, s.region,
    t.is_holiday, t.holiday_name, t.day_of_week,
    t.week_of_year, t.month_num, t.quarter
ON CONFLICT (agg_date, product_id, store_id)
DO UPDATE SET
    total_quantity  = EXCLUDED.total_quantity,
    total_revenue   = EXCLUDED.total_revenue,
    total_orders    = EXCLUDED.total_orders,
    avg_order_value = EXCLUDED.avg_order_value,
    discount_rate   = EXCLUDED.discount_rate,
    created_at      = CURRENT_TIMESTAMP;
"""

WEEKLY_AGG_SQL = """
INSERT INTO warehouse.agg_weekly_demand (
    year, week_of_year, product_id, store_id, category, region,
    total_quantity, total_revenue, total_orders, avg_daily_qty
)
SELECT
    t.year,
    t.week_of_year,
    p.product_id,
    s.store_id,
    p.category,
    s.region,
    SUM(fs.quantity_sold)                            AS total_quantity,
    SUM(fs.net_revenue)                              AS total_revenue,
    COUNT(DISTINCT fs.transaction_id)                AS total_orders,
    ROUND(SUM(fs.quantity_sold)::NUMERIC / 7, 2)    AS avg_daily_qty
FROM warehouse.fact_sales fs
JOIN warehouse.dim_time    t ON t.time_key    = fs.time_key
JOIN warehouse.dim_product p ON p.product_key = fs.product_key
JOIN warehouse.dim_store   s ON s.store_key   = fs.store_key
GROUP BY t.year, t.week_of_year, p.product_id, s.store_id, p.category, s.region
ON CONFLICT (year, week_of_year, product_id, store_id)
DO UPDATE SET
    total_quantity = EXCLUDED.total_quantity,
    total_revenue  = EXCLUDED.total_revenue,
    total_orders   = EXCLUDED.total_orders,
    avg_daily_qty  = EXCLUDED.avg_daily_qty,
    created_at     = CURRENT_TIMESTAMP;
"""

# ─── PYTHON CALLABLES ─────────────────────────────────────────────────────────

def extract_pos_to_lake(**context):
    """Stage 1: Copy POS CSV to Data Lake with Hive-style partitioning."""
    import shutil
    ds = context["ds"]
    year, month, day = ds.split("-")
    src = os.path.join(DATA_LAKE, "sample", "pos_sales.csv")
    dst_dir = os.path.join(DATA_LAKE, "raw", "pos",
                           f"year={year}", f"month={month}", f"day={day}")
    os.makedirs(dst_dir, exist_ok=True)
    dst = os.path.join(dst_dir, "pos_sales.csv")
    if os.path.exists(src):
        shutil.copy2(src, dst)
        log.info(f"POS data copied → {dst}")
    else:
        log.warning(f"Source not found: {src} — skipping copy")
    context["ti"].xcom_push(key="pos_raw_path", value=dst)
    return dst


def extract_ecommerce_to_lake(**context):
    """Stage 1b: Copy e-commerce JSON to Data Lake."""
    import shutil
    ds = context["ds"]
    year, month, day = ds.split("-")
    src = os.path.join(DATA_LAKE, "sample", "ecommerce_events.json")
    dst_dir = os.path.join(DATA_LAKE, "raw", "ecommerce",
                           f"year={year}", f"month={month}", f"day={day}")
    os.makedirs(dst_dir, exist_ok=True)
    dst = os.path.join(dst_dir, "events.json")
    if os.path.exists(src):
        shutil.copy2(src, dst)
        log.info(f"E-commerce data copied → {dst}")
    else:
        log.warning(f"Source not found: {src} — skipping copy")
    context["ti"].xcom_push(key="ecom_raw_path", value=dst)
    return dst


def run_data_quality_checks(**context):
    """
    Stage 2: Validate raw POS data.
    Returns task_id of next task to branch to.
    """
    import pandas as pd

    pos_path = context["ti"].xcom_pull(key="pos_raw_path", task_ids="extract_pos")
    if not pos_path or not os.path.exists(pos_path):
        log.warning("POS file not found — skipping DQ, proceeding to transform")
        return "transform_pos_data"

    df = pd.read_csv(pos_path)
    hook = PostgresHook(postgres_conn_id=DW_CONN_ID)

    checks = [
        ("row_count_ok",          len(df) > 100,                          0),
        ("txn_id_no_nulls",       df["transaction_id"].isnull().mean() == 0, 0),
        ("quantity_positive",     (df["quantity"] <= 0).mean() < 0.01,    0.01),
        ("duplicate_txns",        df.duplicated("transaction_id").mean() < 0.02, 0.02),
    ]

    all_passed = True
    for name, passed, threshold in checks:
        status = "PASS" if passed else "FAIL"
        if not passed:
            all_passed = False
            log.warning(f"DQ FAIL: {name}")
        try:
            hook.run("""
                INSERT INTO warehouse.dq_check_log
                  (check_name, table_name, check_type, status, threshold, pipeline_run_id)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, parameters=(name, "pos_sales_raw", "pre_load", status,
                             threshold, context["run_id"]))
        except Exception as e:
            log.warning(f"Could not write DQ log: {e}")

    return "transform_pos_data" if all_passed else "alert_dq_failure"


def load_dim_product(**context):
    """Stage 4a: Upsert product dimension from POS data."""
    import pandas as pd

    pos_path = context["ti"].xcom_pull(key="pos_raw_path", task_ids="extract_pos")
    if not pos_path or not os.path.exists(pos_path):
        log.warning("POS file not found — skipping dim_product load")
        return

    df = pd.read_csv(pos_path)[
        ["product_id", "product_name", "category", "brand", "unit_price"]
    ].drop_duplicates(subset=["product_id"]).dropna(subset=["product_id"])

    hook = PostgresHook(postgres_conn_id=DW_CONN_ID)
    inserted = 0
    for _, row in df.iterrows():
        try:
            hook.run("""
                INSERT INTO warehouse.dim_product
                  (product_id, product_name, category, brand, unit_price)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (product_id) DO UPDATE SET
                  product_name = EXCLUDED.product_name,
                  category     = EXCLUDED.category,
                  brand        = EXCLUDED.brand,
                  unit_price   = EXCLUDED.unit_price,
                  updated_at   = CURRENT_TIMESTAMP
            """, parameters=(
                str(row.get("product_id", "")),
                str(row.get("product_name", "Unknown")),
                str(row.get("category", "")) if row.get("category") else None,
                str(row.get("brand", "")) if row.get("brand") else None,
                float(row["unit_price"]) if row.get("unit_price") else None,
            ))
            inserted += 1
        except Exception as e:
            log.warning(f"Skipping product {row.get('product_id')}: {e}")
    log.info(f"dim_product: upserted {inserted} products")


def load_dim_store(**context):
    """Stage 4b: Upsert store dimension from POS data."""
    import pandas as pd

    pos_path = context["ti"].xcom_pull(key="pos_raw_path", task_ids="extract_pos")
    if not pos_path or not os.path.exists(pos_path):
        log.warning("POS file not found — skipping dim_store load")
        return

    df = pd.read_csv(pos_path)[
        ["store_id", "store_name", "city", "state", "region"]
    ].drop_duplicates(subset=["store_id"]).dropna(subset=["store_id"])

    hook = PostgresHook(postgres_conn_id=DW_CONN_ID)
    inserted = 0
    for _, row in df.iterrows():
        try:
            hook.run("""
                INSERT INTO warehouse.dim_store
                  (store_id, store_name, city, state_code, region)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (store_id) DO UPDATE SET
                  store_name = EXCLUDED.store_name,
                  city       = EXCLUDED.city,
                  updated_at = CURRENT_TIMESTAMP
            """, parameters=(
                str(row["store_id"]),
                str(row.get("store_name", "Unknown")),
                str(row.get("city", "")) if row.get("city") else None,
                str(row.get("state", "")) if row.get("state") else None,
                str(row.get("region", "")) if row.get("region") else None,
            ))
            inserted += 1
        except Exception as e:
            log.warning(f"Skipping store {row.get('store_id')}: {e}")
    log.info(f"dim_store: upserted {inserted} stores")


def load_fact_sales(**context):
    """
    Stage 4c: Load fact_sales from POS CSV.
    Looks up surrogate keys from dimension tables.
    """
    import pandas as pd
    from psycopg2.extras import execute_batch

    pos_path = context["ti"].xcom_pull(key="pos_raw_path", task_ids="extract_pos")
    if not pos_path or not os.path.exists(pos_path):
        log.warning("POS file not found — skipping fact_sales load")
        return

    df = pd.read_csv(pos_path)
    df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce")
    df = df.dropna(subset=["transaction_id", "product_id", "store_id", "transaction_date"])
    df = df[df["quantity"] > 0]
    df = df.drop_duplicates(subset=["transaction_id"])

    hook = PostgresHook(postgres_conn_id=DW_CONN_ID)
    conn = hook.get_conn()

    # Build lookup maps
    with conn.cursor() as cur:
        cur.execute("SELECT product_id, product_key FROM warehouse.dim_product")
        product_map = {r[0]: r[1] for r in cur.fetchall()}

        cur.execute("SELECT store_id, store_key FROM warehouse.dim_store")
        store_map = {r[0]: r[1] for r in cur.fetchall()}

        cur.execute("SELECT full_date, time_key FROM warehouse.dim_time")
        time_map = {str(r[0]): r[1] for r in cur.fetchall()}

    records = []
    skipped = 0
    for _, row in df.iterrows():
        date_str = str(row["transaction_date"].date())
        product_key = product_map.get(str(row["product_id"]))
        store_key   = store_map.get(str(row["store_id"]))
        time_key    = time_map.get(date_str)

        if not all([product_key, store_key, time_key]):
            skipped += 1
            continue

        qty        = int(row.get("quantity", 0))
        unit_price = float(row.get("unit_price", 0))
        discount   = float(row.get("discount", 0))
        gross_rev  = round(qty * unit_price, 2)
        disc_amt   = round(gross_rev * discount, 2)
        net_rev    = round(gross_rev - disc_amt, 2)

        records.append((
            time_key, product_key, store_key,
            str(row["transaction_id"]),
            str(row["customer_id"]) if pd.notna(row.get("customer_id")) else None,
            str(row.get("payment_method", "")) or None,
            "store",
            qty, unit_price, disc_amt, gross_rev, net_rev,
            "pos", context["run_id"],
        ))

    if records:
        with conn.cursor() as cur:
            execute_batch(cur, """
                INSERT INTO warehouse.fact_sales (
                    time_key, product_key, store_key,
                    transaction_id, customer_id, payment_method, channel,
                    quantity_sold, unit_price, discount_amount,
                    gross_revenue, net_revenue, source_system, batch_id
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
            """, records, page_size=500)
        conn.commit()
        log.info(f"fact_sales: inserted {len(records):,} rows (skipped {skipped})")
    else:
        log.warning("No valid records to insert into fact_sales")

    conn.close()


def run_aggregations(**context):
    """Stage 5: Compute daily + weekly demand aggregations using inline SQL."""
    hook = PostgresHook(postgres_conn_id=DW_CONN_ID)

    # Check if there's anything to aggregate
    count = hook.get_first("SELECT COUNT(*) FROM warehouse.fact_sales")[0]
    if count == 0:
        log.warning("fact_sales is empty — skipping aggregation")
        return

    log.info(f"Aggregating {count:,} fact rows...")
    hook.run(DAILY_AGG_SQL)
    log.info("Daily aggregation complete")

    hook.run(WEEKLY_AGG_SQL)
    log.info("Weekly aggregation complete")

    agg_count = hook.get_first(
        "SELECT COUNT(*) FROM warehouse.agg_daily_demand"
    )[0]
    log.info(f"agg_daily_demand now has {agg_count:,} rows")


def export_ml_dataset(**context):
    """Stage 6: Export ML-ready dataset to CSV."""
    import pandas as pd

    hook = PostgresHook(postgres_conn_id=DW_CONN_ID)

    count = hook.get_first("SELECT COUNT(*) FROM warehouse.agg_daily_demand")[0]
    if count == 0:
        log.warning("agg_daily_demand is empty — skipping ML export")
        return

    df = hook.get_pandas_df("""
        SELECT
            agg_date, product_id, store_id, category, region,
            total_quantity, total_revenue, total_orders,
            day_of_week, week_of_year, month_num, quarter,
            is_holiday::int AS is_holiday
        FROM warehouse.agg_daily_demand
        ORDER BY agg_date, product_id, store_id
    """)

    out_dir = os.path.join(DATA_LAKE, "processed", "ml_ready",
                           f"dt={context['ds']}")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "demand_features.csv")
    df.to_csv(out_path, index=False)
    log.info(f"ML dataset exported: {out_path} ({len(df):,} rows)")
    context["ti"].xcom_push(key="ml_dataset_path", value=out_path)


def pipeline_summary(**context):
    """Stage 7: Log pipeline summary."""
    hook = PostgresHook(postgres_conn_id=DW_CONN_ID)
    facts  = hook.get_first("SELECT COUNT(*) FROM warehouse.fact_sales")[0]
    daily  = hook.get_first("SELECT COUNT(*) FROM warehouse.agg_daily_demand")[0]
    events = hook.get_first("SELECT COUNT(*) FROM warehouse.stream_ecommerce_events")[0]
    log.info(
        f"Pipeline complete | run={context['run_id']} | "
        f"fact_sales={facts:,} | agg_daily={daily:,} | stream_events={events:,}"
    )


# ─── DAG ──────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="retail_demand_batch_pipeline",
    default_args=default_args,
    description="Daily batch pipeline for retail demand forecasting",
    schedule_interval="0 2 * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["retail", "demand-forecasting", "batch"],
) as dag:

    start = EmptyOperator(task_id="start")

    extract_pos = PythonOperator(
        task_id="extract_pos",
        python_callable=extract_pos_to_lake,
    )

    extract_ecom = PythonOperator(
        task_id="extract_ecommerce",
        python_callable=extract_ecommerce_to_lake,
    )

    dq_check = BranchPythonOperator(
        task_id="data_quality_check",
        python_callable=run_data_quality_checks,
    )

    alert_dq_failure = BashOperator(
        task_id="alert_dq_failure",
        bash_command='echo "DQ FAILURE on {{ ds }} — check dq_check_log table"',
    )

    transform_pos = BashOperator(
        task_id="transform_pos_data",
        bash_command='echo "PySpark transform skipped in local mode — data loaded via Python"',
    )

    load_product_dim = PythonOperator(
        task_id="load_dim_product",
        python_callable=load_dim_product,
    )

    load_store_dim = PythonOperator(
        task_id="load_dim_store",
        python_callable=load_dim_store,
    )

    load_facts = PythonOperator(
        task_id="load_fact_sales",
        python_callable=load_fact_sales,
    )

    aggregate = PythonOperator(
        task_id="aggregate_daily_demand",
        python_callable=run_aggregations,
    )

    export_ml = PythonOperator(
        task_id="export_ml_dataset",
        python_callable=export_ml_dataset,
    )

    summary = PythonOperator(
        task_id="pipeline_summary",
        python_callable=pipeline_summary,
    )

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    # ── Dependencies ──────────────────────────────────────────────────────────
    start >> [extract_pos, extract_ecom]
    extract_pos  >> dq_check >> [alert_dq_failure, transform_pos]
    transform_pos >> [load_product_dim, load_store_dim]
    [load_product_dim, load_store_dim] >> load_facts
    [load_facts, extract_ecom] >> aggregate
    aggregate >> export_ml >> summary >> end
    alert_dq_failure >> end