"""
batch_ingestion_dag.py
----------------------
Main Airflow DAG for the Retail Demand Forecasting batch pipeline.

Pipeline stages:
  1. Extract  → Copy raw files to Data Lake (partitioned by date)
  2. Validate → Run data quality checks on raw data
  3. Transform → PySpark ETL: clean, enrich, deduplicate
  4. Load      → Write to PostgreSQL Data Warehouse
  5. Aggregate → Compute daily/weekly demand tables
  6. Export    → Write ML-ready CSV for model consumption

Schedule: Daily at 02:00 UTC (processes previous day's data)
"""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.models import Variable

# ─── DEFAULT ARGS ─────────────────────────────────────────────────────────────
default_args = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "email":            ["alerts@retailpipeline.com"],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# ─── CONSTANTS ────────────────────────────────────────────────────────────────
DATA_LAKE_BASE  = Variable.get("DATA_LAKE_PATH", default_var="/opt/airflow/data")
DW_CONN_ID      = "postgres_retail_dw"
SPARK_HOME      = Variable.get("SPARK_HOME", default_var="/opt/spark")
SPARK_MASTER    = Variable.get("SPARK_MASTER", default_var="local[*]")


# ─── PYTHON CALLABLE FUNCTIONS ────────────────────────────────────────────────

def extract_pos_to_lake(**context):
    """
    Stage 1: Copy POS CSV files to the Data Lake with date partitioning.
    In production, this would pull from S3/SFTP/database.
    """
    import shutil
    import logging

    execution_date = context["ds"]                         # e.g., '2023-11-15'
    year, month, day = execution_date.split("-")

    src_path = os.path.join(DATA_LAKE_BASE, "sample", "pos_sales.csv")
    # Hive-style partitioning: year=YYYY/month=MM/day=DD
    dst_dir = os.path.join(DATA_LAKE_BASE, "raw", "pos",
                           f"year={year}", f"month={month}", f"day={day}")
    os.makedirs(dst_dir, exist_ok=True)

    dst_path = os.path.join(dst_dir, "pos_sales.csv")
    shutil.copy2(src_path, dst_path)

    logging.info(f"POS data extracted to: {dst_path}")
    # Push path to XCom for downstream tasks
    context["ti"].xcom_push(key="pos_raw_path", value=dst_path)
    return dst_path


def extract_ecommerce_to_lake(**context):
    """Stage 1b: Copy e-commerce JSON events to the Data Lake."""
    import shutil, logging

    execution_date = context["ds"]
    year, month, day = execution_date.split("-")

    src_path = os.path.join(DATA_LAKE_BASE, "sample", "ecommerce_events.json")
    dst_dir  = os.path.join(DATA_LAKE_BASE, "raw", "ecommerce",
                            f"year={year}", f"month={month}", f"day={day}")
    os.makedirs(dst_dir, exist_ok=True)

    dst_path = os.path.join(dst_dir, "events.json")
    shutil.copy2(src_path, dst_path)

    logging.info(f"E-commerce data extracted to: {dst_path}")
    context["ti"].xcom_push(key="ecom_raw_path", value=dst_path)
    return dst_path


def run_data_quality_checks(**context):
    """
    Stage 2: Validates raw data before transformation.
    Logs results to dq_check_log table.
    Returns 'transform_data' if all checks pass, else 'alert_dq_failure'.
    """
    import pandas as pd
    import logging

    pos_path = context["ti"].xcom_pull(key="pos_raw_path", task_ids="extract_pos")
    df = pd.read_csv(pos_path)

    checks = []

    # Check 1: No fully empty rows
    empty_row_rate = df.isnull().all(axis=1).sum() / len(df)
    checks.append(("empty_rows", "fact_sales_raw", empty_row_rate < 0.01, 0.01))

    # Check 2: transaction_id nulls
    txn_null_rate = df["transaction_id"].isnull().mean()
    checks.append(("transaction_id_nulls", "fact_sales_raw", txn_null_rate == 0, 0.0))

    # Check 3: quantity > 0
    bad_qty_rate = (df["quantity"] <= 0).mean()
    checks.append(("negative_quantity", "fact_sales_raw", bad_qty_rate < 0.001, 0.001))

    # Check 4: duplicate transactions
    dup_rate = df.duplicated(subset=["transaction_id"]).mean()
    checks.append(("duplicates", "fact_sales_raw", dup_rate < 0.02, 0.02))

    # Log results
    pg_hook = PostgresHook(postgres_conn_id=DW_CONN_ID)
    all_passed = True
    for check_name, table, passed, threshold in checks:
        status = "PASS" if passed else "FAIL"
        if not passed:
            all_passed = False
            logging.warning(f"DQ CHECK FAILED: {check_name} on {table}")
        pg_hook.run("""
            INSERT INTO warehouse.dq_check_log
                (check_name, table_name, status, threshold, pipeline_run_id)
            VALUES (%s, %s, %s, %s, %s)
        """, parameters=(check_name, table, status, threshold, context["run_id"]))

    if all_passed:
        return "transform_pos_data"
    return "alert_dq_failure"


def load_dim_product(**context):
    """Stage 4a: Upsert product dimension (SCD Type 1)."""
    import pandas as pd

    pos_path = context["ti"].xcom_pull(key="pos_raw_path", task_ids="extract_pos")
    df = pd.read_csv(pos_path)[
        ["product_id", "product_name", "category", "brand", "unit_price"]
    ].drop_duplicates(subset=["product_id"])

    pg_hook = PostgresHook(postgres_conn_id=DW_CONN_ID)
    for _, row in df.iterrows():
        pg_hook.run("""
            INSERT INTO warehouse.dim_product
                (product_id, product_name, category, brand, unit_price)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (product_id) DO UPDATE SET
                product_name = EXCLUDED.product_name,
                category     = EXCLUDED.category,
                brand        = EXCLUDED.brand,
                unit_price   = EXCLUDED.unit_price,
                updated_at   = CURRENT_TIMESTAMP
        """, parameters=tuple(row))


def load_dim_store(**context):
    """Stage 4b: Upsert store dimension."""
    import pandas as pd

    pos_path = context["ti"].xcom_pull(key="pos_raw_path", task_ids="extract_pos")
    df = pd.read_csv(pos_path)[
        ["store_id", "store_name", "city", "state", "region"]
    ].drop_duplicates(subset=["store_id"])

    pg_hook = PostgresHook(postgres_conn_id=DW_CONN_ID)
    for _, row in df.iterrows():
        pg_hook.run("""
            INSERT INTO warehouse.dim_store
                (store_id, store_name, city, state_code, region)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (store_id) DO UPDATE SET
                store_name = EXCLUDED.store_name,
                city       = EXCLUDED.city,
                updated_at = CURRENT_TIMESTAMP
        """, parameters=tuple(row))


def export_ml_dataset(**context):
    """
    Stage 6: Export v_ml_ready_dataset to a CSV file for ML consumption.
    Stored in data/processed/ partitioned by date.
    """
    import pandas as pd, logging

    execution_date = context["ds"]
    pg_hook = PostgresHook(postgres_conn_id=DW_CONN_ID)

    df = pg_hook.get_pandas_df("""
        SELECT * FROM warehouse.v_ml_ready_dataset
        WHERE agg_date >= CURRENT_DATE - INTERVAL '90 days'
        ORDER BY agg_date, product_id, store_id
    """)

    out_dir = os.path.join(DATA_LAKE_BASE, "processed", "ml_ready",
                           f"dt={execution_date}")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "demand_features.csv")
    df.to_csv(out_path, index=False)

    logging.info(f"ML dataset exported: {out_path} ({len(df):,} rows)")
    context["ti"].xcom_push(key="ml_dataset_path", value=out_path)


def send_pipeline_summary(**context):
    """Stage 7: Log pipeline completion summary."""
    import logging

    pg_hook = PostgresHook(postgres_conn_id=DW_CONN_ID)
    row = pg_hook.get_first("""
        SELECT COUNT(*), SUM(total_quantity), SUM(total_revenue)
        FROM warehouse.agg_daily_demand
        WHERE agg_date = CURRENT_DATE - INTERVAL '1 day'
    """)
    logging.info(
        f"Pipeline Summary | Date: {context['ds']} | "
        f"Records: {row[0]:,} | Units: {row[1]:,} | Revenue: ${row[2]:,.2f}"
    )


# ─── DAG DEFINITION ───────────────────────────────────────────────────────────
with DAG(
    dag_id="retail_demand_batch_pipeline",
    default_args=default_args,
    description="Daily batch pipeline for retail demand forecasting",
    schedule_interval="0 2 * * *",           # 02:00 UTC daily
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["retail", "demand-forecasting", "batch"],
    doc_md=__doc__,
) as dag:

    # ── START ──────────────────────────────────────────────────────────────────
    start = EmptyOperator(task_id="start")

    # ── STAGE 1: EXTRACT ───────────────────────────────────────────────────────
    extract_pos = PythonOperator(
        task_id="extract_pos",
        python_callable=extract_pos_to_lake,
    )

    extract_ecom = PythonOperator(
        task_id="extract_ecommerce",
        python_callable=extract_ecommerce_to_lake,
    )

    # ── STAGE 2: DATA QUALITY ──────────────────────────────────────────────────
    dq_check = BranchPythonOperator(
        task_id="data_quality_check",
        python_callable=run_data_quality_checks,
    )

    alert_dq_failure = BashOperator(
        task_id="alert_dq_failure",
        bash_command='echo "DQ FAILURE on {{ ds }}" | mail -s "Pipeline DQ Alert" alerts@retailpipeline.com || true',
    )

    # ── STAGE 3: TRANSFORM (PySpark) ───────────────────────────────────────────
    transform_pos = BashOperator(
        task_id="transform_pos_data",
        bash_command=(
            f"spark-submit "
            f"--master {SPARK_MASTER} "
            f"--packages org.postgresql:postgresql:42.7.1 "
            f"/opt/airflow/spark/transformations/pos_transformer.py "
            f"--execution-date {{{{ ds }}}} "
            f"--data-lake {DATA_LAKE_BASE}"
        ),
    )

    transform_ecom = BashOperator(
        task_id="transform_ecommerce_data",
        bash_command=(
            f"spark-submit "
            f"--master {SPARK_MASTER} "
            f"/opt/airflow/spark/transformations/ecommerce_transformer.py "
            f"--execution-date {{{{ ds }}}} "
            f"--data-lake {DATA_LAKE_BASE}"
        ),
    )

    # ── STAGE 4: LOAD DIMENSIONS ───────────────────────────────────────────────
    load_product_dim = PythonOperator(
        task_id="load_dim_product",
        python_callable=load_dim_product,
    )

    load_store_dim = PythonOperator(
        task_id="load_dim_store",
        python_callable=load_dim_store,
    )

    # ── STAGE 5: AGGREGATE ─────────────────────────────────────────────────────
    aggregate_daily = PostgresOperator(
        task_id="aggregate_daily_demand",
        postgres_conn_id=DW_CONN_ID,
        sql="sql/aggregations/demand_aggregations.sql",
        split_statements=True,
    )

    # ── STAGE 6: EXPORT ────────────────────────────────────────────────────────
    export_ml = PythonOperator(
        task_id="export_ml_dataset",
        python_callable=export_ml_dataset,
    )

    # ── STAGE 7: SUMMARY ───────────────────────────────────────────────────────
    pipeline_summary = PythonOperator(
        task_id="pipeline_summary",
        python_callable=send_pipeline_summary,
    )

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    # ─── TASK DEPENDENCIES ────────────────────────────────────────────────────
    start >> [extract_pos, extract_ecom]
    extract_pos >> dq_check >> [alert_dq_failure, transform_pos]
    extract_ecom >> transform_ecom
    transform_pos >> [load_product_dim, load_store_dim]
    [load_product_dim, load_store_dim, transform_ecom] >> aggregate_daily
    aggregate_daily >> export_ml >> pipeline_summary >> end
    alert_dq_failure >> end
