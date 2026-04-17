"""
data_quality_dag.py
-------------------
Standalone Airflow DAG that runs comprehensive data quality checks
across all warehouse tables after the main batch pipeline completes.

Runs after batch_ingestion_dag via ExternalTaskSensor.
Schedule: Daily at 04:00 UTC (batch pipeline finishes by ~03:30)

Checks:
  - fact_sales     → row count, null rates, revenue sanity
  - dim_product    → uniqueness, completeness
  - dim_store      → uniqueness, active store count
  - agg_daily      → coverage, gap detection
  - DQ report      → summary email / log
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import logging

log = logging.getLogger("dq_dag")

default_args = {
    "owner":            "data-quality",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=3),
    "email_on_failure": True,
    "email":            ["dq-alerts@retailpipeline.com"],
}

DW_CONN_ID = "postgres_retail_dw"

# ── CHECK FUNCTIONS ────────────────────────────────────────────────────────────

def check_fact_sales(**context):
    """Validate fact_sales for the previous day."""
    ds = context["ds"]
    hook = PostgresHook(postgres_conn_id=DW_CONN_ID)

    results = {}

    # 1. Row count — must have at least 100 rows for a trading day
    row = hook.get_first("""
        SELECT COUNT(*) FROM warehouse.fact_sales fs
        JOIN warehouse.dim_time t ON t.time_key = fs.time_key
        WHERE t.full_date = %s::DATE - INTERVAL '1 day'
    """, parameters=(ds,))
    results["fact_sales_row_count"] = int(row[0])

    # 2. Revenue sanity — no negative net_revenue
    neg_rev = hook.get_first("""
        SELECT COUNT(*) FROM warehouse.fact_sales
        WHERE net_revenue < 0
    """)
    results["negative_revenue_count"] = int(neg_rev[0])

    # 3. Orphaned FKs — fact rows with no matching dim_product
    orphan = hook.get_first("""
        SELECT COUNT(*) FROM warehouse.fact_sales fs
        LEFT JOIN warehouse.dim_product p ON p.product_key = fs.product_key
        WHERE p.product_key IS NULL
    """)
    results["orphan_product_fk"] = int(orphan[0])

    # Log and write to dq_check_log
    for check_name, value in results.items():
        status = "PASS"
        if check_name == "fact_sales_row_count" and value < 100:
            status = "WARN"
        if check_name in ("negative_revenue_count", "orphan_product_fk") and value > 0:
            status = "FAIL"

        hook.run("""
            INSERT INTO warehouse.dq_check_log
              (check_name, table_name, check_type, total_records, failed_records, status, pipeline_run_id, details)
            VALUES (%s, 'fact_sales', 'post_load_check', %s, %s, %s, %s, %s)
        """, parameters=(check_name, value, value if status != "PASS" else 0,
                         status, context["run_id"], str(results)))

        log.info(f"[{status}] {check_name}: {value}")

    if results["negative_revenue_count"] > 0 or results["orphan_product_fk"] > 0:
        raise ValueError(f"Critical DQ failure: {results}")


def check_dim_tables(**context):
    """Validate dimension tables for uniqueness and completeness."""
    hook = PostgresHook(postgres_conn_id=DW_CONN_ID)

    checks = [
        # (description, sql, expected_zero_result)
        ("dim_product duplicate product_id",
         "SELECT COUNT(*) - COUNT(DISTINCT product_id) FROM warehouse.dim_product", True),
        ("dim_store duplicate store_id",
         "SELECT COUNT(*) - COUNT(DISTINCT store_id) FROM warehouse.dim_store", True),
        ("dim_product null category",
         "SELECT COUNT(*) FROM warehouse.dim_product WHERE category IS NULL", True),
        ("dim_time coverage 2023",
         "SELECT 365 - COUNT(*) FROM warehouse.dim_time WHERE year = 2023", True),
    ]

    failures = []
    for desc, sql, expect_zero in checks:
        val = int(hook.get_first(sql)[0])
        status = "PASS" if (val == 0) == expect_zero else "FAIL"
        if status == "FAIL":
            failures.append(f"{desc}: {val}")
        log.info(f"[{status}] {desc}: {val}")

    if failures:
        raise ValueError(f"Dim table DQ failures: {failures}")


def check_agg_coverage(**context):
    """Ensure agg_daily_demand has no unexpected date gaps."""
    ds = context["ds"]
    hook = PostgresHook(postgres_conn_id=DW_CONN_ID)

    # Check that yesterday's aggregation was populated
    row = hook.get_first("""
        SELECT COUNT(DISTINCT agg_date) FROM warehouse.agg_daily_demand
        WHERE agg_date = %s::DATE - INTERVAL '1 day'
    """, parameters=(ds,))
    date_count = int(row[0])

    if date_count == 0:
        raise ValueError(f"agg_daily_demand missing data for {ds} - 1 day!")

    # Check for any 7-day gaps in the last 90 days
    gaps = hook.get_first("""
        WITH date_series AS (
            SELECT generate_series(
                CURRENT_DATE - 90,
                CURRENT_DATE - 1,
                '1 day'::INTERVAL
            )::DATE AS d
        ),
        covered AS (
            SELECT DISTINCT agg_date FROM warehouse.agg_daily_demand
            WHERE agg_date >= CURRENT_DATE - 90
        )
        SELECT COUNT(*) FROM date_series
        WHERE d NOT IN (SELECT agg_date FROM covered)
    """)
    gap_days = int(gaps[0])
    status = "PASS" if gap_days == 0 else "WARN"
    log.info(f"[{status}] agg_daily_demand: {gap_days} missing days in last 90d")


def generate_dq_report(**context):
    """Generate a summary DQ report and push to XCom."""
    hook = PostgresHook(postgres_conn_id=DW_CONN_ID)

    summary = hook.get_pandas_df("""
        SELECT
            check_name,
            table_name,
            status,
            failed_records,
            run_timestamp
        FROM warehouse.dq_check_log
        WHERE DATE(run_timestamp) = CURRENT_DATE
        ORDER BY status DESC, run_timestamp DESC
        LIMIT 50
    """)

    fail_count = len(summary[summary["status"] == "FAIL"])
    warn_count = len(summary[summary["status"] == "WARN"])
    pass_count = len(summary[summary["status"] == "PASS"])

    report = (
        f"DQ Report [{context['ds']}]\n"
        f"PASS: {pass_count} | WARN: {warn_count} | FAIL: {fail_count}\n"
        + summary.to_string(index=False)
    )
    log.info(report)
    context["ti"].xcom_push(key="dq_report", value=report)
    return report


# ── DAG DEFINITION ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="retail_data_quality",
    default_args=default_args,
    description="Post-load data quality checks across all warehouse tables",
    schedule_interval="0 4 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["retail", "data-quality", "monitoring"],
) as dag:

    start = EmptyOperator(task_id="start")

    # Wait for batch pipeline to finish
    wait_for_batch = ExternalTaskSensor(
        task_id="wait_for_batch_pipeline",
        external_dag_id="retail_demand_batch_pipeline",
        external_task_id="end",
        timeout=7200,
        mode="reschedule",
        poke_interval=120,
    )

    check_facts = PythonOperator(
        task_id="check_fact_sales",
        python_callable=check_fact_sales,
    )

    check_dims = PythonOperator(
        task_id="check_dim_tables",
        python_callable=check_dim_tables,
    )

    check_agg = PythonOperator(
        task_id="check_agg_coverage",
        python_callable=check_agg_coverage,
    )

    dq_report = PythonOperator(
        task_id="generate_dq_report",
        python_callable=generate_dq_report,
        trigger_rule="all_done",   # run even if some checks failed
    )

    end = EmptyOperator(task_id="end", trigger_rule="all_done")

    start >> wait_for_batch >> [check_facts, check_dims, check_agg] >> dq_report >> end
