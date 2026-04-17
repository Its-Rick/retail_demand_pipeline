"""
backfill_pipeline.py
--------------------
Triggers historical backfill of the batch pipeline for a date range.
Useful for initial data load or re-processing after a bug fix.

Supports two modes:
  - airflow: triggers Airflow DAG runs for each date
  - local:   runs PySpark transformations directly (no Airflow needed)

Usage:
    # Airflow mode (requires Airflow running)
    python scripts/backfill_pipeline.py \
        --start 2023-01-01 --end 2023-12-31 --mode airflow

    # Local mode (runs PySpark directly)
    python scripts/backfill_pipeline.py \
        --start 2023-11-01 --end 2023-11-30 --mode local
"""

import argparse
import logging
import subprocess
import sys
import time
import os
from datetime import datetime, timedelta

log = logging.getLogger("backfill")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)

DATA_LAKE = os.getenv("DATA_LAKE_PATH", "./data")
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[2]")
PG_JDBC = os.getenv("PG_JDBC_URL", "jdbc:postgresql://localhost:5432/retail_dw")


def date_range(start: str, end: str):
    """Yield date strings from start to end inclusive."""
    current = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")
    while current <= end_dt:
        yield current.strftime("%Y-%m-%d")
        current += timedelta(days=1)


def backfill_airflow(start: str, end: str, dag_id: str):
    """Trigger Airflow DAG runs for each date in the range."""
    log.info(f"Airflow backfill: {dag_id} | {start} → {end}")
    cmd_base = ["airflow", "dags", "trigger", dag_id, "--conf", '{}', "--run-id"]

    for ds in date_range(start, end):
        run_id = f"backfill__{ds}"
        cmd = cmd_base + [run_id, "--exec-date", ds]
        log.info(f"Triggering: {ds}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            log.error(f"Failed to trigger {ds}: {result.stderr}")
        else:
            log.info(f"  ✅ Triggered run: {run_id}")
        time.sleep(2)   # avoid overwhelming the scheduler


def backfill_local(start: str, end: str):
    """Run PySpark transformers directly for each date."""
    log.info(f"Local backfill: {start} → {end}")
    dates = list(date_range(start, end))
    total = len(dates)

    for i, ds in enumerate(dates, 1):
        log.info(f"[{i}/{total}] Processing {ds}...")

        # Run POS transformer
        pos_cmd = [
            "spark-submit",
            "--master", SPARK_MASTER,
            "--packages", "org.postgresql:postgresql:42.7.1",
            "spark/transformations/pos_transformer.py",
            "--execution-date", ds,
            "--data-lake", DATA_LAKE,
        ]
        result = subprocess.run(pos_cmd, capture_output=True, text=True)
        if result.returncode != 0:
            log.error(f"POS transformer failed for {ds}: {result.stderr[-500:]}")
            continue

        # Run e-commerce transformer
        ecom_cmd = [
            "spark-submit",
            "--master", SPARK_MASTER,
            "spark/transformations/ecommerce_transformer.py",
            "--execution-date", ds,
            "--data-lake", DATA_LAKE,
        ]
        result = subprocess.run(ecom_cmd, capture_output=True, text=True)
        if result.returncode != 0:
            log.warning(f"E-commerce transformer failed for {ds} (non-fatal): {result.stderr[-300:]}")

        log.info(f"  ✅ {ds} complete")

    log.info(f"\nBackfill complete: {total} dates processed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Retail Pipeline Backfill")
    parser.add_argument("--start",  required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end",    required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--mode",   choices=["airflow", "local"], default="local")
    parser.add_argument("--dag-id", default="retail_demand_batch_pipeline")
    args = parser.parse_args()

    try:
        if args.mode == "airflow":
            backfill_airflow(args.start, args.end, args.dag_id)
        else:
            backfill_local(args.start, args.end)
    except KeyboardInterrupt:
        log.info("Backfill interrupted by user")
        sys.exit(0)
