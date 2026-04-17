"""
custom_operators.py
-------------------
Reusable custom Airflow operators for the Retail Demand Pipeline.

Operators:
  - SparkSubmitWithMonitoringOperator  → wraps spark-submit with timing + log capture
  - DataLakePartitionSensor            → waits for a data lake partition to appear
  - PostgresUpsertOperator             → bulk upsert from pandas DataFrame to PG
  - SlackAlertOperator                 → sends pipeline alerts to Slack webhook
"""

import os
import time
import logging
import subprocess
from typing import Optional, Dict, Any

import pandas as pd
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults

log = logging.getLogger("custom_operators")


# ── 1. SPARK SUBMIT WITH MONITORING ───────────────────────────────────────────

class SparkSubmitWithMonitoringOperator(BaseOperator):
    """
    Submits a PySpark script via spark-submit, captures stdout/stderr,
    measures wall-clock runtime, and pushes metrics to XCom.

    Args:
        script_path:   Path to the .py PySpark script
        spark_master:  Spark master URL (default: local[*])
        packages:      Maven packages to include (e.g. postgres JDBC)
        script_args:   Additional CLI args to pass to the script
        env_vars:      Extra environment variables for the spark process
    """

    template_fields = ("script_path", "script_args")
    ui_color = "#f5a623"

    @apply_defaults
    def __init__(
        self,
        script_path: str,
        spark_master: str = "local[*]",
        packages: str = "org.postgresql:postgresql:42.7.1",
        script_args: str = "",
        env_vars: Optional[Dict[str, str]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.script_path = script_path
        self.spark_master = spark_master
        self.packages = packages
        self.script_args = script_args
        self.env_vars = env_vars or {}

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        cmd = (
            f"spark-submit "
            f"--master {self.spark_master} "
            f"--packages {self.packages} "
            f"{self.script_path} "
            f"{self.script_args}"
        )
        self.log.info(f"Executing: {cmd}")

        env = {**os.environ, **self.env_vars}
        start_ts = time.time()

        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, env=env
        )
        elapsed = round(time.time() - start_ts, 2)

        # Log spark output
        if result.stdout:
            self.log.info(f"STDOUT:\n{result.stdout[-5000:]}")   # tail 5k chars
        if result.stderr:
            self.log.warning(f"STDERR:\n{result.stderr[-3000:]}")

        metrics = {
            "return_code":    result.returncode,
            "elapsed_sec":    elapsed,
            "script":         self.script_path,
            "execution_date": context["ds"],
        }

        if result.returncode != 0:
            raise RuntimeError(
                f"spark-submit failed (rc={result.returncode}) after {elapsed}s\n"
                f"{result.stderr[-1000:]}"
            )

        self.log.info(f"Spark job completed in {elapsed}s")
        context["ti"].xcom_push(key="spark_metrics", value=metrics)
        return metrics


# ── 2. DATA LAKE PARTITION SENSOR ─────────────────────────────────────────────

class DataLakePartitionSensor(BaseOperator):
    """
    Waits for a Hive-style partition directory to exist in the Data Lake.
    Useful when upstream jobs write partitioned data and downstream
    tasks must not start until the partition is confirmed present.

    Args:
        base_path:    Root of the data lake (e.g. /data/raw/pos)
        partition_dt: Date string (YYYY-MM-DD) to build the partition path
        timeout_sec:  Max seconds to wait before failing (default: 3600)
        poke_interval: Seconds between checks (default: 60)
    """

    template_fields = ("base_path", "partition_dt")
    ui_color = "#9b59b6"

    @apply_defaults
    def __init__(
        self,
        base_path: str,
        partition_dt: str,
        timeout_sec: int = 3600,
        poke_interval: int = 60,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.base_path = base_path
        self.partition_dt = partition_dt
        self.timeout_sec = timeout_sec
        self.poke_interval = poke_interval

    def _partition_path(self) -> str:
        year, month, day = self.partition_dt.split("-")
        return os.path.join(
            self.base_path,
            f"year={year}", f"month={month}", f"day={day}"
        )

    def execute(self, context: Dict[str, Any]) -> str:
        path = self._partition_path()
        deadline = time.time() + self.timeout_sec
        self.log.info(f"Waiting for partition: {path}")

        while time.time() < deadline:
            if os.path.isdir(path) and os.listdir(path):
                self.log.info(f"Partition found: {path}")
                return path
            self.log.info(f"Partition not ready — sleeping {self.poke_interval}s")
            time.sleep(self.poke_interval)

        raise TimeoutError(
            f"Partition not found after {self.timeout_sec}s: {path}"
        )


# ── 3. POSTGRES UPSERT OPERATOR ────────────────────────────────────────────────

class PostgresUpsertOperator(BaseOperator):
    """
    Bulk-upserts a pandas DataFrame into a PostgreSQL table.
    Uses ON CONFLICT DO UPDATE (UPSERT) via psycopg2 execute_batch.

    Args:
        conn_id:        Airflow connection ID for PostgreSQL
        table:          Target table name (schema.table)
        dataframe_fn:   Callable that returns a pd.DataFrame (receives context)
        conflict_cols:  Columns that form the unique constraint
        update_cols:    Columns to update on conflict (None = all non-conflict cols)
        batch_size:     Rows per INSERT batch (default: 1000)
    """

    template_fields = ("table",)
    ui_color = "#27ae60"

    @apply_defaults
    def __init__(
        self,
        conn_id: str,
        table: str,
        dataframe_fn,
        conflict_cols: list,
        update_cols: Optional[list] = None,
        batch_size: int = 1000,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.table = table
        self.dataframe_fn = dataframe_fn
        self.conflict_cols = conflict_cols
        self.update_cols = update_cols
        self.batch_size = batch_size

    def execute(self, context: Dict[str, Any]) -> int:
        from psycopg2.extras import execute_batch

        df = self.dataframe_fn(context)
        if df.empty:
            self.log.info("Empty DataFrame — nothing to upsert")
            return 0

        cols = list(df.columns)
        update_cols = self.update_cols or [c for c in cols if c not in self.conflict_cols]

        placeholders = ", ".join([f"%({c})s" for c in cols])
        conflict_target = ", ".join(self.conflict_cols)
        update_clause = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])

        sql = f"""
            INSERT INTO {self.table} ({', '.join(cols)})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_target})
            DO UPDATE SET {update_clause}
        """

        hook = PostgresHook(postgres_conn_id=self.conn_id)
        conn = hook.get_conn()
        records = df.to_dict(orient="records")

        with conn.cursor() as cur:
            execute_batch(cur, sql, records, page_size=self.batch_size)
        conn.commit()
        conn.close()

        self.log.info(f"Upserted {len(records):,} rows into {self.table}")
        return len(records)


# ── 4. SLACK ALERT OPERATOR ────────────────────────────────────────────────────

class SlackAlertOperator(BaseOperator):
    """
    Sends a pipeline status message to a Slack webhook.
    Use in on_failure_callback or as an explicit DAG task.

    Args:
        webhook_url:  Slack incoming webhook URL (use Airflow Variable or Secret)
        message_fn:   Callable(context) → str message text
        icon_emoji:   Slack emoji icon for the message (default: :robot_face:)
    """

    ui_color = "#e74c3c"

    @apply_defaults
    def __init__(
        self,
        webhook_url: str,
        message_fn=None,
        icon_emoji: str = ":robot_face:",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.webhook_url = webhook_url
        self.message_fn = message_fn
        self.icon_emoji = icon_emoji

    def execute(self, context: Dict[str, Any]) -> None:
        import json
        import urllib.request

        if self.message_fn:
            text = self.message_fn(context)
        else:
            dag_id = context["dag"].dag_id
            task_id = context["task"].task_id
            ds = context["ds"]
            text = f"Pipeline alert: *{dag_id}* / `{task_id}` on {ds}"

        payload = json.dumps({
            "text": text,
            "icon_emoji": self.icon_emoji,
            "username": "Retail Pipeline Bot",
        }).encode()

        req = urllib.request.Request(
            self.webhook_url,
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        try:
            urllib.request.urlopen(req, timeout=10)
            self.log.info(f"Slack alert sent: {text[:100]}")
        except Exception as e:
            self.log.warning(f"Slack alert failed (non-blocking): {e}")
