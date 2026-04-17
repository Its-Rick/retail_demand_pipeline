"""
setup_airflow_connections.py
-----------------------------
Programmatically creates all required Airflow connections and Variables.
Run this once after docker-compose brings Airflow up.

Usage:
    python scripts/setup_airflow_connections.py

Or inside Docker:
    docker exec retail_airflow_web python /opt/airflow/scripts/setup_airflow_connections.py
"""

import os
import logging

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def setup_connections():
    """Create Airflow connections for PostgreSQL and Kafka."""
    from airflow.models import Connection
    from airflow import settings

    session = settings.Session()

    connections = [
        Connection(
            conn_id="postgres_retail_dw",
            conn_type="postgres",
            host=os.getenv("PG_HOST",     "postgres"),
            schema=os.getenv("PG_DBNAME", "retail_dw"),
            login=os.getenv("PG_USER",    "airflow"),
            password=os.getenv("PG_PASSWORD", "airflow"),
            port=int(os.getenv("PG_PORT", "5432")),
        ),
        Connection(
            conn_id="postgres_airflow_meta",
            conn_type="postgres",
            host=os.getenv("PG_HOST",     "postgres"),
            schema="airflow",
            login=os.getenv("PG_USER",    "airflow"),
            password=os.getenv("PG_PASSWORD", "airflow"),
            port=int(os.getenv("PG_PORT", "5432")),
        ),
    ]

    for conn in connections:
        existing = session.query(Connection).filter_by(conn_id=conn.conn_id).first()
        if existing:
            session.delete(existing)
            log.info(f"Replaced connection: {conn.conn_id}")
        session.add(conn)
        log.info(f"Created connection: {conn.conn_id}")

    session.commit()
    session.close()


def setup_variables():
    """Create Airflow Variables used by the DAGs."""
    from airflow.models import Variable

    variables = {
        "DATA_LAKE_PATH":  os.getenv("DATA_LAKE_PATH",  "/opt/airflow/data"),
        "SPARK_HOME":      os.getenv("SPARK_HOME",      "/opt/spark"),
        "SPARK_MASTER":    os.getenv("SPARK_MASTER",    "local[2]"),
        "ALERT_EMAIL":     os.getenv("ALERT_EMAIL",     "alerts@retailpipeline.com"),
        "DQ_NULL_THRESHOLD": "0.99",
    }

    for key, value in variables.items():
        Variable.set(key, value)
        log.info(f"Set Variable: {key} = {value}")


if __name__ == "__main__":
    try:
        setup_connections()
        setup_variables()
        log.info("✅ Airflow connections and variables configured successfully")
    except ImportError:
        log.error(
            "Airflow not installed or not in PYTHONPATH.\n"
            "Run inside the Airflow container:\n"
            "  docker exec retail_airflow_web python /opt/airflow/scripts/setup_airflow_connections.py"
        )
