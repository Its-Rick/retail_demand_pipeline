"""
test_integration.py
--------------------
End-to-end integration tests for the Retail Demand Pipeline.
These tests require:
  - PostgreSQL running and accessible
  - Sample data generated (run scripts/generate_sample_data.py first)

Mark: pytest -m integration

Skip in CI without infrastructure:
    pytest tests/ -m "not integration"
"""

import os
import json
import pytest
import pandas as pd
from datetime import datetime, date


# ─── FIXTURES ─────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def db_conn():
    """PostgreSQL connection fixture — skips tests if DB not available."""
    psycopg2 = pytest.importorskip("psycopg2")
    dsn = os.getenv("DATABASE_URL",
                    "postgresql://airflow:airflow@localhost:5432/retail_dw")
    try:
        conn = psycopg2.connect(dsn, connect_timeout=5)
        yield conn
        conn.close()
    except Exception as e:
        pytest.skip(f"PostgreSQL not available: {e}")


@pytest.fixture(scope="module")
def sample_data_dir():
    """Path to sample data — skips if not generated."""
    path = os.path.join(os.path.dirname(__file__), "..", "data", "sample")
    if not os.path.isdir(path) or not os.listdir(path):
        pytest.skip("Sample data not generated. Run: python scripts/generate_sample_data.py")
    return path


# ─── DATABASE SCHEMA TESTS ────────────────────────────────────────────────────

@pytest.mark.integration
class TestDatabaseSchema:

    def test_warehouse_schema_exists(self, db_conn):
        """warehouse schema must exist."""
        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT schema_name FROM information_schema.schemata
                WHERE schema_name = 'warehouse'
            """)
            assert cur.fetchone() is not None

    def test_all_tables_exist(self, db_conn):
        """All required tables must be created."""
        required = [
            "fact_sales", "dim_time", "dim_product",
            "dim_store", "agg_daily_demand", "agg_weekly_demand",
            "stream_ecommerce_events", "dq_check_log",
        ]
        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT tablename FROM pg_tables
                WHERE schemaname = 'warehouse'
            """)
            existing = {r[0] for r in cur.fetchall()}

        missing = set(required) - existing
        assert not missing, f"Missing tables: {missing}"

    def test_dim_time_populated(self, db_conn):
        """dim_time should have 365+ rows for 2023."""
        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM warehouse.dim_time
                WHERE year = 2023
            """)
            count = cur.fetchone()[0]
        assert count >= 365, f"Expected 365 rows in dim_time for 2023, got {count}"

    def test_dim_time_no_gaps(self, db_conn):
        """dim_time should have no date gaps."""
        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT MAX(full_date) - MIN(full_date) + 1 AS expected_days,
                       COUNT(*) AS actual_days
                FROM warehouse.dim_time
                WHERE year BETWEEN 2020 AND 2025
            """)
            row = cur.fetchone()
        assert row[0] == row[1], (
            f"Date gaps in dim_time: expected {row[0]} rows, have {row[1]}"
        )

    def test_fact_sales_partitions_exist(self, db_conn):
        """Quarterly partitions for fact_sales should exist."""
        with db_conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM pg_tables
                WHERE tablename LIKE 'fact_sales_2023_%'
                AND schemaname = 'warehouse'
            """)
            count = cur.fetchone()[0]
        assert count >= 4, f"Expected 4 quarterly partitions, found {count}"

    def test_dq_log_table_writable(self, db_conn):
        """Should be able to insert and query dq_check_log."""
        with db_conn.cursor() as cur:
            cur.execute("""
                INSERT INTO warehouse.dq_check_log
                  (check_name, table_name, check_type, total_records,
                   passed_records, failed_records, pass_rate, status, pipeline_run_id)
                VALUES ('integration_test', 'test_table', 'smoke_test',
                        100, 100, 0, 1.0, 'PASS', 'test_run_001')
                RETURNING log_id
            """)
            log_id = cur.fetchone()[0]
            db_conn.commit()
        assert log_id > 0


# ─── DATA PIPELINE INTEGRATION TESTS ─────────────────────────────────────────

@pytest.mark.integration
class TestDataPipelineIntegration:

    def test_pos_csv_loads_to_dataframe(self, sample_data_dir):
        """POS CSV must load cleanly without type errors."""
        path = os.path.join(sample_data_dir, "pos_sales.csv")
        df = pd.read_csv(path)
        assert len(df) > 10_000
        assert df["quantity"].dtype in ["int64", "float64"]
        assert df["unit_price"].dtype in ["float64"]
        assert df["transaction_date"].dtype == object   # string before cast

    def test_ecom_json_parses(self, sample_data_dir):
        """E-commerce JSON must parse cleanly."""
        path = os.path.join(sample_data_dir, "ecommerce_events.json")
        with open(path) as f:
            events = json.load(f)
        assert len(events) > 5_000
        for key in ["event_id", "event_type", "product_id", "event_timestamp"]:
            assert key in events[0], f"Missing key: {key}"

    def test_weather_joins_to_pos(self, sample_data_dir):
        """Weather data should join to POS data on date + state."""
        pos = pd.read_csv(os.path.join(sample_data_dir, "pos_sales.csv"))
        weather = pd.read_csv(os.path.join(sample_data_dir, "weather.csv"))

        pos["transaction_date"] = pd.to_datetime(pos["transaction_date"])
        weather["date"] = pd.to_datetime(weather["date"])

        merged = pos.merge(
            weather,
            left_on=["transaction_date", "state"],
            right_on=["date", "state"],
            how="left"
        )
        # At least 50% of rows should find a weather match
        match_rate = merged["temp_high_f"].notna().mean()
        assert match_rate > 0.5, f"Too few weather matches: {match_rate:.1%}"

    def test_dim_product_upsert(self, db_conn):
        """Dim product upsert should handle new and existing records."""
        test_product = {
            "product_id": "TEST_INTEGRATION_001",
            "product_name": "Integration Test Product",
            "category": "Test",
            "brand": "TestBrand",
            "unit_price": 9.99,
        }
        with db_conn.cursor() as cur:
            # First insert
            cur.execute("""
                INSERT INTO warehouse.dim_product
                    (product_id, product_name, category, brand, unit_price)
                VALUES (%(product_id)s, %(product_name)s, %(category)s,
                        %(brand)s, %(unit_price)s)
                ON CONFLICT (product_id) DO UPDATE SET
                    unit_price = EXCLUDED.unit_price,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING product_key
            """, test_product)
            key1 = cur.fetchone()[0]

            # Update — should return same key
            test_product["unit_price"] = 12.99
            cur.execute("""
                INSERT INTO warehouse.dim_product
                    (product_id, product_name, category, brand, unit_price)
                VALUES (%(product_id)s, %(product_name)s, %(category)s,
                        %(brand)s, %(unit_price)s)
                ON CONFLICT (product_id) DO UPDATE SET
                    unit_price = EXCLUDED.unit_price,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING product_key
            """, test_product)
            key2 = cur.fetchone()[0]

            # Verify price updated
            cur.execute("""
                SELECT unit_price FROM warehouse.dim_product
                WHERE product_id = 'TEST_INTEGRATION_001'
            """)
            price = float(cur.fetchone()[0])

            # Cleanup
            cur.execute("DELETE FROM warehouse.dim_product WHERE product_id = 'TEST_INTEGRATION_001'")
            db_conn.commit()

        assert key1 == key2, "Upsert should return same surrogate key"
        assert abs(price - 12.99) < 0.01, "Price should be updated to 12.99"


# ─── MONITORING INTEGRATION TESTS ─────────────────────────────────────────────

@pytest.mark.integration
class TestMonitoringIntegration:

    def test_health_check_postgres(self, db_conn):
        """Health check for PostgreSQL should return healthy."""
        from monitoring.monitoring import check_postgres_health
        dsn = os.getenv("DATABASE_URL",
                        "postgresql://airflow:airflow@localhost:5432/retail_dw")
        result = check_postgres_health(dsn)
        assert result["status"] == "healthy"
        assert "latency_ms" in result

    def test_metrics_write_to_disk(self, tmp_path):
        """PipelineMetrics.finish() should write to disk."""
        import time
        from monitoring.monitoring import PipelineMetrics, PrometheusExporter

        m = PipelineMetrics("integration_test")
        m.record("rows_processed", 1000)
        m.finish("success")

        exp = PrometheusExporter()
        exp.METRICS_PATH = str(tmp_path / "test.prom")
        exp.write(m)

        assert os.path.exists(exp.METRICS_PATH)
        content = open(exp.METRICS_PATH).read()
        assert "rows_processed" in content
        assert "1000" in content
