"""
conftest.py
-----------
Shared pytest fixtures and configuration for the Retail Demand Pipeline tests.
Auto-loaded by pytest from the tests/ directory.
"""

import os
import sys
import json
import pytest
import pandas as pd

# Add project root to path so imports work without install
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


# ─── SAMPLE DATA FIXTURES ─────────────────────────────────────────────────────

SAMPLE_DIR = os.path.join(ROOT, "data", "sample")


@pytest.fixture(scope="session")
def sample_pos_df():
    """Load sample POS CSV as a pandas DataFrame (session-scoped for speed)."""
    path = os.path.join(SAMPLE_DIR, "pos_sales.csv")
    if not os.path.exists(path):
        pytest.skip("Run scripts/generate_sample_data.py first")
    return pd.read_csv(path)


@pytest.fixture(scope="session")
def sample_ecom_events():
    """Load sample e-commerce events as a list of dicts."""
    path = os.path.join(SAMPLE_DIR, "ecommerce_events.json")
    if not os.path.exists(path):
        pytest.skip("Run scripts/generate_sample_data.py first")
    with open(path) as f:
        return json.load(f)


@pytest.fixture(scope="session")
def sample_weather_df():
    """Load sample weather CSV."""
    path = os.path.join(SAMPLE_DIR, "weather.csv")
    if not os.path.exists(path):
        pytest.skip("Run scripts/generate_sample_data.py first")
    return pd.read_csv(path)


@pytest.fixture(scope="session")
def sample_holidays_df():
    """Load sample holidays CSV."""
    path = os.path.join(SAMPLE_DIR, "holidays.csv")
    if not os.path.exists(path):
        pytest.skip("Run scripts/generate_sample_data.py first")
    return pd.read_csv(path)


# ─── SPARK FIXTURE ────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark_session():
    """
    Session-scoped SparkSession for all Spark tests.
    Reuses the same session to avoid repeated JVM startup (~10s overhead).
    """
    pyspark = pytest.importorskip("pyspark", reason="PySpark not installed")
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("RetailPipeline_TestSuite")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


# ─── DATABASE FIXTURE ─────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def pg_conn():
    """
    Session-scoped PostgreSQL connection.
    Tests using this fixture are automatically skipped if DB is unavailable.
    """
    psycopg2 = pytest.importorskip("psycopg2", reason="psycopg2 not installed")
    dsn = os.getenv("DATABASE_URL",
                    "postgresql://airflow:airflow@localhost:5432/retail_dw")
    try:
        conn = psycopg2.connect(dsn, connect_timeout=3)
        yield conn
        conn.close()
    except Exception as e:
        pytest.skip(f"PostgreSQL not reachable: {e}")


# ─── MOCK KAFKA FIXTURE ───────────────────────────────────────────────────────

@pytest.fixture
def mock_kafka_producer(monkeypatch):
    """Monkeypatch KafkaProducer to avoid real broker connections in unit tests."""
    from unittest.mock import MagicMock
    mock = MagicMock()
    monkeypatch.setattr("kafka.KafkaProducer", lambda **kwargs: mock)
    return mock


@pytest.fixture
def mock_kafka_consumer(monkeypatch):
    """Monkeypatch KafkaConsumer."""
    from unittest.mock import MagicMock
    mock = MagicMock()
    monkeypatch.setattr("kafka.KafkaConsumer", lambda *args, **kwargs: mock)
    return mock


# ─── HELPER UTILITIES ─────────────────────────────────────────────────────────

def make_pos_row(**overrides):
    """Create a minimal valid POS row dict for testing."""
    row = {
        "transaction_id":   "TXN_TEST_001",
        "transaction_date": "2023-06-15",
        "store_id":         "S001",
        "product_id":       "P001",
        "product_name":     "Test Product",
        "category":         "Electronics",
        "brand":            "TestBrand",
        "unit_price":       29.99,
        "quantity":         2,
        "discount":         0.0,
        "total_amount":     59.98,
        "payment_method":   "Credit",
        "customer_id":      "C1234",
        "state":            "CA",
        "region":           "West",
    }
    row.update(overrides)
    return row


def make_ecom_event(**overrides):
    """Create a minimal valid e-commerce event dict for testing."""
    event = {
        "event_id":        "EVT_TEST_001",
        "event_type":      "product_view",
        "event_timestamp": "2023-06-15T10:30:00Z",
        "session_id":      "SES_001",
        "user_id":         "U12345",
        "product_id":      "P001",
        "category":        "Electronics",
        "price":           29.99,
        "quantity":        None,
        "device":          "mobile",
        "platform":        "ios",
        "referrer":        "google",
        "geo":             {"country": "US", "state": "CA"},
    }
    event.update(overrides)
    return event
