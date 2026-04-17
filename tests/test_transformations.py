"""
test_transformations.py
-----------------------
Unit tests for PySpark ETL transformations.
Uses a local SparkSession (no cluster needed).

Run: pytest tests/ -v
"""

import pytest
import pandas as pd
from datetime import date
from unittest.mock import patch, MagicMock

# Only import Spark if available (skip gracefully in CI without Java)
pyspark = pytest.importorskip("pyspark", reason="PySpark not installed")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ─── FIXTURES ─────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    """Create a local SparkSession for testing."""
    spark = (
        SparkSession.builder
        .appName("RetailPipeline_Tests")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture
def raw_pos_df(spark):
    """Sample raw POS data with known issues for testing cleaning logic."""
    data = [
        # (txn_id, txn_date, store_id, product_id, quantity, unit_price, discount, total, category)
        ("TXN001", "2023-06-15", "S001", "P001", 3,  25.00, 0.10, 67.50,  "Electronics"),
        ("TXN002", "2023-06-15", "S001", "P002", 1, 150.00, 0.00, 150.00, "Clothing"),
        ("TXN001", "2023-06-15", "S001", "P001", 3,  25.00, 0.10, 67.50,  "Electronics"),  # duplicate
        ("TXN003", "2023-06-16", "S002", "P003", -1, 10.00, 0.00, -10.00, "Grocery"),     # bad qty
        (None,     "2023-06-16", "S002", "P004", 2,  30.00, 0.00, 60.00,  "Sports"),       # null txn_id
        ("TXN004", "bad-date",   "S003", "P005", 1,  20.00, 0.00, 20.00,  "Toys"),         # bad date
        ("TXN005", "2023-06-17", "S003", "P006", 2,  45.00, 0.05, 85.50,  "Grocery"),     # valid
    ]
    schema = "transaction_id STRING, transaction_date STRING, store_id STRING, " \
             "product_id STRING, quantity INT, unit_price DOUBLE, " \
             "discount DOUBLE, total_amount DOUBLE, category STRING"
    return spark.createDataFrame(data, schema)


# ─── TESTS ────────────────────────────────────────────────────────────────────

class TestPosCleaning:

    def test_removes_null_transaction_ids(self, spark, raw_pos_df):
        """Rows with null transaction_id must be dropped."""
        from spark.transformations.pos_transformer import clean_pos_data
        cleaned = clean_pos_data(raw_pos_df)
        null_count = cleaned.filter(F.col("transaction_id").isNull()).count()
        assert null_count == 0, "Null transaction_ids should be removed"

    def test_removes_duplicates(self, spark, raw_pos_df):
        """Duplicate transaction_ids should be deduplicated."""
        from spark.transformations.pos_transformer import clean_pos_data
        cleaned = clean_pos_data(raw_pos_df)
        txn_ids = [r["transaction_id"] for r in cleaned.collect() if r["transaction_id"]]
        assert len(txn_ids) == len(set(txn_ids)), "Duplicate transactions found after cleaning"

    def test_removes_negative_quantity(self, spark, raw_pos_df):
        """Rows with quantity <= 0 must be dropped."""
        from spark.transformations.pos_transformer import clean_pos_data
        cleaned = clean_pos_data(raw_pos_df)
        bad_qty = cleaned.filter(F.col("quantity") <= 0).count()
        assert bad_qty == 0

    def test_removes_bad_dates(self, spark, raw_pos_df):
        """Rows with unparseable dates must be dropped."""
        from spark.transformations.pos_transformer import clean_pos_data
        cleaned = clean_pos_data(raw_pos_df)
        null_dates = cleaned.filter(F.col("transaction_date").isNull()).count()
        assert null_dates == 0

    def test_net_revenue_calculation(self, spark, raw_pos_df):
        """net_revenue = quantity * unit_price * (1 - discount)"""
        from spark.transformations.pos_transformer import clean_pos_data
        cleaned = clean_pos_data(raw_pos_df)
        row = cleaned.filter(F.col("transaction_id") == "TXN005").first()
        if row:
            expected = round(2 * 45.00 * (1 - 0.05), 2)
            assert abs(row["net_revenue"] - expected) < 0.01

    def test_valid_rows_retained(self, spark, raw_pos_df):
        """TXN002 and TXN005 are valid — must survive cleaning."""
        from spark.transformations.pos_transformer import clean_pos_data
        cleaned = clean_pos_data(raw_pos_df)
        valid_ids = {r["transaction_id"] for r in cleaned.collect()}
        assert "TXN002" in valid_ids
        assert "TXN005" in valid_ids


class TestTimeEnrichment:

    def test_adds_day_of_week(self, spark):
        """day_of_week should be added as integer 1-7."""
        from spark.transformations.pos_transformer import enrich_with_time_features
        data = [("TXN001", date(2023, 6, 15))]  # Thursday
        df = spark.createDataFrame(data, "transaction_id STRING, transaction_date DATE")
        enriched = enrich_with_time_features(df)
        assert "day_of_week" in enriched.columns
        row = enriched.first()
        assert 1 <= row["day_of_week"] <= 7

    def test_adds_quarter(self, spark):
        """Quarter should be 1-4 based on month."""
        from spark.transformations.pos_transformer import enrich_with_time_features
        data = [("TXN001", date(2023, 11, 1))]  # November → Q4
        df = spark.createDataFrame(data, "transaction_id STRING, transaction_date DATE")
        enriched = enrich_with_time_features(df)
        assert enriched.first()["quarter"] == 4

    def test_weekend_flag(self, spark):
        """Saturday (dayofweek=7 in Spark) should be flagged as weekend."""
        from spark.transformations.pos_transformer import enrich_with_time_features
        data = [("TXN001", date(2023, 6, 17))]  # Saturday
        df = spark.createDataFrame(data, "transaction_id STRING, transaction_date DATE")
        enriched = enrich_with_time_features(df)
        assert enriched.first()["is_weekend"] is True


class TestAggregations:

    def test_daily_units_sum(self, spark):
        """Daily aggregation should sum quantities correctly."""
        data = [
            ("2023-06-15", "P001", "S001", 10),
            ("2023-06-15", "P001", "S001", 20),
            ("2023-06-15", "P002", "S001", 5),
        ]
        df = spark.createDataFrame(data, "date STRING, product_id STRING, store_id STRING, qty INT")
        agg = df.groupBy("date","product_id","store_id").agg(F.sum("qty").alias("total"))
        row = agg.filter(
            (F.col("product_id") == "P001") & (F.col("store_id") == "S001")
        ).first()
        assert row["total"] == 30


class TestDataQuality:

    def test_null_rate_check_passes(self, spark):
        """Null rate check should PASS when column is fully populated."""
        from spark.quality.data_quality_checks import DataQualityChecker
        data = [("TXN001",), ("TXN002",), ("TXN003",)]
        df = spark.createDataFrame(data, "transaction_id STRING")
        checker = DataQualityChecker(spark, df, "test_table")
        result = checker.check_null_rate("transaction_id", threshold=0.99)
        assert result.status == "PASS"

    def test_null_rate_check_fails(self, spark):
        """Null rate check should FAIL when too many nulls."""
        from spark.quality.data_quality_checks import DataQualityChecker
        data = [(None,), (None,), ("TXN003",)]
        df = spark.createDataFrame(data, "transaction_id STRING")
        checker = DataQualityChecker(spark, df, "test_table")
        result = checker.check_null_rate("transaction_id", threshold=0.99)
        assert result.status == "FAIL"

    def test_uniqueness_check(self, spark):
        """Uniqueness check should detect duplicates."""
        from spark.quality.data_quality_checks import DataQualityChecker
        data = [("TXN001",), ("TXN001",), ("TXN002",)]  # TXN001 is duplicate
        df = spark.createDataFrame(data, "transaction_id STRING")
        checker = DataQualityChecker(spark, df, "test_table")
        result = checker.check_uniqueness("transaction_id", threshold=1.0)
        assert result.status == "FAIL"
        assert result.failed_records == 1

    def test_range_check(self, spark):
        """Range check should catch out-of-range values."""
        from spark.quality.data_quality_checks import DataQualityChecker
        data = [(10,), (5,), (-3,), (0,)]           # -3 and 0 are invalid if min=1
        df = spark.createDataFrame(data, "quantity INT")
        checker = DataQualityChecker(spark, df, "test_table")
        result = checker.check_range("quantity", min_val=1, threshold=0.99)
        assert result.failed_records == 2
