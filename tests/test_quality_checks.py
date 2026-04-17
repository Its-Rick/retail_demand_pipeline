"""
test_quality_checks.py
-----------------------
Unit tests for data quality checks, monitoring utilities,
Kafka producer/consumer validation logic, and config loading.

Run: pytest tests/ -v --tb=short
"""

import os
import json
import time
import pytest
from unittest.mock import MagicMock, patch, call
from datetime import datetime


# ─── CONFIG TESTS ─────────────────────────────────────────────────────────────

class TestConfig:

    def test_database_config_defaults(self):
        from config.config import DatabaseConfig
        db = DatabaseConfig()
        assert db.host == os.getenv("PG_HOST", "localhost")
        assert db.port == 5432
        assert "postgresql://" in db.dsn
        assert "jdbc:postgresql://" in db.jdbc_url

    def test_kafka_config_defaults(self):
        from config.config import KafkaConfig
        k = KafkaConfig()
        assert k.batch_size == 500
        assert k.flush_interval == 30
        assert k.num_partitions == 6

    def test_data_lake_partition_path(self):
        from config.config import DataLakeConfig
        lake = DataLakeConfig(base_path="/data")
        path = lake.partition_path("raw", "pos", "2023-11-15")
        assert "year=2023" in path
        assert "month=11" in path
        assert "day=15" in path

    def test_pipeline_config_singleton(self):
        from config.config import config
        assert config.db is not None
        assert config.kafka is not None
        assert config.lake is not None


# ─── MONITORING TESTS ─────────────────────────────────────────────────────────

class TestPipelineMetrics:

    def test_record_and_finish(self):
        from monitoring.monitoring import PipelineMetrics
        m = PipelineMetrics("test_pipe")
        m.record("rows_read", 1000)
        m.record("rows_written", 980)
        m.increment("errors", 20)
        time.sleep(0.05)
        summary = m.finish("success")

        assert summary["status"] == "success"
        assert summary["metrics"]["rows_read"] == 1000
        assert summary["metrics"]["errors"] == 20
        assert summary["elapsed_sec"] >= 0.05

    def test_error_tracking(self):
        from monitoring.monitoring import PipelineMetrics
        m = PipelineMetrics("err_pipe")
        m.error("DB connection timeout")
        m.error("Retry 1 failed")
        summary = m.finish("failed")
        assert len(summary["errors"]) == 2
        assert summary["status"] == "failed"

    def test_increment_starts_at_zero(self):
        from monitoring.monitoring import PipelineMetrics
        m = PipelineMetrics("counter_test")
        m.increment("count")
        m.increment("count")
        m.increment("count", by=5)
        assert m.metrics["count"] == 7

    def test_prometheus_export(self, tmp_path):
        from monitoring.monitoring import PipelineMetrics, PrometheusExporter
        m = PipelineMetrics("prom_test")
        m.record("rows_read", 5000)
        m.finish("success")

        exporter = PrometheusExporter()
        exporter.METRICS_PATH = str(tmp_path / "metrics.prom")
        exporter.write(m)

        content = open(exporter.METRICS_PATH).read()
        assert "retail_pipeline_rows_read" in content
        assert "5000" in content


class TestTimedDecorator:

    def test_timed_returns_result(self):
        from monitoring.monitoring import timed

        @timed
        def add(a, b):
            return a + b

        assert add(2, 3) == 5

    def test_timed_reraises_exception(self):
        from monitoring.monitoring import timed

        @timed
        def boom():
            raise ValueError("test error")

        with pytest.raises(ValueError, match="test error"):
            boom()


# ─── KAFKA PRODUCER VALIDATION ────────────────────────────────────────────────

class TestKafkaProducerLogic:
    """Tests for event enrichment and serialization without a real Kafka broker."""

    def _make_producer_instance(self):
        """Instantiate EcommerceProducer with a mocked KafkaProducer."""
        with patch("kafka.KafkaProducer") as mock_kp:
            mock_kp.return_value = MagicMock()
            from kafka.producer.ecommerce_producer import EcommerceProducer
            p = EcommerceProducer.__new__(EcommerceProducer)
            p.producer = MagicMock()
            p.topic = "test_topic"
            return p

    def test_enrich_adds_produced_at(self):
        from kafka.producer.ecommerce_producer import EcommerceProducer
        p = EcommerceProducer.__new__(EcommerceProducer)
        p.producer = MagicMock()
        p.topic = "test_topic"

        event = {"event_id": "e1", "product_id": "P001"}
        enriched = p.enrich_event(event)
        assert "produced_at" in enriched
        assert "pipeline_version" in enriched

    def test_json_serializer(self):
        from kafka.producer.ecommerce_producer import json_serializer
        data = {"key": "value", "num": 42}
        result = json_serializer(data)
        assert isinstance(result, bytes)
        assert json.loads(result) == data

    def test_key_serializer(self):
        from kafka.producer.ecommerce_producer import key_serializer
        assert key_serializer("P001") == b"P001"
        assert key_serializer(None) == b""


# ─── KAFKA CONSUMER VALIDATION ────────────────────────────────────────────────

class TestKafkaConsumerValidation:

    def test_valid_event_passes(self):
        from kafka.consumer.ecommerce_consumer import validate_event
        event = {
            "event_id":        "e1",
            "event_type":      "product_view",
            "event_timestamp": "2023-11-15T10:00:00Z",
            "product_id":      "P001",
            "price":           29.99,
        }
        assert validate_event(event) is True

    def test_missing_event_id_fails(self):
        from kafka.consumer.ecommerce_consumer import validate_event
        assert validate_event({"event_type": "purchase", "product_id": "P001",
                               "event_timestamp": "2023-11-15T10:00:00Z"}) is False

    def test_invalid_event_type_fails(self):
        from kafka.consumer.ecommerce_consumer import validate_event
        assert validate_event({
            "event_id": "e1", "event_type": "INVALID_TYPE",
            "event_timestamp": "2023-11-15T10:00:00Z", "product_id": "P001"
        }) is False

    def test_negative_price_fails(self):
        from kafka.consumer.ecommerce_consumer import validate_event
        assert validate_event({
            "event_id": "e1", "event_type": "purchase",
            "event_timestamp": "2023-11-15T10:00:00Z",
            "product_id": "P001", "price": -10.0
        }) is False

    def test_transform_event_normalises(self):
        from kafka.consumer.ecommerce_consumer import transform_event
        raw = {
            "event_id":        "e1",
            "event_type":      "add_to_cart",
            "event_timestamp": "2023-11-15T10:00:00Z",
            "session_id":      "s1",
            "user_id":         "u1",
            "product_id":      "P001",
            "category":        "Electronics",
            "price":           199.99,
            "quantity":        None,
            "device":          "mobile",
            "platform":        "ios",
            "referrer":        "google",
            "geo":             {"country": "US", "state": "CA"},
        }
        result = transform_event(raw)
        assert result["state"] == "CA"
        assert result["event_timestamp"] == "2023-11-15T10:00:00"   # Z stripped

    def test_transform_missing_geo(self):
        from kafka.consumer.ecommerce_consumer import transform_event
        raw = {"event_id": "e1", "event_type": "product_view",
               "event_timestamp": "2023-11-15T10:00:00Z", "product_id": "P001"}
        result = transform_event(raw)
        assert result["state"] is None


# ─── SAMPLE DATA INTEGRITY ────────────────────────────────────────────────────

class TestSampleData:

    SAMPLE_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "sample")

    def test_pos_csv_exists_and_has_rows(self):
        import pandas as pd
        path = os.path.join(self.SAMPLE_DIR, "pos_sales.csv")
        if not os.path.exists(path):
            pytest.skip("Sample data not generated — run scripts/generate_sample_data.py")
        df = pd.read_csv(path)
        assert len(df) > 10_000
        assert "transaction_id" in df.columns
        assert "product_id" in df.columns

    def test_ecommerce_json_structure(self):
        path = os.path.join(self.SAMPLE_DIR, "ecommerce_events.json")
        if not os.path.exists(path):
            pytest.skip("Sample data not generated")
        with open(path) as f:
            events = json.load(f)
        assert len(events) > 1000
        first = events[0]
        assert "event_id" in first
        assert "event_type" in first
        assert "product_id" in first

    def test_holidays_csv_has_required_columns(self):
        import pandas as pd
        path = os.path.join(self.SAMPLE_DIR, "holidays.csv")
        if not os.path.exists(path):
            pytest.skip("Sample data not generated")
        df = pd.read_csv(path)
        assert "date" in df.columns
        assert "holiday_name" in df.columns
        assert len(df) > 5

    def test_weather_csv_no_nulls_in_key_cols(self):
        import pandas as pd
        path = os.path.join(self.SAMPLE_DIR, "weather.csv")
        if not os.path.exists(path):
            pytest.skip("Sample data not generated")
        df = pd.read_csv(path)
        assert df["date"].isnull().sum() == 0
        assert df["state"].isnull().sum() == 0
