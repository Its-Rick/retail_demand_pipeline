# =============================================================================
# config.py
# Centralised configuration for the Retail Demand Pipeline.
# All settings are read from environment variables with sensible defaults.
# Never hardcode credentials — use .env or Docker secrets in production.
# =============================================================================

import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class DatabaseConfig:
    """PostgreSQL Data Warehouse connection settings."""
    host:     str = os.getenv("PG_HOST",     "localhost")
    port:     int = int(os.getenv("PG_PORT", "5432"))
    dbname:   str = os.getenv("PG_DBNAME",   "retail_dw")
    user:     str = os.getenv("PG_USER",     "airflow")
    password: str = os.getenv("PG_PASSWORD", "airflow")
    schema:   str = "warehouse"

    @property
    def dsn(self) -> str:
        return (f"postgresql://{self.user}:{self.password}"
                f"@{self.host}:{self.port}/{self.dbname}")

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.dbname}"


@dataclass
class KafkaConfig:
    """Kafka broker and topic settings."""
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic:             str = os.getenv("KAFKA_TOPIC",              "ecommerce_events")
    dlq_topic:         str = os.getenv("KAFKA_DLQ_TOPIC",         "ecommerce_events_dlq")
    group_id:          str = os.getenv("KAFKA_GROUP_ID",          "retail-demand-pipeline")
    num_partitions:    int = int(os.getenv("KAFKA_PARTITIONS",    "6"))
    batch_size:        int = int(os.getenv("CONSUMER_BATCH_SIZE", "500"))
    flush_interval:    int = int(os.getenv("CONSUMER_FLUSH_SEC",  "30"))


@dataclass
class DataLakeConfig:
    """Local or S3 Data Lake path settings."""
    base_path:      str = os.getenv("DATA_LAKE_PATH", "./data")
    raw_pos:        str = field(init=False)
    raw_ecommerce:  str = field(init=False)
    raw_external:   str = field(init=False)
    processed:      str = field(init=False)
    sample:         str = field(init=False)

    def __post_init__(self):
        self.raw_pos       = os.path.join(self.base_path, "raw", "pos")
        self.raw_ecommerce = os.path.join(self.base_path, "raw", "ecommerce")
        self.raw_external  = os.path.join(self.base_path, "raw", "external")
        self.processed     = os.path.join(self.base_path, "processed")
        self.sample        = os.path.join(self.base_path, "sample")

    def partition_path(self, layer: str, source: str, date_str: str) -> str:
        """Build Hive-style partition path: base/layer/source/year=Y/month=M/day=D"""
        year, month, day = date_str.split("-")
        return os.path.join(
            self.base_path, layer, source,
            f"year={year}", f"month={month}", f"day={day}"
        )


@dataclass
class SparkConfig:
    """PySpark session settings."""
    master:              str = os.getenv("SPARK_MASTER",       "local[*]")
    app_name:            str = "RetailDemandPipeline"
    shuffle_partitions:  int = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "50"))
    jdbc_packages:       str = "org.postgresql:postgresql:42.7.1"
    driver_memory:       str = os.getenv("SPARK_DRIVER_MEMORY",  "2g")
    executor_memory:     str = os.getenv("SPARK_EXECUTOR_MEMORY", "4g")
    executor_cores:      int = int(os.getenv("SPARK_EXECUTOR_CORES", "2"))


@dataclass
class PipelineConfig:
    """Top-level pipeline orchestration settings."""
    db:         DatabaseConfig = field(default_factory=DatabaseConfig)
    kafka:      KafkaConfig    = field(default_factory=KafkaConfig)
    lake:       DataLakeConfig = field(default_factory=DataLakeConfig)
    spark:      SparkConfig    = field(default_factory=SparkConfig)

    # Airflow DAG settings
    dag_schedule:    str = "0 2 * * *"   # 02:00 UTC daily
    dq_schedule:     str = "0 4 * * *"   # 04:00 UTC daily
    dag_retries:     int = 2
    dag_retry_delay: int = 5             # minutes

    # Data quality thresholds
    dq_null_threshold:      float = 0.99
    dq_duplicate_threshold: float = 0.99
    dq_range_threshold:     float = 0.99

    # Monitoring
    slack_webhook: Optional[str] = os.getenv("SLACK_WEBHOOK_URL")
    alert_email:   str           = os.getenv("ALERT_EMAIL", "alerts@retailpipeline.com")


# Singleton config instance — import this in all modules
config = PipelineConfig()


# ── USAGE EXAMPLE ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("Pipeline Configuration:")
    print(f"  DB DSN:             {config.db.dsn}")
    print(f"  Kafka brokers:      {config.kafka.bootstrap_servers}")
    print(f"  Data lake:          {config.lake.base_path}")
    print(f"  Spark master:       {config.spark.master}")
    print(f"  DAG schedule:       {config.dag_schedule}")
    print(f"  DQ null threshold:  {config.dq_null_threshold:.0%}")

    # Example: build a partition path
    path = config.lake.partition_path("raw", "pos", "2023-11-15")
    print(f"  Sample partition:   {path}")
