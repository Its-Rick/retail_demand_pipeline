"""
ecommerce_transformer.py
------------------------
PySpark ETL for e-commerce JSON events consumed from Kafka and
stored in the Data Lake as hourly JSONL partitions.

Steps:
  1. READ    → Load all hourly JSONL files for execution_date
  2. FLATTEN → Parse nested geo JSON, normalize types
  3. CLEAN   → Validate event_type, drop bad records
  4. ENRICH  → Join product dimension for category/price
  5. LOAD    → Write to stream_ecommerce_events table (PostgreSQL)
               Write Parquet to processed/ layer (Data Lake)

Usage:
    spark-submit ecommerce_transformer.py --execution-date 2023-11-15
"""

import argparse
import logging
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType, BooleanType
)

log = logging.getLogger("ecommerce_transformer")
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

PG_URL    = os.getenv("PG_JDBC_URL",  "jdbc:postgresql://localhost:5432/retail_dw")
PG_USER   = os.getenv("PG_USER",      "airflow")
PG_PASS   = os.getenv("PG_PASSWORD",  "airflow")
PG_DRIVER = "org.postgresql.Driver"

VALID_EVENT_TYPES = ["product_view", "add_to_cart", "remove_from_cart",
                     "purchase", "wishlist_add"]

# Explicit schema for the JSONL files written by the Kafka consumer
ECOM_SCHEMA = StructType([
    StructField("event_id",        StringType(),  True),
    StructField("event_type",      StringType(),  True),
    StructField("event_timestamp", StringType(),  True),
    StructField("session_id",      StringType(),  True),
    StructField("user_id",         StringType(),  True),
    StructField("product_id",      StringType(),  True),
    StructField("category",        StringType(),  True),
    StructField("price",           DoubleType(),  True),
    StructField("quantity",        IntegerType(), True),
    StructField("device",          StringType(),  True),
    StructField("platform",        StringType(),  True),
    StructField("referrer",        StringType(),  True),
    StructField("state",           StringType(),  True),
])


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("RetailEcom_ETL")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "20")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1")
        .getOrCreate()
    )


def read_ecom_events(spark, data_lake: str, execution_date: str):
    """Read all hourly JSONL partitions for the given date from Data Lake."""
    year, month, day = execution_date.split("-")
    path = os.path.join(
        data_lake, "raw", "ecommerce",
        f"year={year}", f"month={month}", f"day={day}",
        "*", "*.jsonl"   # hour=HH/*.jsonl
    )
    log.info(f"Reading e-commerce events from: {path}")

    # Fallback to sample file if no streaming data exists yet
    if not _path_has_files(path):
        log.warning("No streaming JSONL found — falling back to sample JSON")
        sample = os.path.join(data_lake, "sample", "ecommerce_events.json")
        df = spark.read.option("multiline", "true").schema(ECOM_SCHEMA).json(sample)
    else:
        df = spark.read.schema(ECOM_SCHEMA).json(path)

    count = df.count()
    log.info(f"Read {count:,} e-commerce events")
    return df


def _path_has_files(path: str) -> bool:
    """Check if a glob path resolves to any files (local FS only)."""
    import glob
    return bool(glob.glob(path))


def clean_events(df):
    """
    Clean e-commerce events:
    - Drop missing event_id / event_type / product_id
    - Filter to valid event_types only
    - Remove negative prices
    - Deduplicate by event_id
    - Parse timestamp string to TimestampType
    """
    log.info("Cleaning e-commerce events...")
    raw_count = df.count()

    df = df.filter(
        F.col("event_id").isNotNull() &
        F.col("event_type").isNotNull() &
        F.col("product_id").isNotNull()
    )
    df = df.filter(F.col("event_type").isin(VALID_EVENT_TYPES))
    df = df.filter(F.col("price").isNull() | (F.col("price") > 0))

    # Deduplicate on event_id
    from pyspark.sql.window import Window
    w = Window.partitionBy("event_id").orderBy("event_timestamp")
    df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

    # Parse timestamp
    df = df.withColumn(
        "event_timestamp",
        F.to_timestamp(F.col("event_timestamp"), "yyyy-MM-dd'T'HH:mm:ss")
    )

    clean_count = df.count()
    log.info(f"Cleaned: {raw_count:,} → {clean_count:,} ({raw_count - clean_count:,} dropped)")
    return df


def enrich_events(df, spark, data_lake: str):
    """Join product dimension data for category enrichment."""
    # Try reading from DW first, fallback to sample CSV
    try:
        product_df = (
            spark.read.format("jdbc")
            .option("url", PG_URL)
            .option("dbtable", "warehouse.dim_product")
            .option("user", PG_USER).option("password", PG_PASS)
            .option("driver", PG_DRIVER)
            .load()
            .select("product_id", F.col("category").alias("dim_category"),
                    F.col("unit_price").alias("list_price"))
        )
        log.info("Loaded product dimension from PostgreSQL")
    except Exception as e:
        log.warning(f"Could not load dim_product from DB: {e} — skipping enrichment")
        return df

    return df.join(product_df, on="product_id", how="left")


def compute_session_metrics(df):
    """
    Compute per-session aggregates as a separate output:
    total events, purchase flag, session duration estimate.
    """
    from pyspark.sql.window import Window
    w_sess = Window.partitionBy("session_id")

    return (
        df
        .withColumn("is_purchase", (F.col("event_type") == "purchase").cast("int"))
        .withColumn("session_event_count", F.count("event_id").over(w_sess))
        .withColumn("session_has_purchase", F.max("is_purchase").over(w_sess))
    )


def write_to_postgres(df, table: str, mode: str = "append"):
    """Write cleaned events to PostgreSQL via JDBC."""
    log.info(f"Writing to {table}...")
    (
        df.write
        .format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", table)
        .option("user", PG_USER)
        .option("password", PG_PASS)
        .option("driver", PG_DRIVER)
        .option("batchsize", "5000")
        .option("numPartitions", "4")
        .mode(mode)
        .save()
    )
    log.info(f"Write complete → {table}")


def write_parquet(df, data_lake: str, execution_date: str):
    """Write processed events as Parquet, partitioned by event_type."""
    year, month, day = execution_date.split("-")
    out = os.path.join(
        data_lake, "processed", "ecommerce_events",
        f"year={year}", f"month={month}", f"day={day}"
    )
    log.info(f"Writing Parquet → {out}")
    df.write.mode("overwrite").partitionBy("event_type").parquet(out)


def main(execution_date: str, data_lake: str):
    log.info(f"{'='*60}")
    log.info(f"  E-commerce Transformer | date={execution_date}")
    log.info(f"{'='*60}")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        raw_df     = read_ecom_events(spark, data_lake, execution_date)
        clean_df   = clean_events(raw_df)
        enriched   = enrich_events(clean_df, spark, data_lake)
        final_df   = compute_session_metrics(enriched)

        # Select only columns that match stream_ecommerce_events schema
        pg_df = final_df.select(
            "event_id", "event_type", "event_timestamp",
            "session_id", "user_id", "product_id", "category",
            "price", "quantity", "device", "platform", "referrer", "state"
        )

        write_to_postgres(pg_df, "warehouse.stream_ecommerce_events")
        write_parquet(final_df, data_lake, execution_date)

        log.info(f"✅ E-commerce ETL complete | {pg_df.count():,} events loaded")

    except Exception as e:
        log.error(f"ETL FAILED: {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--execution-date", required=True)
    parser.add_argument("--data-lake", default="./data")
    args = parser.parse_args()
    main(args.execution_date, args.data_lake)
