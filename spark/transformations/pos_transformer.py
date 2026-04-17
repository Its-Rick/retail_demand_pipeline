"""
pos_transformer.py
------------------
PySpark ETL script for POS (Point-of-Sale) data.

Steps:
  1. READ   → Load raw CSV from Data Lake partition
  2. CLEAN  → Handle nulls, duplicates, bad data types
  3. ENRICH → Join with dim_time, holiday flags, weather
  4. LOAD   → Write to fact_sales in PostgreSQL (JDBC)

Usage:
    spark-submit pos_transformer.py --execution-date 2023-11-15 --data-lake ./data
"""

import argparse
import logging
import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DateType, IntegerType, DoubleType, TimestampType, BooleanType
)
from pyspark.sql.window import Window

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s"
)
log = logging.getLogger("pos_transformer")

# ─── CONFIG ───────────────────────────────────────────────────────────────────
PG_URL    = os.getenv("PG_JDBC_URL",  "jdbc:postgresql://localhost:5432/retail_dw")
PG_USER   = os.getenv("PG_USER",      "airflow")
PG_PASS   = os.getenv("PG_PASSWORD",  "airflow")
PG_DRIVER = "org.postgresql.Driver"

# POS raw schema — explicit schema for performance (no inferSchema scan)
POS_SCHEMA = StructType([
    StructField("transaction_id",   StringType(),  True),
    StructField("transaction_date", StringType(),  True),   # will cast to Date
    StructField("transaction_time", StringType(),  True),
    StructField("store_id",         StringType(),  True),
    StructField("store_name",       StringType(),  True),
    StructField("city",             StringType(),  True),
    StructField("state",            StringType(),  True),
    StructField("region",           StringType(),  True),
    StructField("product_id",       StringType(),  True),
    StructField("product_name",     StringType(),  True),
    StructField("category",         StringType(),  True),
    StructField("brand",            StringType(),  True),
    StructField("unit_price",       DoubleType(),  True),
    StructField("quantity",         IntegerType(), True),
    StructField("discount",         DoubleType(),  True),
    StructField("total_amount",     DoubleType(),  True),
    StructField("payment_method",   StringType(),  True),
    StructField("customer_id",      StringType(),  True),
])


# ─── SPARK SESSION ────────────────────────────────────────────────────────────

def create_spark_session(app_name: str = "RetailPOS_ETL") -> SparkSession:
    """Create and configure SparkSession."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "50")        # tune for cluster size
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        # PostgreSQL JDBC config
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1")
        .getOrCreate()
    )


# ─── STEP 1: READ ─────────────────────────────────────────────────────────────

def read_raw_pos(spark: SparkSession, data_lake: str, execution_date: str) -> DataFrame:
    """Load raw POS CSV from the Data Lake partition for the given date."""
    year, month, day = execution_date.split("-")
    raw_path = os.path.join(
        data_lake, "raw", "pos",
        f"year={year}", f"month={month}", f"day={day}", "*.csv"
    )
    log.info(f"Reading raw POS data from: {raw_path}")
    df = (
        spark.read
        .option("header", "true")
        .option("nullValue", "")
        .option("nanValue", "NaN")
        .schema(POS_SCHEMA)
        .csv(raw_path)
    )
    count = df.count()
    log.info(f"Raw rows loaded: {count:,}")
    return df


# ─── STEP 2: CLEAN ────────────────────────────────────────────────────────────

def clean_pos_data(df: DataFrame) -> DataFrame:
    """
    Apply data cleaning rules:
    - Drop rows with null critical fields
    - Remove exact duplicate transactions
    - Fix data type issues
    - Clip out-of-range values
    """
    log.info("Applying data cleaning...")
    raw_count = df.count()

    # 2a. Drop rows where primary keys are null
    df = df.filter(
        F.col("transaction_id").isNotNull() &
        F.col("product_id").isNotNull() &
        F.col("store_id").isNotNull()
    )

    # 2b. Cast transaction_date to DateType
    df = df.withColumn(
        "transaction_date",
        F.to_date(F.col("transaction_date"), "yyyy-MM-dd")
    )

    # 2c. Remove rows with invalid dates
    df = df.filter(F.col("transaction_date").isNotNull())

    # 2d. Clean numeric fields: negative quantity or price is invalid
    df = df.filter(
        (F.col("quantity") > 0) &
        (F.col("unit_price") > 0)
    )

    # 2e. Fill missing discount with 0
    df = df.fillna({"discount": 0.0})

    # 2f. Fill missing product_name with product_id (fallback)
    df = df.withColumn(
        "product_name",
        F.coalesce(F.col("product_name"), F.col("product_id"))
    )

    # 2g. Remove exact duplicate transaction_ids (keep first occurrence)
    # Use row_number() to deterministically pick one row per transaction
    window = Window.partitionBy("transaction_id").orderBy("transaction_date")
    df = (
        df.withColumn("_rn", F.row_number().over(window))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )

    # 2h. Recalculate net revenue to ensure consistency
    df = df.withColumn(
        "net_revenue",
        F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount")), 2)
    )

    clean_count = df.count()
    log.info(f"Cleaning complete: {raw_count:,} → {clean_count:,} rows "
             f"({raw_count - clean_count:,} removed)")
    return df


# ─── STEP 3: ENRICH ───────────────────────────────────────────────────────────

def enrich_with_time_features(df: DataFrame) -> DataFrame:
    """Add time dimension features directly (avoid extra JOIN for batch)."""
    log.info("Enriching with time features...")
    return (
        df
        .withColumn("day_of_week",  F.dayofweek("transaction_date"))
        .withColumn("week_of_year", F.weekofyear("transaction_date"))
        .withColumn("month_num",    F.month("transaction_date"))
        .withColumn("quarter",      F.quarter("transaction_date"))
        .withColumn("year",         F.year("transaction_date"))
        .withColumn("is_weekend",   F.dayofweek("transaction_date").isin([1, 7]))
    )


def enrich_with_weather(df: DataFrame, spark: SparkSession, data_lake: str) -> DataFrame:
    """Left-join weather data on (transaction_date, state)."""
    weather_path = os.path.join(data_lake, "sample", "weather.csv")
    if not os.path.exists(weather_path):
        log.warning("Weather data not found — skipping enrichment")
        return df

    log.info("Joining weather data...")
    weather_df = (
        spark.read.option("header", "true").csv(weather_path)
        .select(
            F.to_date("date", "yyyy-MM-dd").alias("w_date"),
            F.col("state").alias("w_state"),
            F.col("temp_high_f").cast("double"),
            F.col("precipitation_in").cast("double"),
            F.col("condition").alias("weather_condition")
        )
    )
    return df.join(
        weather_df,
        (df.transaction_date == weather_df.w_date) & (df.state == weather_df.w_state),
        how="left"
    ).drop("w_date", "w_state")


def enrich_with_holidays(df: DataFrame, spark: SparkSession, data_lake: str) -> DataFrame:
    """Flag holiday dates from external holidays CSV."""
    holiday_path = os.path.join(data_lake, "sample", "holidays.csv")
    if not os.path.exists(holiday_path):
        log.warning("Holidays data not found — skipping")
        return df.withColumn("is_holiday", F.lit(False)).withColumn("holiday_name", F.lit(None).cast("string"))

    log.info("Joining holiday flags...")
    holiday_df = (
        spark.read.option("header", "true").csv(holiday_path)
        .select(
            F.to_date("date", "yyyy-MM-dd").alias("h_date"),
            F.col("holiday_name"),
            F.lit(True).alias("is_holiday")
        )
    )
    return df.join(
        holiday_df,
        df.transaction_date == holiday_df.h_date,
        how="left"
    ).drop("h_date").fillna({"is_holiday": False})


# ─── STEP 4: LOAD ─────────────────────────────────────────────────────────────

def write_to_postgres(df: DataFrame, table: str, mode: str = "append"):
    """Write DataFrame to PostgreSQL via JDBC with connection pooling."""
    log.info(f"Writing {df.count():,} rows to {table}...")
    (
        df.write
        .format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", table)
        .option("user", PG_USER)
        .option("password", PG_PASS)
        .option("driver", PG_DRIVER)
        .option("batchsize", "5000")           # number of rows per INSERT batch
        .option("numPartitions", "4")          # parallel JDBC writes
        .option("isolationLevel", "READ_COMMITTED")
        .mode(mode)
        .save()
    )
    log.info(f"Write complete → {table}")


def write_to_data_lake_parquet(df: DataFrame, data_lake: str, execution_date: str):
    """Save processed data to Data Lake as Parquet (partitioned by date)."""
    year, month, day = execution_date.split("-")
    out_path = os.path.join(
        data_lake, "processed", "pos_fact",
        f"year={year}", f"month={month}", f"day={day}"
    )
    log.info(f"Writing Parquet to: {out_path}")
    (
        df.write
        .mode("overwrite")
        .parquet(out_path)
    )


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main(execution_date: str, data_lake: str):
    log.info(f"{'='*60}")
    log.info(f"  POS Transformer | execution_date={execution_date}")
    log.info(f"{'='*60}")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # 1. Read
        raw_df = read_raw_pos(spark, data_lake, execution_date)

        # 2. Clean
        clean_df = clean_pos_data(raw_df)

        # 3. Enrich
        enriched_df = enrich_with_time_features(clean_df)
        enriched_df = enrich_with_weather(enriched_df, spark, data_lake)
        enriched_df = enrich_with_holidays(enriched_df, spark, data_lake)

        # Select final columns for fact_sales
        fact_df = enriched_df.select(
            F.col("transaction_id"),
            F.col("transaction_date"),
            F.col("store_id"),
            F.col("product_id"),
            F.col("customer_id"),
            F.col("payment_method"),
            F.lit("store").alias("channel"),
            F.col("quantity").alias("quantity_sold"),
            F.col("unit_price"),
            F.round(F.col("unit_price") * F.col("quantity") * F.col("discount"), 2).alias("discount_amount"),
            F.round(F.col("unit_price") * F.col("quantity"), 2).alias("gross_revenue"),
            F.col("net_revenue"),
            F.lit("pos").alias("source_system"),
        )

        # 4. Write processed Parquet to Data Lake
        write_to_data_lake_parquet(fact_df, data_lake, execution_date)

        # 5. Load dimensions lookup and write to warehouse
        # (Airflow loads dims separately; here we log counts for monitoring)
        log.info(f"✅ ETL complete | {fact_df.count():,} fact rows ready for DW load")

    except Exception as e:
        log.error(f"ETL FAILED: {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="POS Data Transformer")
    parser.add_argument("--execution-date", required=True,
                        help="Date to process (YYYY-MM-DD)")
    parser.add_argument("--data-lake", default="./data",
                        help="Base path of Data Lake")
    args = parser.parse_args()
    main(args.execution_date, args.data_lake)
