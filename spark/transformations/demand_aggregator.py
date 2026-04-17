"""
demand_aggregator.py
--------------------
PySpark script that reads from the processed Parquet layer and
computes demand aggregations directly in Spark (without hitting
PostgreSQL for the heavy computation).

Outputs:
  - data/processed/agg_daily/   → Daily demand Parquet (for ML)
  - data/processed/agg_weekly/  → Weekly demand Parquet (for ML)
  - Writes agg tables to PostgreSQL via JDBC

Why Spark instead of SQL?
  For large datasets (millions of rows), Spark distributed aggregation
  is faster than doing GROUP BY inside PostgreSQL. Final upsert to PG
  is done on the aggregated (smaller) result set.

Usage:
    spark-submit demand_aggregator.py --lookback-days 7
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

log = logging.getLogger("demand_aggregator")
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

PG_URL    = os.getenv("PG_JDBC_URL",  "jdbc:postgresql://localhost:5432/retail_dw")
PG_USER   = os.getenv("PG_USER",      "airflow")
PG_PASS   = os.getenv("PG_PASSWORD",  "airflow")
PG_DRIVER = "org.postgresql.Driver"
DATA_LAKE = os.getenv("DATA_LAKE_PATH", "./data")


def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("DemandAggregator")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "20")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1")
        .getOrCreate()
    )


def read_processed_pos(spark, lookback_days: int):
    """
    Read processed POS Parquet from Data Lake for the last N days.
    Falls back to sample CSV if processed layer doesn't exist yet.
    """
    paths = []
    for i in range(1, lookback_days + 1):
        dt = (datetime.today() - timedelta(days=i)).strftime("%Y-%m-%d")
        y, m, d = dt.split("-")
        p = os.path.join(DATA_LAKE, "processed", "pos_fact",
                         f"year={y}", f"month={m}", f"day={d}")
        if os.path.isdir(p):
            paths.append(p)

    if not paths:
        log.warning("No processed Parquet found — reading sample CSV")
        return spark.read.option("header", "true").option("inferSchema", "true")\
            .csv(os.path.join(DATA_LAKE, "sample", "pos_sales.csv"))

    log.info(f"Reading {len(paths)} Parquet partitions")
    return spark.read.parquet(*paths)


def compute_daily_demand(df):
    """
    Aggregate to: date × product_id × store_id grain.
    Computes key demand metrics used as ML features and in the dashboard.
    """
    log.info("Computing daily demand aggregations...")
    return (
        df
        .groupBy(
            F.col("transaction_date").alias("agg_date"),
            "product_id", "store_id", "category", "region"
        )
        .agg(
            F.sum("quantity_sold").alias("total_quantity"),
            F.sum("net_revenue").alias("total_revenue"),
            F.countDistinct("transaction_id").alias("total_orders"),
            F.round(
                F.sum("net_revenue") / F.countDistinct("transaction_id"), 2
            ).alias("avg_order_value"),
            F.round(
                F.sum("discount_amount") / F.sum("gross_revenue"), 4
            ).alias("discount_rate"),
        )
        # Add time features for ML
        .withColumn("day_of_week",  F.dayofweek("agg_date"))
        .withColumn("week_of_year", F.weekofyear("agg_date"))
        .withColumn("month_num",    F.month("agg_date"))
        .withColumn("quarter",      F.quarter("agg_date"))
        .withColumn("year",         F.year("agg_date"))
    )


def compute_weekly_demand(daily_df):
    """
    Aggregate daily → weekly and compute week-over-week % change.
    Uses Spark window functions for the WoW lag.
    """
    log.info("Computing weekly demand aggregations...")

    weekly = (
        daily_df
        .groupBy("year", "week_of_year", "product_id", "store_id", "category", "region")
        .agg(
            F.sum("total_quantity").alias("total_quantity"),
            F.sum("total_revenue").alias("total_revenue"),
            F.sum("total_orders").alias("total_orders"),
            F.round(F.sum("total_quantity") / 7, 2).alias("avg_daily_qty"),
        )
    )

    # Week-over-week lag using window
    w = Window.partitionBy("product_id", "store_id")\
              .orderBy("year", "week_of_year")
    weekly = (
        weekly
        .withColumn("prev_week_qty", F.lag("total_quantity", 1).over(w))
        .withColumn(
            "wow_change_pct",
            F.round(
                (F.col("total_quantity") - F.col("prev_week_qty"))
                / F.col("prev_week_qty") * 100, 4
            )
        )
    )
    return weekly


def add_lag_features(daily_df):
    """
    Add rolling and lag features for ML model consumption.
    These are computed in Spark to avoid expensive SQL window functions
    over millions of rows in PostgreSQL.
    """
    log.info("Adding lag and rolling features...")
    w = Window.partitionBy("product_id", "store_id").orderBy("agg_date")

    return (
        daily_df
        .withColumn("demand_lag_7d",   F.lag("total_quantity", 7).over(w))
        .withColumn("demand_lag_14d",  F.lag("total_quantity", 14).over(w))
        .withColumn("demand_lag_28d",  F.lag("total_quantity", 28).over(w))
        .withColumn(
            "rolling_avg_7d",
            F.avg("total_quantity").over(
                w.rowsBetween(-7, -1)
            )
        )
        .withColumn(
            "rolling_avg_28d",
            F.avg("total_quantity").over(
                w.rowsBetween(-28, -1)
            )
        )
        .withColumn(
            "rolling_std_7d",
            F.stddev("total_quantity").over(
                w.rowsBetween(-7, -1)
            )
        )
    )


def write_to_postgres(df, table: str, mode: str = "append"):
    """Write aggregated DataFrame to PostgreSQL via JDBC."""
    count = df.count()
    log.info(f"Writing {count:,} rows to {table}...")
    (
        df.write
        .format("jdbc")
        .option("url", PG_URL).option("dbtable", table)
        .option("user", PG_USER).option("password", PG_PASS)
        .option("driver", PG_DRIVER)
        .option("batchsize", "2000").option("numPartitions", "2")
        .mode(mode).save()
    )
    log.info(f"✓ Written {count:,} rows → {table}")


def write_parquet_agg(df, name: str):
    """Persist aggregated data as Parquet in the processed layer."""
    out = os.path.join(DATA_LAKE, "processed", name)
    os.makedirs(out, exist_ok=True)
    log.info(f"Writing Parquet → {out}")
    df.write.mode("overwrite").parquet(out)


def main(lookback_days: int):
    log.info(f"{'='*60}")
    log.info(f"  Demand Aggregator | lookback={lookback_days}d")
    log.info(f"{'='*60}")

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        raw_df    = read_processed_pos(spark, lookback_days)
        daily_df  = compute_daily_demand(raw_df)
        weekly_df = compute_weekly_demand(daily_df)
        ml_df     = add_lag_features(daily_df)

        # Write aggregations to Parquet (Data Lake processed layer)
        write_parquet_agg(daily_df, "agg_daily_demand")
        write_parquet_agg(weekly_df, "agg_weekly_demand")
        write_parquet_agg(ml_df, "ml_ready_demand")

        log.info("✅ Demand aggregation complete")

    except Exception as e:
        log.error(f"Aggregation FAILED: {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--lookback-days", type=int, default=7,
                        help="Days of processed data to aggregate")
    args = parser.parse_args()
    main(args.lookback_days)
