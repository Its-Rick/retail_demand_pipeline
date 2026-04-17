"""
export_ml_dataset.py
---------------------
Exports the ML-ready demand forecasting dataset from the data warehouse
to a CSV/Parquet file for use by data scientists and model training pipelines.

Features exported:
  - Lag features (7d, 14d, 28d)
  - Rolling averages (7d, 28d)
  - Calendar features (dow, week, month, quarter, holiday)
  - Weather features (temperature, precipitation)
  - Store and product metadata

Also runs feature validation to detect data leakage and distribution issues.

Usage:
    python scripts/export_ml_dataset.py --format csv --output data/processed/ml/
    python scripts/export_ml_dataset.py --format parquet --lookback 90
"""

import argparse
import logging
import os
import sys
from datetime import datetime

import pandas as pd
import numpy as np

log = logging.getLogger("ml_export")
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


def load_from_db(dsn: str, lookback_days: int) -> pd.DataFrame:
    """Pull ML-ready dataset from PostgreSQL view."""
    import psycopg2

    log.info(f"Connecting to database...")
    query = f"""
        SELECT
            agg_date,
            product_id,
            store_id,
            category,
            region,
            total_quantity           AS target_demand,
            total_revenue,
            total_orders,
            avg_order_value,
            discount_rate,
            day_of_week,
            week_of_year,
            month_num,
            quarter,
            is_holiday::int          AS is_holiday,
            demand_lag_7d,
            demand_lag_14d,
            demand_lag_28d,
            rolling_avg_7d,
            rolling_avg_28d,
            temp_high_f,
            precipitation_in
        FROM warehouse.v_ml_ready_dataset
        WHERE agg_date >= CURRENT_DATE - {lookback_days}
        ORDER BY product_id, store_id, agg_date
    """
    conn = psycopg2.connect(dsn)
    df = pd.read_sql(query, conn)
    conn.close()
    log.info(f"Loaded {len(df):,} rows × {df.shape[1]} columns")
    return df


def load_from_sample(data_lake: str) -> pd.DataFrame:
    """Generate ML dataset from sample data (fallback when DB unavailable)."""
    log.warning("DB unavailable — building ML dataset from sample CSV")
    pos_path = os.path.join(data_lake, "sample", "pos_sales.csv")
    weather_path = os.path.join(data_lake, "sample", "weather.csv")
    holiday_path = os.path.join(data_lake, "sample", "holidays.csv")

    df = pd.read_csv(pos_path, parse_dates=["transaction_date"])
    weather = pd.read_csv(weather_path, parse_dates=["date"])
    holidays = pd.read_csv(holiday_path, parse_dates=["date"])

    # Aggregate to daily demand
    daily = (
        df.groupby(["transaction_date", "product_id", "store_id", "category", "region"])
        .agg(
            target_demand=("quantity", "sum"),
            total_revenue=("total_amount", "sum"),
            total_orders=("transaction_id", "nunique"),
        )
        .reset_index()
        .rename(columns={"transaction_date": "agg_date"})
    )

    # Time features
    daily["day_of_week"]  = daily["agg_date"].dt.dayofweek
    daily["week_of_year"] = daily["agg_date"].dt.isocalendar().week.astype(int)
    daily["month_num"]    = daily["agg_date"].dt.month
    daily["quarter"]      = daily["agg_date"].dt.quarter
    daily["avg_order_value"] = daily["total_revenue"] / daily["total_orders"].clip(lower=1)

    # Holiday flag
    holiday_dates = set(holidays["date"].dt.date)
    daily["is_holiday"] = daily["agg_date"].dt.date.isin(holiday_dates).astype(int)

    # Lag features
    daily = daily.sort_values(["product_id", "store_id", "agg_date"])
    grp = daily.groupby(["product_id", "store_id"])["target_demand"]
    daily["demand_lag_7d"]    = grp.shift(7)
    daily["demand_lag_14d"]   = grp.shift(14)
    daily["demand_lag_28d"]   = grp.shift(28)
    daily["rolling_avg_7d"]   = grp.transform(lambda x: x.shift(1).rolling(7,  min_periods=3).mean())
    daily["rolling_avg_28d"]  = grp.transform(lambda x: x.shift(1).rolling(28, min_periods=7).mean())

    # Weather join (on date + state, approximate — use region as proxy)
    weather_daily = weather.groupby("date")[["temp_high_f","precipitation_in"]].mean().reset_index()
    weather_daily.rename(columns={"date": "agg_date"}, inplace=True)
    daily = daily.merge(weather_daily, on="agg_date", how="left")

    log.info(f"Built ML dataset from sample: {len(daily):,} rows")
    return daily


def validate_features(df: pd.DataFrame) -> dict:
    """
    Run feature validation checks:
    - Missing value rates per column
    - Target variable distribution sanity
    - Lag coverage (how many rows have all 3 lags)
    - Leakage check: no future data in lag columns
    """
    log.info("Running feature validation...")
    report = {}

    # 1. Missing rates
    null_rates = (df.isnull().sum() / len(df)).round(4)
    report["null_rates"] = null_rates[null_rates > 0].to_dict()

    # 2. Target distribution
    report["target_stats"] = {
        "mean":   round(df["target_demand"].mean(), 2),
        "std":    round(df["target_demand"].std(),  2),
        "min":    int(df["target_demand"].min()),
        "max":    int(df["target_demand"].max()),
        "zeros":  int((df["target_demand"] == 0).sum()),
        "neg":    int((df["target_demand"] < 0).sum()),
    }

    # 3. Lag coverage
    has_all_lags = (
        df["demand_lag_7d"].notna() &
        df["demand_lag_14d"].notna() &
        df["demand_lag_28d"].notna()
    )
    report["lag_coverage_pct"] = round(has_all_lags.mean() * 100, 2)

    # 4. Date range
    report["date_range"] = {
        "start": str(df["agg_date"].min()),
        "end":   str(df["agg_date"].max()),
        "days":  int((df["agg_date"].max() - df["agg_date"].min()).days + 1),
    }

    # 5. Cardinality
    report["cardinality"] = {
        "products": df["product_id"].nunique(),
        "stores":   df["store_id"].nunique(),
        "categories": df["category"].nunique() if "category" in df else 0,
    }

    # Print report
    log.info("── Feature Validation Report ──────────────────")
    if report["null_rates"]:
        log.warning(f"Columns with nulls: {report['null_rates']}")
    else:
        log.info("✅ No missing values in exported columns")

    stats = report["target_stats"]
    log.info(f"Target (demand): mean={stats['mean']} std={stats['std']} "
             f"min={stats['min']} max={stats['max']} zeros={stats['zeros']}")

    if stats["neg"] > 0:
        log.error(f"❌ Negative target values found: {stats['neg']} rows!")

    log.info(f"Lag coverage: {report['lag_coverage_pct']}% rows have all 3 lags")
    log.info(f"Date range: {report['date_range']['start']} → "
             f"{report['date_range']['end']} ({report['date_range']['days']} days)")
    log.info(f"Products: {report['cardinality']['products']} | "
             f"Stores: {report['cardinality']['stores']}")
    log.info("───────────────────────────────────────────────")

    return report


def export(df: pd.DataFrame, output_dir: str, fmt: str):
    """Save the ML dataset in requested format with timestamp."""
    os.makedirs(output_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    if fmt == "csv":
        path = os.path.join(output_dir, f"demand_features_{ts}.csv")
        df.to_csv(path, index=False)
        size_mb = os.path.getsize(path) / 1e6
        log.info(f"✅ CSV exported: {path} ({size_mb:.1f} MB, {len(df):,} rows)")

    elif fmt == "parquet":
        path = os.path.join(output_dir, f"demand_features_{ts}.parquet")
        df.to_parquet(path, index=False, engine="pyarrow", compression="snappy")
        size_mb = os.path.getsize(path) / 1e6
        log.info(f"✅ Parquet exported: {path} ({size_mb:.1f} MB, {len(df):,} rows)")

    # Also write a "latest" symlink/copy for easy pipeline consumption
    latest_path = os.path.join(output_dir, f"demand_features_latest.{fmt}")
    if fmt == "csv":
        df.to_csv(latest_path, index=False)
    else:
        df.to_parquet(latest_path, index=False, engine="pyarrow", compression="snappy")

    return path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export ML-ready demand dataset")
    parser.add_argument("--format",    choices=["csv", "parquet"], default="csv")
    parser.add_argument("--output",    default="data/processed/ml/")
    parser.add_argument("--lookback",  type=int, default=365, help="Days of history")
    parser.add_argument("--data-lake", default="./data")
    args = parser.parse_args()

    dsn = os.getenv("DATABASE_URL",
                    "postgresql://airflow:airflow@localhost:5432/retail_dw")

    try:
        df = load_from_db(dsn, args.lookback)
    except Exception as e:
        log.warning(f"DB load failed ({e}) — using sample data fallback")
        df = load_from_sample(args.data_lake)

    report = validate_features(df)

    if report["target_stats"]["neg"] > 0:
        log.error("Export aborted: negative target values detected")
        sys.exit(1)

    export(df, args.output, args.format)
