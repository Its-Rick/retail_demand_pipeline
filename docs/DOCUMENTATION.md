# Retail Demand Forecasting Pipeline — Technical Documentation

> **Version:** 1.0.0 | **Last Updated:** April 2026 | **Author:** Data Engineering Team

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [System Architecture](#2-system-architecture)
3. [Infrastructure & Services](#3-infrastructure--services)
4. [Data Sources](#4-data-sources)
5. [Data Lake Design](#5-data-lake-design)
6. [ETL Pipeline — Step by Step](#6-etl-pipeline--step-by-step)
7. [Streaming Pipeline](#7-streaming-pipeline)
8. [Data Warehouse Schema](#8-data-warehouse-schema)
9. [Data Quality Framework](#9-data-quality-framework)
10. [Airflow DAG Reference](#10-airflow-dag-reference)
11. [PySpark Transformations](#11-pyspark-transformations)
12. [Aggregation Layer](#12-aggregation-layer)
13. [ML Feature Engineering](#13-ml-feature-engineering)
14. [Dashboard & Forecasting](#14-dashboard--forecasting)
15. [Monitoring & Alerting](#15-monitoring--alerting)
16. [Configuration Reference](#16-configuration-reference)
17. [Deployment Guide](#17-deployment-guide)
18. [Testing Guide](#18-testing-guide)
19. [Troubleshooting Reference](#19-troubleshooting-reference)
20. [Architecture Decision Records](#20-architecture-decision-records)

---

## 1. Project Overview

### Purpose

The Retail Demand Forecasting Pipeline is a production-grade data engineering system that:

- **Ingests** retail data from POS systems, e-commerce platforms, and external sources (weather, holidays)
- **Processes** data through both batch (daily) and real-time (streaming) pathways
- **Stores** clean, structured data in a PostgreSQL Data Warehouse using a Star Schema
- **Serves** analytics dashboards and ML-ready datasets for demand forecasting models

### Business Value

| Stakeholder | Value Delivered |
|------------|-----------------|
| Data Scientists | ML-ready demand dataset with engineered lag features |
| Business Analysts | Aggregated demand tables, 10 analytical SQL views |
| Operations | Real-time inventory signals from streaming events |
| Engineering | Modular, testable, containerised pipeline |

### Key Metrics

| Metric | Value |
|--------|-------|
| Daily POS transactions processed | ~50,000 |
| Streaming events/second | 10 |
| Batch pipeline schedule | Daily 02:00 UTC |
| Data freshness (batch) | T+1 day |
| Data freshness (stream) | < 30 seconds |
| Test coverage | 32+ test cases |
| Docker services | 12 containers |

---

## 2. System Architecture

### Lambda Architecture

The system implements the **Lambda Architecture** pattern with three layers:

```
┌──────────────────────────────────────────────────────────────────┐
│                         BATCH LAYER                               │
│  Airflow → PySpark → PostgreSQL                                  │
│  Schedule: Daily 02:00 UTC | Latency: T+1 | Accuracy: High      │
├──────────────────────────────────────────────────────────────────┤
│                         SPEED LAYER                               │
│  Kafka Producer → Kafka Broker → Kafka Consumer → PostgreSQL     │
│  Latency: < 30s | Throughput: 10 events/sec | Accuracy: Good    │
├──────────────────────────────────────────────────────────────────┤
│                        SERVING LAYER                              │
│  PostgreSQL Views → Streamlit Dashboard → ML CSV Export          │
│  Query latency: < 2s | Freshness: Real-time for stream           │
└──────────────────────────────────────────────────────────────────┘
```

### Why Lambda Architecture?

**Batch layer** provides correctness — full reprocessing is possible if bugs are found. It handles the bulk of business data (POS transactions).

**Speed layer** provides timeliness — e-commerce clickstream events are available within 30 seconds, enabling same-day demand signals before the nightly batch runs.

**Serving layer** merges both layers into a unified view for analytics.

### Data Flow Diagram

```
Day D-1 22:00  POS system exports CSV
Day D   02:00  Airflow triggers:
               ├── extract_pos        → data/raw/pos/year=Y/month=M/day=D/
               ├── extract_ecommerce  → data/raw/ecommerce/year=Y/.../
               ├── data_quality_check → warehouse.dq_check_log
               ├── load_dim_product   → warehouse.dim_product
               ├── load_dim_store     → warehouse.dim_store
               ├── load_fact_sales    → warehouse.fact_sales
               ├── aggregate_demand   → warehouse.agg_daily_demand
               │                       warehouse.agg_weekly_demand
               ├── export_ml_dataset  → data/processed/ml/demand_features.csv
               └── pipeline_summary   → logs

Continuous     Kafka producer → ecommerce_events topic (10 evt/sec)
Continuous     Kafka consumer → warehouse.stream_ecommerce_events
```

---

## 3. Infrastructure & Services

### Docker Services

| Service | Image | Port | Purpose |
|---------|-------|------|---------|
| `retail_postgres` | postgres:15-alpine | 5432 | Airflow metadata + Data Warehouse |
| `retail_zookeeper` | cp-zookeeper:7.5.3 | 2181 | Kafka coordination |
| `retail_kafka` | cp-kafka:7.5.3 | 29092 | Event broker |
| `retail_kafka_setup` | cp-kafka:7.5.3 | — | One-time topic creation |
| `retail_airflow_init` | retail_airflow:2.8.1 | — | DB migrate + user create |
| `retail_airflow_web` | retail_airflow:2.8.1 | 8080 | Airflow Web UI |
| `retail_airflow_scheduler` | retail_airflow:2.8.1 | — | DAG scheduling |
| `retail_spark_master` | apache/spark:3.5.1 | 8090, 7077 | Spark coordinator |
| `retail_spark_worker` | apache/spark:3.5.1 | — | Spark executor |
| `retail_kafka_producer` | python:3.10-slim | — | E-commerce event simulation |
| `retail_kafka_consumer` | python:3.10-slim | — | Kafka → DB + Data Lake sink |
| `retail_dashboard` | python:3.10-slim | 8501 | Streamlit analytics UI |

### Network

All services run on the `retail_pipeline_net` bridge network. Inter-container communication uses service names as hostnames (e.g., `postgres`, `kafka`, `zookeeper`).

### Volumes

| Volume | Purpose |
|--------|---------|
| `postgres_data` | PostgreSQL data persistence |
| `zookeeper_data` | Zookeeper state |
| `kafka_data` | Kafka log segments |
| `airflow_logs` | Airflow task execution logs |

### Database Layout

```
PostgreSQL Server (retail_postgres)
├── airflow          ← Airflow metadata (DAG runs, task states, connections)
└── retail_dw        ← Data Warehouse
    └── warehouse    ← Schema containing all analytical tables
        ├── dim_time
        ├── dim_product
        ├── dim_store
        ├── fact_sales
        ├── agg_daily_demand
        ├── agg_weekly_demand
        ├── stream_ecommerce_events
        └── dq_check_log
```

---

## 4. Data Sources

### 4.1 POS Sales Data (Batch · CSV)

**Source:** Point-of-sale system export, delivered daily as CSV  
**File:** `data/sample/pos_sales.csv`  
**Volume:** ~50,000 rows/day  
**Schedule:** Available by 22:00 previous day

| Column | Type | Description |
|--------|------|-------------|
| `transaction_id` | VARCHAR | Unique transaction identifier (UUID) |
| `transaction_date` | DATE | Sale date (YYYY-MM-DD) |
| `transaction_time` | TIME | Sale time |
| `store_id` | VARCHAR | Store natural key (e.g., S001) |
| `store_name` | VARCHAR | Store display name |
| `city`, `state`, `region` | VARCHAR | Store location |
| `product_id` | VARCHAR | Product natural key (e.g., P0001) |
| `product_name` | VARCHAR | Product display name (3% null rate) |
| `category` | VARCHAR | Product category |
| `brand` | VARCHAR | Brand name |
| `unit_price` | DECIMAL | Price per unit |
| `quantity` | INT | Units sold |
| `discount` | DECIMAL | Discount rate (0.0–1.0) |
| `total_amount` | DECIMAL | Gross sale amount |
| `payment_method` | VARCHAR | Cash / Credit / Debit / UPI |
| `customer_id` | VARCHAR | Customer ID (20% anonymous) |

**Known data quality issues:**
- ~3% null `product_name` (tolerated — fallback to `product_id`)
- ~1% duplicate `transaction_id` (deduplication applied)
- ~0.5% negative or zero `quantity` (filtered out)

### 4.2 E-commerce Events (Streaming · JSON)

**Source:** Web/mobile application event stream via Kafka  
**File:** `data/sample/ecommerce_events.json`  
**Volume:** ~10 events/second  
**Format:** JSON objects published per event

| Field | Type | Description |
|-------|------|-------------|
| `event_id` | STRING | UUID — unique event identifier |
| `event_type` | STRING | product_view / add_to_cart / purchase / wishlist_add |
| `event_timestamp` | STRING | ISO 8601 UTC timestamp |
| `session_id` | STRING | Browser/app session |
| `user_id` | STRING | Authenticated user (may be null) |
| `product_id` | STRING | Product being interacted with |
| `category` | STRING | Product category |
| `price` | FLOAT | Product price at event time |
| `quantity` | INT | Quantity (purchase events only) |
| `device` | STRING | mobile / desktop / tablet |
| `platform` | STRING | web / ios / android |
| `referrer` | STRING | google / direct / email / social |
| `geo.country` | STRING | Country code |
| `geo.state` | STRING | State code |

### 4.3 Weather Data (Batch · CSV)

**Source:** External weather API (simulated)  
**File:** `data/sample/weather.csv`  
**Grain:** Daily per state  
**Used for:** Feature enrichment in fact_sales and ML dataset

### 4.4 Holidays Data (Static · CSV)

**Source:** US federal and retail holiday calendar  
**File:** `data/sample/holidays.csv`  
**Used for:** `is_holiday` flag in dim_time and ML features

---

## 5. Data Lake Design

### Partitioning Strategy

All raw data is stored using **Hive-style partitioning** for efficient scan pruning:

```
data/
├── raw/
│   ├── pos/
│   │   └── year=2023/month=11/day=15/pos_sales.csv
│   └── ecommerce/
│       └── year=2023/month=11/day=15/hour=14/batch_1700000000.jsonl
└── processed/
    ├── pos_fact/
    │   └── year=2023/month=11/day=15/*.parquet
    ├── agg_daily_demand/*.parquet
    ├── agg_weekly_demand/*.parquet
    └── ml_ready/
        └── dt=2023-11-15/demand_features.csv
```

### Benefits

- Airflow and PySpark read **only the relevant partition** — no full scans
- Easy to expire old partitions (detach without downtime)
- Compatible with AWS Athena / Glue for cloud migration
- Hourly partitioning on streaming data limits file accumulation

### File Formats

| Layer | Format | Reason |
|-------|--------|--------|
| Raw ingestion | CSV / JSON | Source system format, preserved as-is |
| Processed | Parquet (Snappy) | Columnar, compressed, fast scan |
| ML export | CSV | Universal compatibility for ML tools |

---

## 6. ETL Pipeline — Step by Step

### Batch Pipeline (Airflow DAG: `retail_demand_batch_pipeline`)

The DAG runs **daily at 02:00 UTC** and processes the previous day's data through 10 tasks:

#### Task 1 & 2: Extract (Parallel)

**`extract_pos`** and **`extract_ecommerce`** run in parallel. Each copies the source file to the appropriate Data Lake partition:

```python
src = "data/sample/pos_sales.csv"
dst = "data/raw/pos/year=2023/month=11/day=15/pos_sales.csv"
shutil.copy2(src, dst)
```

Path is pushed to XCom for downstream tasks.

#### Task 3: Data Quality Check (Branch)

Runs 4 validation checks on raw POS data:
- Row count > 100
- `transaction_id` has zero nulls
- Less than 1% negative quantities
- Less than 2% duplicate transaction IDs

**If all pass →** branches to `transform_pos_data`  
**If any fail →** branches to `alert_dq_failure` and logs to `dq_check_log`

#### Task 4: Transform POS Data

Placeholder for PySpark spark-submit (runs as Python ETL in Docker mode). The actual cleaning logic is applied in Tasks 5–6.

#### Tasks 5 & 6: Load Dimensions (Parallel)

**`load_dim_product`** and **`load_dim_store`** upsert dimension data from the POS CSV:

```sql
INSERT INTO warehouse.dim_product (product_id, product_name, category, brand, unit_price)
VALUES (...)
ON CONFLICT (product_id) DO UPDATE SET ...
```

#### Task 7: Load Fact Sales

Builds lookup maps from dim tables, then bulk-inserts 500 rows per batch:

```python
# Build surrogate key maps
product_map = {"P0001": 1, "P0002": 2, ...}
store_map   = {"S001": 1, "S002": 2, ...}
time_map    = {"2023-11-15": 1415, ...}

# Look up FKs and compute metrics
net_revenue = qty * unit_price * (1 - discount)

# Bulk insert with psycopg2 execute_batch
execute_batch(cur, INSERT_SQL, records, page_size=500)
```

#### Task 8: Aggregate Demand

Runs inline SQL (no file dependency) to populate `agg_daily_demand` and `agg_weekly_demand`:

```sql
INSERT INTO warehouse.agg_daily_demand (...)
SELECT t.full_date, p.product_id, s.store_id, ...
FROM warehouse.fact_sales fs
JOIN warehouse.dim_time t ON ...
GROUP BY ...
ON CONFLICT (agg_date, product_id, store_id) DO UPDATE SET ...
```

#### Task 9: Export ML Dataset

Queries `agg_daily_demand` and writes a CSV to `data/processed/ml_ready/dt=YYYY-MM-DD/`.

#### Task 10: Pipeline Summary

Logs final row counts across all warehouse tables.

### PySpark Cleaning Rules (pos_transformer.py)

| Rule | Action |
|------|--------|
| Null `transaction_id` | Drop row |
| Null `product_id` | Drop row |
| Null `store_id` | Drop row |
| Unparseable date | Drop row |
| `quantity` ≤ 0 | Drop row |
| `unit_price` ≤ 0 | Drop row |
| Null `discount` | Fill with 0.0 |
| Null `product_name` | Fill with `product_id` |
| Duplicate `transaction_id` | Keep first (by date order) |
| `net_revenue` | Recalculate: `qty × price × (1 - discount)` |

---

## 7. Streaming Pipeline

### Architecture

```
ecommerce_producer.py
    │ Publishes 10 events/sec
    │ Key: product_id (ensures ordered processing per product)
    │ Compression: gzip
    │ acks=all (no message loss)
    ▼
Kafka Topic: ecommerce_events (6 partitions)
    │
    ▼
ecommerce_consumer.py
    │ Consumer group: retail-demand-pipeline
    │ Buffers 500 events OR 30 seconds (whichever first)
    │ Manual offset commit (only after successful sink)
    ├──► warehouse.stream_ecommerce_events (PostgreSQL)
    └──► data/raw/ecommerce/year=Y/month=M/day=D/hour=H/*.jsonl (Data Lake)
```

### Consumer Micro-batch Pattern

```python
# Buffer events in memory
buffer = []

while True:
    records = consumer.poll(timeout_ms=1000)
    for msg in records:
        if validate_event(msg.value):
            buffer.append(transform_event(msg.value))

    # Flush when batch full OR time elapsed
    if len(buffer) >= 500 or time_since_flush >= 30:
        pg_sink.flush(buffer)       # Write to PostgreSQL
        lake_sink.flush(buffer)     # Write JSONL to Data Lake
        consumer.commit()           # Commit only after successful write
        buffer.clear()
```

### Validation Rules

| Check | Action on Failure |
|-------|-------------------|
| Missing `event_id` | Drop silently |
| Missing `product_id` | Drop silently |
| Unknown `event_type` | Drop silently |
| Negative `price` | Drop silently |
| Duplicate `event_id` | `ON CONFLICT DO NOTHING` in DB |

### Dead Letter Queue

Invalid events that repeatedly fail are routed to `ecommerce_events_dlq` (2 partitions, 7-day retention) for manual investigation.

---

## 8. Data Warehouse Schema

### Star Schema Design

```
                     ┌─────────────────┐
                     │    dim_time     │
                     │─────────────────│
                     │ time_key PK     │
                     │ full_date UNIQUE│
                     │ day_of_week     │
                     │ week_of_year    │
                     │ month_num       │
                     │ quarter · year  │
                     │ is_weekend      │
                     │ is_holiday      │
                     │ holiday_name    │
                     └────────┬────────┘
                              │ FK
┌─────────────────┐           │           ┌─────────────────┐
│   dim_product   │           │           │    dim_store    │
│─────────────────│     ┌─────▼─────┐     │─────────────────│
│ product_key PK  │◄────│ fact_sales│────►│ store_key PK    │
│ product_id UNQ  │  FK │───────────│ FK  │ store_id UNQ    │
│ product_name    │     │ sales_key │     │ store_name      │
│ category        │     │ time_key  │     │ store_type      │
│ brand           │     │ prod_key  │     │ city · state    │
│ unit_price      │     │ store_key │     │ region          │
│ SCD Type 2 cols │     │ txn_id    │     │ lat · long      │
└─────────────────┘     │ qty_sold  │     └─────────────────┘
                        │ net_rev   │
                        │ gross_rev │
                        │ discount  │
                        └─────┬─────┘
                              │ Aggregated
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
   ┌──────────────────┐  ┌──────────────┐  ┌──────────────────┐
   │ agg_daily_demand │  │agg_weekly_dem│  │stream_ecom_events│
   │──────────────────│  │──────────────│  │──────────────────│
   │ agg_date         │  │ year         │  │ event_id UNIQUE  │
   │ product_id       │  │ week_of_year │  │ event_type       │
   │ store_id         │  │ product_id   │  │ event_timestamp  │
   │ total_quantity   │  │ total_qty    │  │ product_id       │
   │ total_revenue    │  │ wow_change%  │  │ user_id          │
   │ total_orders     │  │ avg_daily    │  │ device · platform│
   │ is_holiday       │  └──────────────┘  └──────────────────┘
   │ temp_high_f      │
   └──────────────────┘
```

### Table Reference

#### `warehouse.dim_time`
Pre-populated with every date from 2020-01-01 to 2030-12-31 (4,018 rows). Never changes — all time lookups use this table.

#### `warehouse.dim_product`
Uses **SCD Type 2** with `effective_from`, `effective_to`, `is_current_record` columns. When a product's price changes, a new row is inserted and the old row's `effective_to` is set.

#### `warehouse.fact_sales`
**Grain:** One row per product per transaction.  
All monetary values stored as `NUMERIC(12,2)`.  
`ON CONFLICT DO NOTHING` on `transaction_id` prevents duplicates from reprocessing.

#### `warehouse.agg_daily_demand`
Pre-computed daily aggregates. Populated by `aggregate_daily_demand` task. Used as the primary table for dashboards and ML export. `UNIQUE(agg_date, product_id, store_id)` enables idempotent upserts.

---

## 9. Data Quality Framework

### Check Types

```python
from spark.quality.data_quality_checks import DataQualityChecker

checker = DataQualityChecker(spark, df, table_name="pos_sales_raw")

# Available checks
checker.check_null_rate("column", threshold=0.99)
checker.check_uniqueness("column", threshold=1.0)
checker.check_range("column", min_val=1, max_val=10000)
checker.check_value_in_set("column", {"Cash","Credit","Debit"})
checker.check_date_format("column", fmt="yyyy-MM-dd")
checker.check_completeness(["col1","col2","col3"])
checker.check_referential_integrity("fk_col", ref_df, "pk_col")
```

### CheckResult Structure

```python
@dataclass
class CheckResult:
    check_name:     str
    table_name:     str
    check_type:     str
    total_records:  int
    passed_records: int
    failed_records: int
    pass_rate:      float    # 0.0 – 1.0
    threshold:      float    # minimum acceptable pass_rate
    status:         str      # "PASS" | "WARN" | "FAIL"
```

**Status logic:**
- `PASS` if `pass_rate >= threshold`
- `WARN` if `pass_rate >= threshold * 0.95` (within 5% of threshold)
- `FAIL` if `pass_rate < threshold * 0.95`

### DQ Log Table

All check results are persisted to `warehouse.dq_check_log` for trending and alerting:

```sql
SELECT check_name, status, pass_rate, run_timestamp
FROM warehouse.dq_check_log
WHERE DATE(run_timestamp) = CURRENT_DATE
ORDER BY status DESC, run_timestamp DESC;
```

---

## 10. Airflow DAG Reference

### DAG: `retail_demand_batch_pipeline`

| Property | Value |
|----------|-------|
| Schedule | `0 2 * * *` (02:00 UTC daily) |
| Catchup | Disabled |
| Max active runs | 1 |
| Retries | 1 × 5 min backoff |
| Timeout | 2 hours per task |

**Task dependency graph:**
```
start
  ├── extract_pos
  │     └── data_quality_check
  │           ├── [FAIL] alert_dq_failure → end
  │           └── [PASS] transform_pos_data
  │                 ├── load_dim_product
  │                 └── load_dim_store
  │                       └── load_fact_sales
  │                             └── aggregate_daily_demand
  └── extract_ecommerce ──────────────────┘
                                          └── export_ml_dataset
                                                └── pipeline_summary
                                                      └── end
```

### DAG: `retail_data_quality`

| Property | Value |
|----------|-------|
| Schedule | `0 4 * * *` (04:00 UTC daily) |
| Trigger | After `retail_demand_batch_pipeline` via ExternalTaskSensor |

Validates warehouse table integrity after load:
- `fact_sales` row count, negative revenue, orphaned FKs
- Dimension table uniqueness and completeness
- `agg_daily_demand` date coverage and gap detection

### Airflow Connection Required

```bash
# Must be created before triggering DAGs
airflow connections add postgres_retail_dw \
  --conn-type postgres \
  --conn-host postgres \
  --conn-schema retail_dw \
  --conn-login airflow \
  --conn-password airflow \
  --conn-port 5432
```

---

## 11. PySpark Transformations

### Session Configuration

```python
SparkSession.builder
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.shuffle.partitions", "50")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
```

### POS Transformer (`pos_transformer.py`)

**Input:** Raw CSV from Data Lake  
**Output:** Cleaned Parquet to Data Lake + JDBC to `fact_sales`

Key transformations:
1. Explicit schema read (no `inferSchema` — 3x faster)
2. Cast `transaction_date` with `F.to_date()`
3. Window-based deduplication: `row_number() OVER (PARTITION BY transaction_id ORDER BY date)`
4. Left-join weather on `(transaction_date, state)`
5. Left-join holidays on `transaction_date`
6. Recalculate `net_revenue = qty × price × (1 - discount)`

### E-commerce Transformer (`ecommerce_transformer.py`)

**Input:** JSONL files from Data Lake  
**Output:** Cleaned Parquet + JDBC to `stream_ecommerce_events`

Handles nested `geo` struct, deduplicates on `event_id`, filters unknown `event_type` values.

### Demand Aggregator (`demand_aggregator.py`)

**Input:** Processed POS Parquet  
**Output:** `agg_daily_demand`, `agg_weekly_demand`, lag features

Uses Spark window functions for lag computation — significantly faster than SQL window functions over millions of rows in PostgreSQL:

```python
w = Window.partitionBy("product_id", "store_id").orderBy("agg_date")
df = df.withColumn("demand_lag_7d", F.lag("total_quantity", 7).over(w))
```

---

## 12. Aggregation Layer

### Daily Demand (`agg_daily_demand`)

**Grain:** product × store × date  
**Populated by:** `aggregate_daily_demand` Airflow task  
**Update strategy:** `ON CONFLICT DO UPDATE` (idempotent upsert)

Key metrics:
- `total_quantity` — sum of units sold
- `total_revenue` — sum of net revenue after discounts
- `total_orders` — count of distinct transactions
- `avg_order_value` — revenue / orders
- `discount_rate` — discount_amount / gross_revenue

### Weekly Demand (`agg_weekly_demand`)

**Grain:** product × store × year × week  
Adds `wow_change_pct` — week-over-week percentage change vs prior week.

### Analytical Views

10 pre-built SQL views for common business queries:

| View | Purpose |
|------|---------|
| `v_top_products_monthly` | Top products by revenue per month |
| `v_store_performance` | QTD vs same quarter last year |
| `v_category_demand_trend` | 7-day and 30-day rolling average by category |
| `v_ml_ready_dataset` | Full feature set for ML (includes lag features) |

Plus query templates in `sql/queries/analytical_queries.sql`:
- Month-over-month revenue trend
- Demand spike detection (Z-score > 2)
- Holiday impact analysis
- Customer cohort retention
- Product ABC classification (80/15/5 revenue split)
- Discount effectiveness analysis
- Slow movers (zero sales in 30 days)

---

## 13. ML Feature Engineering

### Dataset Generation

```bash
python scripts/export_ml_dataset.py --format csv --output data/processed/ml/
```

### Feature Catalog

| Feature | Type | Description | How Computed |
|---------|------|-------------|--------------|
| `target_demand` | INT | Units sold (predict this) | `SUM(quantity_sold)` |
| `demand_lag_7d` | INT | Units 7 days ago | `LAG(qty, 7) OVER (PARTITION BY product, store ORDER BY date)` |
| `demand_lag_14d` | INT | Units 14 days ago | `LAG(qty, 14) OVER (...)` |
| `demand_lag_28d` | INT | Units 28 days ago | `LAG(qty, 28) OVER (...)` |
| `rolling_avg_7d` | FLOAT | 7-day moving average | `AVG(qty) OVER (...ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING)` |
| `rolling_avg_28d` | FLOAT | 28-day moving average | Same, 28-day window |
| `day_of_week` | INT | 1=Mon … 7=Sun | From `dim_time` |
| `week_of_year` | INT | ISO week 1–53 | From `dim_time` |
| `month_num` | INT | 1–12 | From `dim_time` |
| `quarter` | INT | 1–4 | From `dim_time` |
| `is_holiday` | INT | 1 if public holiday, 0 otherwise | From `dim_time.is_holiday` |
| `temp_high_f` | FLOAT | Daily high temperature °F | Joined from weather data |
| `precipitation_in` | FLOAT | Daily precipitation inches | Joined from weather data |
| `discount_rate` | FLOAT | Average discount for the day | From `agg_daily_demand` |

### Feature Validation

`export_ml_dataset.py` runs these checks automatically:

- **Null rates** per column (lag features will have nulls for first N days — expected)
- **Target distribution** — mean, std, min, max, zero count, negative count
- **Lag coverage** — % of rows with all 3 lags populated (should be > 80%)
- **Date range** — confirms full history is present

### Recommended Models

| Model | When to Use |
|-------|------------|
| LightGBM / XGBoost | Best overall accuracy, handles lag features natively |
| Facebook Prophet | Strong seasonality, built-in holiday support |
| LSTM / Temporal Fusion Transformer | Complex multi-store patterns, sufficient data |
| SARIMA | Simple univariate baseline |

---

## 14. Dashboard & Forecasting

### Streamlit Dashboard (`streamlit/dashboard.py`)

**URL:** http://localhost:8501

**Data mode auto-detection:**
- `🟢 Live DB` — connects to `retail_dw` and queries warehouse tables
- `🟡 Demo Mode` — falls back to synthetic data if DB is unavailable

**Visualizations:**

| Chart | Data Source | Library |
|-------|-------------|---------|
| KPI cards (4) | `agg_daily_demand` | HTML/CSS |
| Daily demand trend | `agg_daily_demand` | Plotly |
| Revenue by category | `agg_daily_demand` | Plotly |
| Demand by region | `agg_daily_demand` | Plotly |
| Day-of-week heatmap | `agg_daily_demand` | Plotly |
| Top products table | `agg_daily_demand` | Streamlit dataframe |
| ML dataset download | `agg_daily_demand` | Streamlit download button |

### Forecasting Page (`streamlit/forecasting.py`)

Three models combined into an ensemble:

**Moving Average (MA)**
```python
forecast = series.iloc[-7:].mean()  # Last 7 days average
```

**Linear Trend (LT)**
```python
slope, intercept = np.polyfit(x[-30:], y[-30:], 1)
forecast = slope * future_x + intercept
```

**Seasonal Naive (SN)**
```python
forecast = series.iloc[-7:].values  # Repeat last week
```

**Ensemble**
```python
ensemble = (MA + LT + SN) / 3
std = pd.DataFrame({"ma":MA, "lt":LT, "sn":SN}).std(axis=1)
lower_80 = (ensemble - 1.28 * std).clip(lower=0)
upper_80 =  ensemble + 1.28 * std
lower_95 = (ensemble - 1.96 * std).clip(lower=0)
upper_95 =  ensemble + 1.96 * std
```

**Plotly color rules** — all colors use 6-digit hex or `rgba()`. 8-digit hex is not supported by Plotly:
```python
# ✅ Correct
marker_color="rgba(0,119,182,0.44)"
gridcolor="rgba(255,255,255,0.08)"

# ❌ Wrong — Plotly rejects 8-digit hex
marker_color="#0077b670"
```

---

## 15. Monitoring & Alerting

### Structured Logging

All pipeline components use the `monitoring.py` structured logger:

```python
from monitoring.monitoring import get_logger
log = get_logger("pos_transformer")
log.info("ETL complete")   # JSON output in production, human-readable locally
```

Set `LOG_FORMAT=json` in environment for structured JSON log output compatible with Datadog, CloudWatch, and ELK Stack.

### Pipeline Metrics

```python
from monitoring.monitoring import PipelineMetrics

metrics = PipelineMetrics("pos_etl")
metrics.record("rows_read", 50000)
metrics.record("rows_written", 49200)
metrics.increment("errors_dropped", 800)
summary = metrics.finish("success")
# Writes to logs/pipeline_metrics.jsonl
# Exports to logs/metrics.prom for Prometheus
```

### Prometheus Metrics

Start the monitoring stack:
```bash
docker compose -f docker/docker-compose.yml \
               -f docker/docker-compose.monitoring.yml up -d
```

Access:
- **Prometheus:** http://localhost:9090
- **Grafana:** http://localhost:3000 (admin/admin)

### Alert Rules (8 total)

| Alert | Condition | Severity |
|-------|-----------|----------|
| `KafkaConsumerLagHigh` | Consumer lag > 10,000 for 5 min | Warning |
| `KafkaBrokerDown` | Broker unreachable for 1 min | Critical |
| `PostgresDown` | DB unreachable for 1 min | Critical |
| `PostgresConnectionsHigh` | Active connections > 90 | Warning |
| `PipelineETLFailed` | ETL status metric = 0 | Critical |
| `PipelineETLSlow` | ETL elapsed > 3600s | Warning |
| `DQCheckFailed` | DQ status metric = 0 | Warning |
| `DataLakeDiskLow` | Disk < 10% free | Warning |

---

## 16. Configuration Reference

### Environment Variables

All settings are read from environment variables with defaults. Copy `config/.env.example` to `config/.env`:

| Variable | Default | Description |
|----------|---------|-------------|
| `PG_HOST` | `localhost` | PostgreSQL host |
| `PG_PORT` | `5432` | PostgreSQL port |
| `PG_DBNAME` | `retail_dw` | Data Warehouse database |
| `PG_USER` | `airflow` | Database user |
| `PG_PASSWORD` | `airflow` | Database password |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka brokers |
| `KAFKA_TOPIC` | `ecommerce_events` | Main event topic |
| `KAFKA_GROUP_ID` | `retail-demand-pipeline` | Consumer group |
| `CONSUMER_BATCH_SIZE` | `500` | Events per flush |
| `CONSUMER_FLUSH_SEC` | `30` | Flush interval (seconds) |
| `DATA_LAKE_PATH` | `./data` | Base data lake path |
| `SPARK_MASTER` | `local[*]` | Spark master URL |
| `SPARK_DRIVER_MEMORY` | `2g` | Spark driver memory |

### Centralised Config (`config/config.py`)

```python
from config.config import config

# Access any setting
config.db.dsn          # Full PostgreSQL DSN
config.db.jdbc_url     # JDBC URL for Spark
config.kafka.topic     # Kafka topic name
config.lake.partition_path("raw", "pos", "2023-11-15")
# → "data/raw/pos/year=2023/month=11/day=15"
```

---

## 17. Deployment Guide

### Local Development (Option A — Docker)

```bash
# One-command full setup
make quickstart

# Manual steps
pip install -r requirements.txt
python scripts/generate_sample_data.py
docker compose -f docker/docker-compose.yml up -d
python scripts/setup_db.py  # Only if running outside Docker
docker exec retail_airflow_web airflow connections add postgres_retail_dw \
  --conn-type postgres --conn-host postgres --conn-schema retail_dw \
  --conn-login airflow --conn-password airflow --conn-port 5432
docker exec retail_airflow_web airflow dags trigger retail_demand_batch_pipeline
```

### Local Development (Option B — No Docker)

```bash
# Requires: PostgreSQL, Kafka, Java 17, Spark installed locally
pip install -r requirements.txt
python scripts/generate_sample_data.py
python scripts/setup_db.py
python scripts/init_kafka_topics.py
python kafka/producer/ecommerce_producer.py &
python kafka/consumer/ecommerce_consumer.py &
spark-submit spark/transformations/pos_transformer.py --execution-date $(date +%Y-%m-%d)
streamlit run streamlit/dashboard.py
```

### AWS Production

| Component | AWS Service | Notes |
|-----------|-------------|-------|
| Data Lake | Amazon S3 | Change `DATA_LAKE_PATH=s3://bucket/prefix` |
| Data Warehouse | Amazon RDS PostgreSQL | Same schema, change connection string |
| Airflow | Amazon MWAA | Upload DAGs to S3 DAGs bucket |
| Kafka | Amazon MSK | Change `KAFKA_BOOTSTRAP_SERVERS` |
| Spark | Amazon EMR | Change `SPARK_MASTER=yarn` |
| Dashboard | AWS EC2 / ECS | Expose port 8501 |

### Backfilling Historical Data

```bash
# Airflow mode (triggers DAG runs for each date)
python scripts/backfill_pipeline.py \
  --start 2023-01-01 --end 2023-12-31 --mode airflow

# Local mode (runs PySpark directly)
python scripts/backfill_pipeline.py \
  --start 2023-11-01 --end 2023-11-30 --mode local
```

---

## 18. Testing Guide

### Test Structure

```
tests/
├── conftest.py              # Shared fixtures (spark_session, pg_conn, sample data)
├── test_transformations.py  # 12 Spark ETL unit tests
├── test_quality_checks.py   # 20 unit tests (config, monitoring, Kafka, DQ)
└── test_integration.py      # Integration tests (DB schema, pipeline, health)
```

### Running Tests

```bash
# All unit tests (no infrastructure)
pytest tests/test_quality_checks.py tests/test_transformations.py -v

# Integration tests (needs PostgreSQL)
pytest tests/test_integration.py -v -m integration

# Specific test class
pytest tests/test_transformations.py::TestPosCleaning -v

# With coverage report
pytest tests/ --cov=. --cov-report=html --cov-report=term-missing

# Exclude slow Spark tests
pytest tests/ -m "not slow" -v
```

### Test Markers

| Marker | Description |
|--------|-------------|
| `@pytest.mark.slow` | Tests requiring JVM/Spark startup |
| `@pytest.mark.integration` | Tests requiring live DB/Kafka |
| `@pytest.mark.unit` | Pure unit tests, no external deps |

### Key Fixtures

```python
# In conftest.py — auto-loaded by pytest
spark_session   # Session-scoped SparkSession (local[1])
pg_conn         # Session-scoped PostgreSQL connection (skips if unavailable)
sample_pos_df   # pandas DataFrame from pos_sales.csv
sample_ecom_events  # list of event dicts
mock_kafka_producer # Monkeypatched KafkaProducer
```

---

## 19. Troubleshooting Reference

### Container Issues

```bash
# Check all container statuses
docker compose -f docker/docker-compose.yml ps

# View logs for a specific container
docker logs retail_kafka_consumer --tail 50
docker logs retail_airflow_scheduler --tail 50
docker logs retail_postgres --tail 50

# Restart a single container
docker restart retail_dashboard

# Full reset
docker compose -f docker/docker-compose.yml down -v
docker compose -f docker/docker-compose.yml up -d
```

### Common Errors

| Error | Cause | Fix |
|-------|-------|-----|
| `database "retail_dw" does not exist` | `create_db.sql` not mounted or ran in wrong order | Check `01_create_db.sql` is in `/docker-entrypoint-initdb.d/` |
| `relation "warehouse.X" does not exist` | Querying with `-d airflow` instead of `-d retail_dw` | Use `psql -d retail_dw` |
| `bootstrap-server must be specified` | YAML `>` folding breaks multi-line kafka-topics command | Use `|` block scalar instead of `>` |
| `ValueError: not enough values to unpack` | `load_data()` return value mismatch | Dashboard `_generate_demo_data()` must be unpacked before adding `"demo"` |
| `Invalid color #XXXXXXXX` in Plotly | 8-digit hex not supported by Plotly | Convert to `rgba(r,g,b,a)` |
| `relation "log" does not exist` | Airflow metadata tables not yet created | Wait for `airflow db migrate` to complete |
| `kafka-producer` not starting | Depends on `kafka-setup` which failed | Fix kafka-setup command; use `restart: on-failure` |

### Database Diagnostics

```bash
# Verify retail_dw exists
docker exec retail_postgres psql -U airflow -c "\l"

# List all warehouse tables
docker exec retail_postgres psql -U airflow -d retail_dw -c "\dt warehouse.*"

# Row counts across all tables
docker exec retail_postgres psql -U airflow -d retail_dw -c "
SELECT 'dim_time'               AS t, COUNT(*) FROM warehouse.dim_time
UNION ALL SELECT 'dim_product',          COUNT(*) FROM warehouse.dim_product
UNION ALL SELECT 'dim_store',            COUNT(*) FROM warehouse.dim_store
UNION ALL SELECT 'fact_sales',           COUNT(*) FROM warehouse.fact_sales
UNION ALL SELECT 'agg_daily_demand',     COUNT(*) FROM warehouse.agg_daily_demand
UNION ALL SELECT 'stream_events',        COUNT(*) FROM warehouse.stream_ecommerce_events;
"

# Check DQ failures
docker exec retail_postgres psql -U airflow -d retail_dw -c "
SELECT check_name, status, pass_rate, run_timestamp
FROM warehouse.dq_check_log
WHERE status != 'PASS'
ORDER BY run_timestamp DESC LIMIT 20;"
```

### Kafka Diagnostics

```bash
# List topics
docker exec retail_kafka kafka-topics --bootstrap-server localhost:9092 --list

# Check consumer group lag
docker exec retail_kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group retail-demand-pipeline --describe

# View last 5 messages in topic
docker exec retail_kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic ecommerce_events \
  --from-beginning --max-messages 5
```

---

## 20. Architecture Decision Records

### ADR-001: Lambda Architecture over Kappa

**Decision:** Use Lambda (batch + stream) instead of Kappa (stream-only).

**Reasoning:** POS data arrives as daily CSV exports — stream-only processing would require a batch ingestion path anyway. Lambda cleanly separates the reliable daily batch from the real-time stream without forcing streaming semantics onto batch data.

### ADR-002: PostgreSQL over BigQuery/Snowflake

**Decision:** PostgreSQL 15 as the Data Warehouse.

**Reasoning:** For sub-1M rows/day volume, PostgreSQL with partitioning matches cloud DW query performance at zero cost. JDBC interface enables seamless PySpark writes. Migration path is straightforward — swap the JDBC target URL.

### ADR-003: PySpark over pandas for ETL

**Decision:** PySpark DataFrame API for all transformations.

**Reasoning:** Identical API scales from `local[*]` to 100-node EMR without code changes. A pandas-based pipeline would require full rewrite at 10× data volume. The marginal overhead on small datasets is acceptable.

### ADR-004: Star Schema over Snowflake Schema

**Decision:** Denormalised Star Schema (dim tables not further normalised).

**Reasoning:** Fewer JOINs in analytics queries. BI tools (Tableau, Streamlit) navigate Star Schema naturally. Minor storage overhead (duplicate city/state in `dim_store`) is acceptable vs query speed.

### ADR-005: SCD Type 2 on dim_product

**Decision:** Slowly Changing Dimension Type 2 on `dim_product` for price tracking.

**Reasoning:** Preserves historical unit prices so revenue reports always show the correct price at time of sale. Alternative (Type 1 overwrite) would corrupt historical revenue calculations when prices change.

### ADR-006: Inline SQL in DAG over file-based SQL

**Decision:** Embed SQL as Python string constants in the DAG file.

**Reasoning:** Airflow's `PostgresOperator` resolves SQL file paths relative to the Jinja template search path (DAGs folder), causing `TemplateNotFound` errors when the file is in `sql/aggregations/`. Inline SQL eliminates the file path dependency.

### ADR-007: Custom Airflow image over `_PIP_ADDITIONAL_REQUIREMENTS`

**Decision:** Build a custom `Dockerfile.airflow` with pandas pre-installed.

**Reasoning:** `_PIP_ADDITIONAL_REQUIREMENTS` installs packages on **every container start**, adding 2–3 minutes to `airflow-init` startup. This creates a race condition where the postgres healthcheck passes before `db migrate` finishes. Pre-baking pandas into the image makes startup deterministic and fast.

---

*Documentation maintained by the Data Engineering team. For issues or contributions, please open a GitHub issue.*
