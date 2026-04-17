# Retail Demand Forecasting Pipeline — Technical Documentation

## Table of Contents
1. [Architecture Decisions](#architecture-decisions)
2. [Data Flow Walkthrough](#data-flow-walkthrough)
3. [ETL Step-by-Step](#etl-step-by-step)
4. [Schema Design Rationale](#schema-design-rationale)
5. [Partitioning Strategy](#partitioning-strategy)
6. [Data Quality Framework](#data-quality-framework)
7. [Deployment Guide](#deployment-guide)
8. [Scaling Guide](#scaling-guide)
9. [ML Feature Engineering](#ml-feature-engineering)
10. [Troubleshooting](#troubleshooting)

---

## 1. Architecture Decisions

### Why Lambda Architecture?

Lambda architecture combines batch and streaming pipelines:

- **Batch layer** (Airflow + PySpark): Processes previous day's POS data at 02:00 UTC. Authoritative, allows full reprocessing if bugs are found. Handles the bulk of data volume (50K+ transactions/day).
- **Speed layer** (Kafka + Consumer): Captures e-commerce clickstream in near real-time (<30s latency). Enables same-day demand signals before the batch pipeline runs.
- **Serving layer** (PostgreSQL views + Streamlit): Merges batch and stream outputs into unified analytics.

### Why PostgreSQL over BigQuery/Snowflake?

For this project scope (single-region retail, <1M rows/day), PostgreSQL with table partitioning delivers query performance comparable to cloud DWs at zero cost. The JDBC interface allows seamless PySpark integration. For 10B+ row scale, swap the JDBC write target to BigQuery or Snowflake — the PySpark code is unchanged.

### Why PySpark over pandas?

PySpark's DataFrame API is identical at small scale but parallelises automatically as data grows. A pipeline built on pandas would need rewriting at 10x data volume. PySpark processes the same code on a local machine (local[*]) or a 100-node EMR cluster.

---

## 2. Data Flow Walkthrough

```
Day D-1 22:00  → POS system exports CSV to SFTP / S3
Day D   02:00  → Airflow DAG triggers:
                   extract_pos     : copies CSV to data lake (partitioned)
                   extract_ecom    : copies JSON to data lake
                   data_quality    : validates raw data
                   transform_pos   : PySpark ETL → Parquet + JDBC → fact_sales
                   transform_ecom  : PySpark ETL → Parquet + JDBC → stream_events
                   aggregate       : SQL → agg_daily_demand, agg_weekly_demand
                   export_ml       : SQL view → demand_features.csv
Day D   04:00  → DQ DAG validates warehouse tables
Day D   08:00  → Data scientists consume demand_features.csv for ML training
Continuous     → Kafka producer streams e-commerce events at 10/sec
Continuous     → Kafka consumer buffers 500 events → flush to DB + lake
```

---

## 3. ETL Step-by-Step

### Batch: POS Data (`pos_transformer.py`)

| Step | Action | Implementation |
|------|--------|----------------|
| 1 | Read raw CSV | `spark.read.schema(POS_SCHEMA).csv(path)` — explicit schema, no type inference |
| 2 | Drop critical nulls | Filter: transaction_id, product_id, store_id must be non-null |
| 3 | Cast transaction_date | `F.to_date(col, "yyyy-MM-dd")` then filter nulls |
| 4 | Remove invalid rows | quantity > 0, unit_price > 0 |
| 5 | Fill missing discount | `fillna({"discount": 0.0})` |
| 6 | Deduplicate | `row_number() OVER (PARTITION BY transaction_id ORDER BY date)` → keep rn=1 |
| 7 | Recalculate revenue | `net_revenue = qty × price × (1 - discount)` — never trust source totals |
| 8 | Enrich time features | `dayofweek`, `weekofyear`, `month`, `quarter`, `is_weekend` |
| 9 | Join weather | Left join on (transaction_date, state) |
| 10 | Join holidays | Left join on transaction_date → add `is_holiday`, `holiday_name` |
| 11 | Write Parquet | Hive-partitioned: `year=Y/month=M/day=D/` |
| 12 | Write JDBC | Batch 5000 rows, 4 parallel partitions → `fact_sales` |

### Stream: E-commerce Events (`ecommerce_consumer.py`)

| Step | Action | Notes |
|------|--------|-------|
| 1 | Poll Kafka | 1s timeout, auto_offset_reset=earliest |
| 2 | Validate | Check required fields, valid event_type, non-negative price |
| 3 | Transform | Flatten geo dict, strip Z from timestamp, normalise fields |
| 4 | Buffer | Accumulate 500 events or 30 seconds |
| 5 | Flush to PostgreSQL | `execute_batch` INSERT ON CONFLICT DO NOTHING |
| 6 | Flush to Data Lake | Write hourly JSONL: `raw/ecommerce/year=Y/month=M/day=D/hour=H/` |
| 7 | Commit offset | Only after successful dual-sink write (prevents data loss) |

---

## 4. Schema Design Rationale

### Star Schema vs Snowflake Schema

We chose **Star Schema** (denormalized) over Snowflake (normalized) because:
- Fewer JOINs in analytical queries → faster dashboard queries
- Simpler for BI tools (Tableau, Power BI, Streamlit) to navigate
- Slightly more storage (duplicate city/state in dim_store) is acceptable vs query speed

### SCD Type 2 on dim_product

`dim_product` uses Slowly Changing Dimension Type 2 for price tracking:
```sql
effective_from     DATE NOT NULL DEFAULT CURRENT_DATE,
effective_to       DATE,                     -- NULL = current record
is_current_record  BOOLEAN DEFAULT TRUE
```
This preserves historical unit_price, so a December sale report shows December's prices, not today's.

### fact_sales Grain

One row per **product per transaction** (not per transaction). A single transaction buying 3 different products = 3 fact rows. This enables product-level demand analysis without JSON arrays.

---

## 5. Partitioning Strategy

### Data Lake (Hive-style)
```
data/raw/pos/year=2023/month=11/day=15/pos_sales.csv
data/raw/ecommerce/year=2023/month=11/day=15/hour=14/batch_1700000000.jsonl
```
- Airflow/PySpark reads only the partition for the relevant date → no full-table scan
- Athena / Spark can use partition pruning automatically

### PostgreSQL (Range Partitioning)
```sql
-- fact_sales partitioned by time_key ranges (quarterly)
fact_sales_2023_q1  → time_keys for Jan-Mar 2023
fact_sales_2023_q2  → time_keys for Apr-Jun 2023
```
- `EXPLAIN ANALYZE` on date-filtered queries shows Partition Pruning: enabled
- Old partitions can be detached and archived without locking the table

---

## 6. Data Quality Framework

### Check Types

| Check | Threshold | Failure Action |
|-------|-----------|----------------|
| `null_check` | 99% non-null (critical fields: 100%) | FAIL → branch to alert |
| `uniqueness` | 99% unique on transaction_id | FAIL → block load |
| `range_check` | quantity: 1-10000, price: 0.01-100000 | WARN if <0.5% bad |
| `value_set` | payment_method in known set | WARN |
| `date_format` | All dates parse as yyyy-MM-dd | FAIL |
| `completeness` | All PK columns non-null together | FAIL |

### DQ Results Flow
```
raw data → DataQualityChecker.run_all_pos()
         → List[CheckResult]
         → PostgreSQL: dq_check_log (for trending/alerting)
         → Airflow XCom: pass/fail for branch decision
         → If FAIL → BranchPythonOperator → alert_dq_failure task
```

---

## 7. Deployment Guide

### Local Development
```bash
make quickstart   # Full setup in one command
```

### Docker Production
```bash
# Set environment variables
cp config/.env.example config/.env
# Edit .env with production credentials

docker-compose -f docker/docker-compose.yml up -d

# Scale Spark workers
docker-compose -f docker/docker-compose.yml up -d --scale spark-worker=3

# View service health
docker-compose -f docker/docker-compose.yml ps
```

### AWS Production
1. **Data Lake**: Replace `DATA_LAKE_PATH=./data` with `s3://your-bucket/retail-lake`
2. **PostgreSQL**: Use Amazon RDS PostgreSQL 15 (same schema, change connection string)
3. **Airflow**: Use Amazon MWAA or self-hosted on EKS
4. **Kafka**: Use Amazon MSK (Managed Streaming for Kafka)
5. **Spark**: Use Amazon EMR or Glue (change `SPARK_MASTER=yarn`)

---

## 8. Scaling Guide

| Bottleneck | Symptom | Solution |
|-----------|---------|----------|
| Kafka consumer lag | Consumer group lag > 10k events | Add consumer instances (same group_id) |
| PySpark slow | ETL >60 min | Add Spark workers; increase `spark.executor.cores` |
| PostgreSQL slow | Queries >10s | Add read replicas; partition more granularly |
| Airflow slow | DAG scheduling lag | Switch to CeleryExecutor with Redis |
| Data lake slow | Listing/reading files | Switch to Delta Lake or Apache Iceberg format |

---

## 9. ML Feature Engineering

The `v_ml_ready_dataset` SQL view and `demand_aggregator.py` produce these features:

### Target Variable
- `total_quantity` — daily units sold per product per store

### Lag Features (capture autocorrelation)
- `demand_lag_7d`  — same day last week
- `demand_lag_14d` — two weeks ago
- `demand_lag_28d` — four weeks ago (month seasonality)

### Rolling Features (capture trend)
- `rolling_avg_7d`  — 7-day moving average
- `rolling_avg_28d` — 28-day moving average
- `rolling_std_7d`  — 7-day demand volatility

### Calendar Features
- `day_of_week`, `week_of_year`, `month_num`, `quarter`
- `is_holiday`, `holiday_name` (one-hot encode for models)

### External Features
- `temp_high_f`, `precipitation_in` — weather impact on demand
- `discount_rate` — promotion signal

### Recommended Models
1. **LightGBM / XGBoost** — fast, handles lag features well, good baseline
2. **Prophet** (Facebook) — built-in seasonality, holiday effects
3. **LSTM / Temporal Fusion Transformer** — for complex multi-store patterns

---

## 10. Troubleshooting

### Kafka consumer not connecting
```bash
# Check Kafka is ready
docker exec retail_kafka kafka-topics --bootstrap-server localhost:9092 --list
# Check consumer group lag
docker exec retail_kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group retail-demand-pipeline --describe
```

### PySpark JDBC write fails
```bash
# Verify PostgreSQL is accepting connections
psql -h localhost -U airflow -d retail_dw -c "SELECT COUNT(*) FROM warehouse.fact_sales"
# Check JDBC jar is available
ls $SPARK_HOME/jars/postgresql-*.jar
```

### Airflow DAG not triggering
```bash
# Check scheduler logs
docker logs retail_airflow_scheduler --tail 50
# Unpause the DAG
airflow dags unpause retail_demand_batch_pipeline
# Trigger manually
airflow dags trigger retail_demand_batch_pipeline
```

### fact_sales partition missing
```sql
-- Check which partitions exist
SELECT schemaname, tablename FROM pg_tables
WHERE tablename LIKE 'fact_sales_%';
-- Create missing partition manually
CREATE TABLE fact_sales_2024_q1 PARTITION OF warehouse.fact_sales
  FOR VALUES FROM (366) TO (458);
```
