<div align="center">

# 🛒 Retail Demand Forecasting Data Pipeline

**A production-grade, end-to-end Data Engineering project implementing Lambda Architecture for retail demand forecasting and analytics.**

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8.1-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)](https://airflow.apache.org)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-7.5.3-231F20?style=flat-square&logo=apachekafka&logoColor=white)](https://kafka.apache.org)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.1-E25A1C?style=flat-square&logo=apachespark&logoColor=white)](https://spark.apache.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?style=flat-square&logo=postgresql&logoColor=white)](https://postgresql.org)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker&logoColor=white)](https://docker.com)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.31-FF4B4B?style=flat-square&logo=streamlit&logoColor=white)](https://streamlit.io)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=flat-square)](LICENSE)

[Features](#-features) • [Architecture](#-architecture) • [Quick Start](#-quick-start) • [Project Structure](#-project-structure) • [Tech Stack](#-tech-stack) • [Dashboard](#-dashboard) • [Documentation](#-documentation)

![Pipeline Architecture](https://img.shields.io/badge/Architecture-Lambda-success?style=for-the-badge)

</div>

---

## 📌 Overview

This project builds a **scalable, production-ready data pipeline** that ingests retail data from multiple sources (POS systems, e-commerce platforms, weather APIs), processes it using both batch and streaming pipelines, and prepares it for **demand forecasting and business analytics**.

The system is built on the **Lambda Architecture** pattern — combining:
- **Batch Layer** (Apache Airflow + PySpark) for accurate, historical data processing
- **Speed Layer** (Apache Kafka) for real-time event streaming
- **Serving Layer** (PostgreSQL + Streamlit) for analytics and ML-ready data export

---

## ✨ Features

| Feature | Description |
|---------|-------------|
| **Lambda Architecture** | Unified batch + streaming pipeline |
| **Automated Orchestration** | Airflow DAG runs daily at 02:00 UTC |
| **Real-time Streaming** | Kafka producer/consumer at 10 events/sec |
| **Star Schema DW** | Fact + dimension tables with SCD Type 2 |
| **Data Quality** | 8 configurable check types with logging |
| **ML-Ready Export** | Lag features, rolling averages, holiday flags |
| **Interactive Dashboard** | Demand trends, forecasting, KPI cards |
| **Full Containerization** | One-command Docker Compose startup |
| **Monitoring** | Prometheus + Grafana + alert rules |
| **32+ Tests** | Unit, Spark, and integration test suites |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      DATA SOURCES                            │
│   [POS CSV]      [E-commerce JSON]      [Weather/Holidays]  │
└──────┬──────────────────┬──────────────────────┬────────────┘
       │                  │                       │
       ▼                  ▼                       ▼
┌─────────────────────────────────────────────────────────────┐
│                    INGESTION LAYER                           │
│         [Apache Airflow]           [Apache Kafka]           │
│         Batch · Daily 02:00        Streaming · 10 evt/sec   │
└──────┬───────────────────────────────────┬──────────────────┘
       │                                   │
       ▼                                   ▼
┌─────────────────────────────────────────────────────────────┐
│                 DATA LAKE (Local / S3)                       │
│   raw/pos/year=Y/month=M/day=D/     raw/ecommerce/hour=H/   │
└──────┬────────────────────────────────────────────────────--┘
       │ PySpark ETL
       ▼
┌─────────────────────────────────────────────────────────────┐
│              PROCESSING LAYER (PySpark)                      │
│   Clean → Deduplicate → Enrich → Aggregate → Validate       │
└──────┬──────────────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────────────┐
│          DATA WAREHOUSE (PostgreSQL) — Star Schema          │
│   fact_sales │ dim_product │ dim_store │ dim_time           │
│   agg_daily_demand │ agg_weekly_demand │ stream_events      │
└──────┬──────────────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────────────┐
│                    SERVING LAYER                             │
│   [Streamlit Dashboard]     [ML-Ready CSV Export]           │
└─────────────────────────────────────────────────────────────┘
```

---

## 🚀 Quick Start

### Prerequisites

```bash
# Required
Docker Desktop    # https://www.docker.com/products/docker-desktop
Python 3.10+
Git
```

### 1. Clone the repository

```bash
git clone https://github.com/yourusername/retail-demand-pipeline.git
cd retail-demand-pipeline
```

### 2. Generate sample data

```bash
pip install faker pandas numpy
python scripts/generate_sample_data.py
```

### 3. Start all services

```bash
docker compose -f docker/docker-compose.yml up -d
```

> ⏳ First run takes ~5 minutes to build the Airflow image with pandas.

### 4. Verify everything is running

```bash
docker compose -f docker/docker-compose.yml ps
```

### 5. Add Airflow connection

```bash
docker exec retail_airflow_web airflow connections add postgres_retail_dw \
  --conn-type postgres \
  --conn-host postgres \
  --conn-schema retail_dw \
  --conn-login airflow \
  --conn-password airflow \
  --conn-port 5432
```

### 6. Trigger the pipeline

```bash
docker exec retail_airflow_web airflow dags trigger retail_demand_batch_pipeline
```

### 7. Open the dashboards

| Service | URL | Credentials |
|---------|-----|-------------|
| 🌊 Airflow | http://localhost:8080 | admin / admin |
| ⚡ Spark UI | http://localhost:8090 | — |
| 📊 Dashboard | http://localhost:8501 | — |
| 🐘 PostgreSQL | localhost:5432 | airflow / airflow |

---

## 📁 Project Structure

```
retail_demand_pipeline/
│
├── airflow/
│   ├── dags/
│   │   ├── batch_ingestion_dag.py    # Main 10-task batch pipeline
│   │   └── data_quality_dag.py       # Post-load DQ validation
│   └── plugins/
│       └── custom_operators.py       # 4 reusable Airflow operators
│
├── kafka/
│   ├── producer/ecommerce_producer.py  # Publishes events at 10/sec
│   └── consumer/ecommerce_consumer.py  # Micro-batch sink to DB + Lake
│
├── spark/
│   ├── transformations/
│   │   ├── pos_transformer.py          # POS ETL (12 cleaning steps)
│   │   ├── ecommerce_transformer.py    # E-commerce event ETL
│   │   └── demand_aggregator.py        # Daily/weekly + lag features
│   └── quality/
│       └── data_quality_checks.py      # 8 check types, structured results
│
├── sql/
│   ├── schema/
│   │   ├── create_db.sql               # Creates retail_dw database
│   │   └── star_schema.sql             # Full DDL — all 8 tables
│   ├── aggregations/
│   │   └── demand_aggregations.sql     # Daily + weekly aggregation SQL
│   └── queries/
│       └── analytical_queries.sql      # 10 BI queries (ABC, cohort, etc.)
│
├── streamlit/
│   ├── dashboard.py                    # Analytics: KPIs, trends, heatmap
│   ├── forecasting.py                  # MA + Linear + Seasonal ensemble
│   └── app.py                          # Multi-page router
│
├── docker/
│   ├── docker-compose.yml              # Full 12-service stack
│   ├── docker-compose.monitoring.yml   # Prometheus + Grafana overlay
│   ├── Dockerfile.airflow              # Airflow + pandas pre-installed
│   ├── Dockerfile.producer
│   ├── Dockerfile.consumer
│   └── Dockerfile.streamlit
│
├── monitoring/
│   ├── monitoring.py                   # Structured logger + metrics
│   ├── prometheus.yml                  # Scrape config
│   └── alert_rules.yml                 # 8 alert rules
│
├── scripts/
│   ├── generate_sample_data.py         # 50K POS + 20K events
│   ├── setup_db.py                     # DB initialization
│   ├── setup_airflow_connections.py
│   ├── init_kafka_topics.py
│   ├── backfill_pipeline.py            # Historical backfill
│   └── export_ml_dataset.py            # Feature-engineered CSV
│
├── tests/
│   ├── conftest.py                     # Shared fixtures
│   ├── test_transformations.py         # 12 Spark unit tests
│   ├── test_quality_checks.py          # 20 unit tests
│   └── test_integration.py             # DB + pipeline integration
│
├── config/
│   ├── config.py                       # Centralised config dataclass
│   └── .env.example                    # Environment template
│
├── data/
│   ├── sample/                         # Pre-generated test data
│   ├── raw/                            # Data Lake (partitioned)
│   └── processed/                      # Clean Parquet + ML CSV
│
├── docs/
│   └── DOCUMENTATION.md                # Full technical documentation
│
├── Makefile                            # 20 developer shortcuts
├── pytest.ini
├── requirements.txt
└── README.md
```

---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Orchestration** | Apache Airflow 2.8.1 | DAG scheduling, task dependencies |
| **Streaming** | Apache Kafka 7.5.3 | Real-time event ingestion |
| **Processing** | Apache Spark 3.5.1 | Distributed ETL & aggregation |
| **Data Warehouse** | PostgreSQL 15 | Star schema storage |
| **Data Lake** | Local FS / S3-compatible | Partitioned raw + processed data |
| **Dashboard** | Streamlit 1.31 | Interactive analytics UI |
| **Monitoring** | Prometheus + Grafana | Metrics, alerts |
| **Containerization** | Docker + Compose | Reproducible deployment |
| **Language** | Python 3.10 | All pipeline code |
| **Testing** | pytest + PySpark | 32+ test cases |

---

## 📊 Dashboard

The Streamlit dashboard at **http://localhost:8501** provides:

- **KPI Cards** — Total revenue, units sold, orders, avg order value
- **Demand Trend** — Daily chart with 7-day and 30-day moving averages
- **Category Breakdown** — Revenue by product category
- **Regional Distribution** — Demand pie chart by region
- **Day-of-Week Heatmap** — Demand patterns across months
- **Top Products Table** — Ranked by revenue with download
- **Demand Forecasting Page** — Moving Average + Linear Trend + Seasonal Naive ensemble with 80%/95% confidence bands
- **ML Dataset Export** — Download feature-engineered CSV directly

---

## 🗄️ Data Model

```
             dim_time
                │
dim_product ── fact_sales ── dim_store
                │
        agg_daily_demand
        agg_weekly_demand
       stream_ecommerce_events
```

**Grain:** `fact_sales` — one row per product per transaction  
**Partitioning:** Data Lake uses Hive-style `year=Y/month=M/day=D`  
**SCD:** `dim_product` uses Slowly Changing Dimension Type 2

---

## 🔬 Data Quality

Eight check types run automatically before each load:

```python
checker.check_null_rate("transaction_id", threshold=1.00)   # Zero nulls
checker.check_uniqueness("transaction_id", threshold=0.99)  # No duplicates
checker.check_range("quantity", min_val=1, max_val=10000)   # Valid range
checker.check_value_in_set("payment_method", valid_set)     # Known values
checker.check_date_format("transaction_date")               # Correct format
checker.check_completeness(["id", "product_id", "store_id"]) # All present
```

Results are logged to `warehouse.dq_check_log` and branch the DAG on failure.

---

## 🧪 Running Tests

```bash
# Unit tests only (no infrastructure needed)
pytest tests/test_quality_checks.py -v

# Spark transformation tests (needs Java)
pytest tests/test_transformations.py -v

# Integration tests (needs PostgreSQL running)
pytest tests/test_integration.py -v -m integration

# All tests with coverage
pytest tests/ --cov=. --cov-report=html
```

---

## 🤖 ML-Ready Dataset

After the pipeline runs, export the feature-engineered dataset:

```bash
python scripts/export_ml_dataset.py --format csv --output data/processed/ml/
```

**Features included:**

| Feature | Description |
|---------|-------------|
| `target_demand` | Daily units sold (prediction target) |
| `demand_lag_7d` | Units sold 7 days ago |
| `demand_lag_14d` | Units sold 14 days ago |
| `demand_lag_28d` | Units sold 28 days ago |
| `rolling_avg_7d` | 7-day moving average |
| `rolling_avg_28d` | 28-day moving average |
| `day_of_week` | 1=Monday … 7=Sunday |
| `is_holiday` | Public holiday flag |
| `temp_high_f` | Daily high temperature |
| `precipitation_in` | Daily precipitation |

---

## 🐳 Docker Services

```bash
# Start full stack
docker compose -f docker/docker-compose.yml up -d

# Start with monitoring (Prometheus + Grafana)
docker compose -f docker/docker-compose.yml \
               -f docker/docker-compose.monitoring.yml up -d

# View logs
docker compose -f docker/docker-compose.yml logs -f

# Stop everything
docker compose -f docker/docker-compose.yml down

# Full reset (removes volumes)
docker compose -f docker/docker-compose.yml down -v
```

---

## ⚡ Makefile Shortcuts

```bash
make quickstart    # Full setup from scratch
make up            # Start Docker services
make down          # Stop Docker services
make data          # Generate sample data
make db            # Initialize database schema
make producer      # Start Kafka producer
make consumer      # Start Kafka consumer
make dashboard     # Launch Streamlit
make test          # Run unit tests
make lint          # Lint all Python files
make health        # Run health checks
```

---

## 📈 Scaling Guide

| Bottleneck | Solution |
|-----------|----------|
| Kafka consumer lag | Add more consumer instances (same group_id) |
| PySpark slow | Add Spark workers via `docker compose scale spark-worker=3` |
| PostgreSQL slow | Add read replicas; increase partitioning granularity |
| Airflow slow | Switch to `CeleryExecutor` with Redis |
| Storage limits | Replace local Data Lake with AWS S3 |

---

## 📝 Documentation

Full technical documentation is available in [`docs/DOCUMENTATION.md`](docs/DOCUMENTATION.md), covering:

- Architecture decision records
- ETL step-by-step walkthrough
- Schema design rationale
- Data quality framework details
- Deployment guide (local + AWS)
- Troubleshooting reference

---

## 🙋 Resume Talking Points

> Use these when describing this project in interviews:

- *"Implemented Lambda Architecture supporting both batch (Airflow + PySpark) and streaming (Kafka) pipelines processing 50K+ daily transactions"*
- *"Designed a partitioned Star Schema in PostgreSQL with SCD Type 2 for slowly changing product dimensions"*
- *"Built a configurable data quality framework with 8 check types, structured result logging, and DAG branching on failure"*
- *"Engineered lag and rolling-window features creating an ML-ready demand forecasting dataset with 20 features per row"*
- *"Containerized the full stack (12 services) with Docker Compose for reproducible one-command deployment"*
- *"Implemented monitoring with Prometheus alert rules and structured JSON logging for production observability"*

---

## 📄 License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

---

<div align="center">

**Built with ❤️ for the Data Engineering community**

⭐ Star this repo if it helped you!

</div>
