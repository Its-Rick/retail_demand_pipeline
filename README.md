# 🛒 Retail Demand Forecasting Data Pipeline

A production-ready, end-to-end data engineering project implementing **Lambda Architecture** for retail demand forecasting.

---

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                  │
│   [POS CSV Files]    [E-commerce JSON]    [Weather/Holiday APIs]     │
└────────┬──────────────────┬──────────────────────┬───────────────────┘
         │                  │                       │
         ▼                  ▼                       ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     INGESTION LAYER                                  │
│   [Apache Airflow]              [Apache Kafka]                       │
│   (Batch Scheduler)             (Streaming Events)                   │
└────────┬──────────────────────────────┬────────────────────────────-┘
         │                              │
         ▼                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     STORAGE LAYER - DATA LAKE (S3/Local)             │
│   raw/pos/          raw/ecommerce/          raw/external/            │
│   (Partitioned by date)                                              │
└────────┬──────────────────────────────┬─────────────────────────────┘
         │                              │
         ▼ Batch (PySpark)              ▼ Stream (Kafka Consumer)
┌─────────────────────────────────────────────────────────────────────┐
│                     PROCESSING LAYER                                 │
│   [PySpark ETL]  →  Cleaning → Enrichment → Aggregation             │
│   Data Quality Checks | Deduplication | Schema Validation            │
└────────┬─────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────┐
│               DATA WAREHOUSE (PostgreSQL) - Star Schema              │
│   fact_sales  |  dim_product  |  dim_store  |  dim_time             │
│   agg_daily_demand  |  agg_weekly_demand                            │
└────────┬─────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     SERVING LAYER                                    │
│   [Streamlit Dashboard]       [ML-Ready Dataset Export]             │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 📁 Project Structure

```
retail_demand_pipeline/
├── airflow/
│   ├── dags/
│   │   ├── batch_ingestion_dag.py       # Main batch pipeline DAG
│   │   └── data_quality_dag.py          # Quality check DAG
│   └── plugins/
│       └── custom_operators.py          # Custom Airflow operators
├── kafka/
│   ├── producer/
│   │   └── ecommerce_producer.py        # Simulates e-commerce events
│   └── consumer/
│       └── ecommerce_consumer.py        # Consumes and stores events
├── spark/
│   ├── transformations/
│   │   ├── pos_transformer.py           # POS data ETL
│   │   ├── ecommerce_transformer.py     # E-commerce ETL
│   │   └── demand_aggregator.py         # Daily/weekly aggregations
│   └── quality/
│       └── data_quality_checks.py       # Great Expectations-style checks
├── sql/
│   ├── schema/
│   │   └── star_schema.sql              # All DDL statements
│   ├── queries/
│   │   └── analytical_queries.sql       # Business queries
│   └── aggregations/
│       └── demand_aggregations.sql      # Demand metrics SQL
├── streamlit/
│   └── dashboard.py                     # Analytics dashboard
├── docker/
│   ├── docker-compose.yml               # Full stack orchestration
│   ├── Dockerfile.airflow
│   └── Dockerfile.spark
├── scripts/
│   ├── generate_sample_data.py          # Generates synthetic data
│   └── setup_db.py                      # DB initialization
├── data/
│   ├── raw/                             # Raw data lake (partitioned)
│   ├── processed/                       # Processed/clean data
│   └── sample/                         # Sample input files
├── tests/
│   ├── test_transformations.py
│   └── test_quality_checks.py
├── logs/
├── requirements.txt
└── README.md
```

---

## 🚀 Quick Start

```bash
# 1. Clone and setup
git clone <repo>
cd retail_demand_pipeline
pip install -r requirements.txt

# 2. Generate sample data
python scripts/generate_sample_data.py

# 3. Start infrastructure
docker-compose -f docker/docker-compose.yml up -d

# 4. Initialize database
python scripts/setup_db.py

# 5. Start Kafka producer (new terminal)
python kafka/producer/ecommerce_producer.py

# 6. Start Kafka consumer (new terminal)
python kafka/consumer/ecommerce_consumer.py

# 7. Trigger Airflow DAG or run PySpark manually
python spark/transformations/pos_transformer.py

# 8. Launch dashboard
streamlit run streamlit/dashboard.py
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| Orchestration | Apache Airflow 2.x |
| Streaming | Apache Kafka |
| Processing | PySpark 3.x |
| Data Warehouse | PostgreSQL |
| Dashboard | Streamlit |
| Containerization | Docker & Docker Compose |
| Language | Python 3.10+ |
