# =============================================================================
# Makefile
# Retail Demand Forecasting Pipeline — Developer Shortcuts
# =============================================================================

.PHONY: help setup data db up down logs test lint clean airflow-ui spark-ui dashboard

PYTHON := python3
DOCKER  := docker-compose -f docker/docker-compose.yml

help:		## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ── SETUP ─────────────────────────────────────────────────────────────────────

setup:		## Install Python dependencies
	pip install -r requirements.txt

data:		## Generate sample data (POS CSV, e-commerce JSON, weather, holidays)
	$(PYTHON) scripts/generate_sample_data.py

db:		## Initialise PostgreSQL schema (requires running postgres)
	$(PYTHON) scripts/setup_db.py

# ── DOCKER ─────────────────────────────────────────────────────────────────────

up:		## Start all services (Postgres, Kafka, Airflow, Spark, Streamlit)
	$(DOCKER) up -d
	@echo "Waiting for services to start..."
	@sleep 10
	@echo "✅  Services running:"
	@echo "    Airflow:   http://localhost:8080  (admin/admin)"
	@echo "    Spark UI:  http://localhost:8090"
	@echo "    Dashboard: http://localhost:8501"

down:		## Stop all services
	$(DOCKER) down

restart:	## Restart all services
	$(DOCKER) restart

logs:		## Tail logs for all services
	$(DOCKER) logs -f

logs-airflow:	## Tail Airflow scheduler logs only
	$(DOCKER) logs -f airflow-scheduler

logs-kafka:	## Tail Kafka broker logs
	$(DOCKER) logs -f retail_kafka

ps:		## Show service status
	$(DOCKER) ps

# ── PIPELINE RUNS ─────────────────────────────────────────────────────────────

producer:	## Start Kafka producer (foreground)
	$(PYTHON) kafka/producer/ecommerce_producer.py --rate 10

consumer:	## Start Kafka consumer (foreground)
	$(PYTHON) kafka/consumer/ecommerce_consumer.py

transform-pos:	## Run PySpark POS transformer for today
	spark-submit \
	  --master local[2] \
	  --packages org.postgresql:postgresql:42.7.1 \
	  spark/transformations/pos_transformer.py \
	  --execution-date $$(date +%Y-%m-%d) \
	  --data-lake ./data

transform-ecom:	## Run PySpark e-commerce transformer for today
	spark-submit \
	  --master local[2] \
	  spark/transformations/ecommerce_transformer.py \
	  --execution-date $$(date +%Y-%m-%d) \
	  --data-lake ./data

aggregate:	## Run demand aggregator (last 7 days)
	spark-submit \
	  --master local[2] \
	  --packages org.postgresql:postgresql:42.7.1 \
	  spark/transformations/demand_aggregator.py \
	  --lookback-days 7

dq-check:	## Run data quality checks on sample data
	spark-submit \
	  --master local[1] \
	  spark/quality/data_quality_checks.py

# ── TESTING ───────────────────────────────────────────────────────────────────

test:		## Run all unit tests (excluding Spark tests)
	pytest tests/test_quality_checks.py -v

test-all:	## Run full test suite including Spark tests (slow)
	pytest tests/ -v -m "not integration"

test-spark:	## Run Spark transformation tests only
	pytest tests/test_transformations.py -v -m slow

coverage:	## Run tests with coverage report
	pytest tests/ --cov=. --cov-report=term-missing --cov-report=html

# ── CODE QUALITY ──────────────────────────────────────────────────────────────

lint:		## Lint all Python files with flake8
	flake8 . --max-line-length=100 \
	  --exclude=.git,__pycache__,data,logs \
	  --ignore=E501,W503

format:		## Auto-format with black
	black . --line-length 100 --exclude "(data|logs|\.git)"

typecheck:	## Type check with mypy
	mypy config/ monitoring/ scripts/ --ignore-missing-imports

# ── UIs ───────────────────────────────────────────────────────────────────────

dashboard:	## Launch Streamlit dashboard locally
	streamlit run streamlit/dashboard.py

health:		## Run pipeline health checks
	$(PYTHON) -c "from monitoring.monitoring import run_health_checks; import json; print(json.dumps(run_health_checks(), indent=2))"

# ── CLEANUP ───────────────────────────────────────────────────────────────────

clean-data:	## Remove generated data files (keeps sample/)
	find data/raw data/processed -name "*.csv" -o -name "*.json" -o \
	  -name "*.parquet" -o -name "*.jsonl" | xargs rm -f 2>/dev/null || true

clean-logs:	## Remove log files
	rm -f logs/*.log logs/*.jsonl logs/*.prom

clean-pycache:	## Remove Python cache files
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true

clean:		## Full cleanup (data + logs + cache)
	make clean-data clean-logs clean-pycache

# ── QUICK START ───────────────────────────────────────────────────────────────

quickstart:	## Full setup from scratch: deps → data → docker up → db init
	@echo "🚀 Retail Demand Pipeline — Quick Start"
	make setup
	make data
	make up
	@sleep 15
	make db
	@echo ""
	@echo "✅  Pipeline ready!"
	@echo "    Open Airflow:   http://localhost:8080"
	@echo "    Open Dashboard: http://localhost:8501"
