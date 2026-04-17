"""
monitoring.py
-------------
Centralised logging, metrics, and monitoring utilities for the pipeline.

Features:
  - Structured JSON logging (compatible with Datadog, CloudWatch, ELK)
  - Pipeline run metrics tracker (duration, row counts, error rates)
  - Simple Prometheus-compatible metrics export
  - Health check endpoint helper

Usage:
    from monitoring.monitoring import get_logger, PipelineMetrics

    log = get_logger("pos_transformer")
    metrics = PipelineMetrics("pos_etl")
    metrics.record("rows_read", 50000)
    metrics.finish()
"""

import os
import json
import time
import logging
import functools
from datetime import datetime
from typing import Any, Dict, Optional
from dataclasses import dataclass, field, asdict


# ─── STRUCTURED JSON LOGGER ───────────────────────────────────────────────────

class JsonFormatter(logging.Formatter):
    """Formats log records as single-line JSON for log aggregation systems."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level":     record.levelname,
            "logger":    record.name,
            "message":   record.getMessage(),
            "module":    record.module,
            "line":      record.lineno,
        }
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_entry)


def get_logger(name: str, level: str = None) -> logging.Logger:
    """
    Returns a logger with JSON formatting when LOG_FORMAT=json,
    otherwise falls back to human-readable format for local dev.
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # already configured

    log_level = getattr(logging, (level or os.getenv("LOG_LEVEL", "INFO")).upper())
    logger.setLevel(log_level)

    handler = logging.StreamHandler()
    if os.getenv("LOG_FORMAT", "text") == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        ))

    logger.addHandler(handler)
    logger.propagate = False
    return logger


# ─── PIPELINE METRICS TRACKER ─────────────────────────────────────────────────

@dataclass
class PipelineMetrics:
    """
    Tracks key metrics for a single pipeline run.
    Call record() to add metrics, finish() to compute duration and log summary.

    Persists metrics to logs/metrics.jsonl for external monitoring ingestion.
    """
    pipeline_name: str
    run_id:        str = field(default_factory=lambda: datetime.utcnow().strftime("%Y%m%d_%H%M%S"))
    start_time:    float = field(default_factory=time.time)
    metrics:       Dict[str, Any] = field(default_factory=dict)
    errors:        list = field(default_factory=list)
    status:        str = "running"

    def record(self, key: str, value: Any) -> None:
        """Record a named metric value."""
        self.metrics[key] = value

    def increment(self, key: str, by: int = 1) -> None:
        """Increment a counter metric."""
        self.metrics[key] = self.metrics.get(key, 0) + by

    def error(self, msg: str) -> None:
        """Log an error event for this run."""
        self.errors.append({"time": datetime.utcnow().isoformat(), "msg": msg})

    def finish(self, status: str = "success") -> Dict[str, Any]:
        """Finalise metrics, log summary, write to metrics.jsonl."""
        self.status = status
        elapsed = round(time.time() - self.start_time, 2)
        self.metrics["elapsed_sec"] = elapsed

        summary = {
            "pipeline":    self.pipeline_name,
            "run_id":      self.run_id,
            "status":      self.status,
            "elapsed_sec": elapsed,
            "metrics":     self.metrics,
            "errors":      self.errors,
            "finished_at": datetime.utcnow().isoformat() + "Z",
        }

        log = get_logger(f"metrics.{self.pipeline_name}")
        log.info(f"Pipeline finished | {self.pipeline_name} | "
                 f"status={status} | elapsed={elapsed}s | "
                 f"metrics={json.dumps(self.metrics)}")

        # Append to metrics log file
        metrics_dir = os.path.join(os.path.dirname(__file__), "..", "logs")
        os.makedirs(metrics_dir, exist_ok=True)
        metrics_file = os.path.join(metrics_dir, "pipeline_metrics.jsonl")
        with open(metrics_file, "a") as f:
            f.write(json.dumps(summary) + "\n")

        return summary


# ─── TIMING DECORATOR ─────────────────────────────────────────────────────────

def timed(func=None, *, logger_name: str = None):
    """
    Decorator that logs function execution time.

    Usage:
        @timed
        def my_function(): ...

        @timed(logger_name="pos_etl")
        def my_function(): ...
    """
    def decorator(f):
        log = get_logger(logger_name or f.__module__)

        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            start = time.time()
            log.info(f"START: {f.__name__}")
            try:
                result = f(*args, **kwargs)
                elapsed = round(time.time() - start, 2)
                log.info(f"END:   {f.__name__} | {elapsed}s")
                return result
            except Exception as e:
                elapsed = round(time.time() - start, 2)
                log.error(f"FAIL:  {f.__name__} | {elapsed}s | {e}")
                raise
        return wrapper

    if func is not None:
        return decorator(func)
    return decorator


# ─── HEALTH CHECK ─────────────────────────────────────────────────────────────

def check_postgres_health(dsn: str) -> Dict[str, Any]:
    """Check PostgreSQL connectivity and return status dict."""
    import psycopg2
    start = time.time()
    try:
        conn = psycopg2.connect(dsn, connect_timeout=5)
        conn.close()
        return {"service": "postgres", "status": "healthy",
                "latency_ms": round((time.time() - start) * 1000, 1)}
    except Exception as e:
        return {"service": "postgres", "status": "unhealthy", "error": str(e)}


def check_kafka_health(bootstrap_servers: str) -> Dict[str, Any]:
    """Check Kafka broker connectivity."""
    from kafka import KafkaAdminClient
    start = time.time()
    try:
        admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers,
                                 request_timeout_ms=5000)
        topics = admin.list_topics()
        admin.close()
        return {"service": "kafka", "status": "healthy",
                "topics": len(topics),
                "latency_ms": round((time.time() - start) * 1000, 1)}
    except Exception as e:
        return {"service": "kafka", "status": "unhealthy", "error": str(e)}


def run_health_checks() -> Dict[str, Any]:
    """Run all health checks and return aggregated status."""
    from config.config import config
    results = {
        "postgres": check_postgres_health(config.db.dsn),
        "kafka":    check_kafka_health(config.kafka.bootstrap_servers),
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }
    all_healthy = all(v.get("status") == "healthy" for v in results.values()
                      if isinstance(v, dict))
    results["overall"] = "healthy" if all_healthy else "degraded"
    return results


# ─── PROMETHEUS METRICS EXPORT ────────────────────────────────────────────────

class PrometheusExporter:
    """
    Simple Prometheus text-format metrics writer.
    Writes to logs/metrics.prom — scrape with node_exporter textfile collector
    or push to Pushgateway.

    Example output:
        retail_pipeline_rows_processed{pipeline="pos_etl"} 50000
        retail_pipeline_duration_seconds{pipeline="pos_etl"} 42.3
    """

    METRICS_PATH = os.path.join(os.path.dirname(__file__), "..", "logs", "metrics.prom")

    def write(self, metrics: PipelineMetrics) -> None:
        lines = [f"# Retail Pipeline Metrics — {datetime.utcnow().isoformat()}Z"]
        pipeline = metrics.pipeline_name

        for key, value in metrics.metrics.items():
            if isinstance(value, (int, float)):
                metric_name = f"retail_pipeline_{key}"
                lines.append(f'{metric_name}{{pipeline="{pipeline}"}} {value}')

        lines.append(
            f'retail_pipeline_status{{pipeline="{pipeline}"}} '
            f'{"1" if metrics.status == "success" else "0"}'
        )

        os.makedirs(os.path.dirname(self.METRICS_PATH), exist_ok=True)
        with open(self.METRICS_PATH, "w") as f:
            f.write("\n".join(lines) + "\n")


# ─── QUICK TEST ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    log = get_logger("monitoring_test")
    log.info("Testing structured logger")
    log.warning("This is a warning")

    metrics = PipelineMetrics("test_pipeline")
    metrics.record("rows_read", 50_000)
    metrics.record("rows_written", 49_200)
    metrics.increment("errors_dropped", 800)
    time.sleep(0.1)
    summary = metrics.finish("success")
    print(json.dumps(summary, indent=2))

    exporter = PrometheusExporter()
    exporter.write(metrics)
    print(f"Prometheus metrics written to: {exporter.METRICS_PATH}")
