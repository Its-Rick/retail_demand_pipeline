"""
ecommerce_consumer.py
---------------------
Consumes e-commerce events from Kafka, performs lightweight validation
and transformation, then writes to:
  1. PostgreSQL  → stream_ecommerce_events (near real-time query)
  2. Data Lake   → data/raw/ecommerce/  (micro-batch JSON files, hourly)

The consumer uses a micro-batch pattern: events are buffered in memory
and flushed every FLUSH_INTERVAL_SEC seconds or BATCH_SIZE events.

Usage:
    python kafka/consumer/ecommerce_consumer.py
"""

import os
import json
import time
import signal
import logging
from datetime import datetime
from collections import defaultdict
from typing import List, Dict

from kafka import KafkaConsumer
from kafka.errors import KafkaError
import psycopg2
from psycopg2.extras import execute_batch

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
log = logging.getLogger("ecommerce_consumer")

# ─── CONFIG ───────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP    = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC        = os.getenv("KAFKA_TOPIC",              "ecommerce_events")
KAFKA_GROUP_ID     = os.getenv("KAFKA_GROUP_ID",           "retail-demand-pipeline")
DATA_LAKE_BASE     = os.getenv("DATA_LAKE_PATH",           "./data")
DB_DSN             = os.getenv("DATABASE_URL",
                                "postgresql://airflow:airflow@localhost:5432/retail_dw")

BATCH_SIZE         = int(os.getenv("CONSUMER_BATCH_SIZE", "500"))
FLUSH_INTERVAL_SEC = int(os.getenv("CONSUMER_FLUSH_SEC",  "30"))
POLL_TIMEOUT_MS    = 1_000


# ─── VALIDATION ───────────────────────────────────────────────────────────────

VALID_EVENT_TYPES = {
    "product_view", "add_to_cart", "remove_from_cart", "purchase", "wishlist_add"
}

def validate_event(event: dict) -> bool:
    """
    Lightweight validation before inserting to DW.
    Returns True if event is valid, False to drop/quarantine.
    """
    required_keys = ["event_id", "event_type", "event_timestamp", "product_id"]
    for key in required_keys:
        if not event.get(key):
            log.warning(f"Dropping event — missing field '{key}': {event.get('event_id','?')}")
            return False
    if event["event_type"] not in VALID_EVENT_TYPES:
        log.warning(f"Dropping event — unknown type: {event['event_type']}")
        return False
    if event.get("price") and float(event["price"]) < 0:
        log.warning(f"Dropping event — negative price: {event['event_id']}")
        return False
    return True


def transform_event(event: dict) -> dict:
    """
    Lightweight transformation for stream events.
    Normalizes fields for DB insertion.
    """
    return {
        "event_id":        event.get("event_id"),
        "event_type":      event.get("event_type"),
        "event_timestamp": event.get("event_timestamp", "").replace("Z",""),
        "session_id":      event.get("session_id"),
        "user_id":         event.get("user_id"),
        "product_id":      event.get("product_id"),
        "category":        event.get("category"),
        "price":           event.get("price"),
        "quantity":        event.get("quantity"),
        "device":          event.get("device"),
        "platform":        event.get("platform"),
        "referrer":        event.get("referrer"),
        "state":           (event.get("geo") or {}).get("state"),
    }


# ─── SINK: POSTGRESQL ─────────────────────────────────────────────────────────

class PostgresSink:
    """Batch-inserts transformed events into PostgreSQL."""

    INSERT_SQL = """
        INSERT INTO warehouse.stream_ecommerce_events (
            event_id, event_type, event_timestamp, session_id, user_id,
            product_id, category, price, quantity, device, platform, referrer, state
        ) VALUES (
            %(event_id)s, %(event_type)s, %(event_timestamp)s, %(session_id)s,
            %(user_id)s, %(product_id)s, %(category)s, %(price)s, %(quantity)s,
            %(device)s, %(platform)s, %(referrer)s, %(state)s
        )
        ON CONFLICT (event_id) DO NOTHING
    """

    def __init__(self, dsn: str):
        self.conn = psycopg2.connect(dsn)
        self.conn.autocommit = False
        log.info("PostgresSink: Connected to database")

    def flush(self, records: List[dict]) -> int:
        """Bulk-insert records. Returns count inserted."""
        if not records:
            return 0
        try:
            with self.conn.cursor() as cur:
                execute_batch(cur, self.INSERT_SQL, records, page_size=200)
            self.conn.commit()
            return len(records)
        except Exception as e:
            self.conn.rollback()
            log.error(f"PostgresSink flush error: {e}")
            return 0

    def close(self):
        self.conn.close()


# ─── SINK: DATA LAKE ──────────────────────────────────────────────────────────

class DataLakeSink:
    """
    Writes micro-batches as hourly-partitioned JSONL files.
    Path: data/raw/ecommerce/year=YYYY/month=MM/day=DD/hour=HH/batch_*.jsonl
    """

    def __init__(self, base_path: str):
        self.base = base_path

    def flush(self, records: List[dict]) -> str:
        """Write a batch to disk. Returns path written."""
        if not records:
            return ""
        now = datetime.utcnow()
        out_dir = os.path.join(
            self.base, "raw", "ecommerce",
            f"year={now.year}", f"month={now.month:02d}",
            f"day={now.day:02d}", f"hour={now.hour:02d}"
        )
        os.makedirs(out_dir, exist_ok=True)
        fname = f"batch_{int(time.time()*1000)}.jsonl"
        fpath = os.path.join(out_dir, fname)
        with open(fpath, "w") as f:
            for rec in records:
                f.write(json.dumps(rec) + "\n")
        return fpath


# ─── CONSUMER ─────────────────────────────────────────────────────────────────

class EcommerceConsumer:

    def __init__(self):
        self.consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=KAFKA_GROUP_ID,
            auto_offset_reset="earliest",          # start from beginning if no offset
            enable_auto_commit=False,              # manual commit after DB flush
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            session_timeout_ms=30_000,
            heartbeat_interval_ms=10_000,
        )
        self.pg_sink   = PostgresSink(DB_DSN)
        self.lake_sink = DataLakeSink(DATA_LAKE_BASE)
        self.buffer: List[dict] = []
        self.stats = defaultdict(int)
        self.last_flush = time.time()
        self.running = True

        # Handle graceful shutdown
        signal.signal(signal.SIGINT,  self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)
        log.info(f"Consumer started | topic={KAFKA_TOPIC} | group={KAFKA_GROUP_ID}")

    def _shutdown(self, signum, frame):
        log.info("Shutdown signal received — flushing buffer...")
        self._flush()
        self.running = False

    def _flush(self):
        if not self.buffer:
            return
        n_inserted = self.pg_sink.flush(self.buffer)
        lake_path  = self.lake_sink.flush(self.buffer)
        self.consumer.commit()                     # commit only after successful sink

        self.stats["total_flushed"] += n_inserted
        log.info(
            f"Flushed {len(self.buffer)} events | "
            f"DB inserts: {n_inserted} | Lake: {lake_path or 'skip'}"
        )
        self.buffer.clear()
        self.last_flush = time.time()

    def run(self):
        """Main consume loop."""
        while self.running:
            records = self.consumer.poll(timeout_ms=POLL_TIMEOUT_MS)

            for topic_partition, messages in records.items():
                for msg in messages:
                    event = msg.value
                    self.stats["total_consumed"] += 1

                    if not validate_event(event):
                        self.stats["dropped"] += 1
                        continue

                    transformed = transform_event(event)
                    self.buffer.append(transformed)
                    self.stats["valid"] += 1

            # Flush when batch is full OR flush interval elapsed
            time_since_flush = time.time() - self.last_flush
            if len(self.buffer) >= BATCH_SIZE or time_since_flush >= FLUSH_INTERVAL_SEC:
                self._flush()

            # Periodic stats log
            if self.stats["total_consumed"] % 1000 == 0 and self.stats["total_consumed"] > 0:
                log.info(
                    f"Stats | consumed={self.stats['total_consumed']:,} "
                    f"valid={self.stats['valid']:,} "
                    f"dropped={self.stats['dropped']:,} "
                    f"flushed={self.stats['total_flushed']:,}"
                )

        # Final flush on exit
        self._flush()
        self.consumer.close()
        self.pg_sink.close()
        log.info(f"Consumer stopped. Final stats: {dict(self.stats)}")


# ─── ENTRY POINT ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    consumer = EcommerceConsumer()
    consumer.run()
