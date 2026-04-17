"""
ecommerce_producer.py
---------------------
Simulates a real-time e-commerce event stream by reading from the sample
JSON file and publishing each event to a Kafka topic.

Usage:
    python kafka/producer/ecommerce_producer.py [--rate 10]

Arguments:
    --rate   Events per second to publish (default: 10)
    --topic  Kafka topic name (default: ecommerce_events)
    --reset  Start from beginning of events file (default: random)
"""

import json
import time
import random
import argparse
import logging
import os
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("ecommerce_producer")

# ─── CONFIG ───────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DEFAULT_TOPIC   = "ecommerce_events"
SAMPLE_DATA     = os.path.join(
    os.path.dirname(__file__), "..", "..", "data", "sample", "ecommerce_events.json"
)


# ─── SERIALIZERS ──────────────────────────────────────────────────────────────

def json_serializer(data: dict) -> bytes:
    """Serialize a Python dict to JSON bytes for Kafka."""
    return json.dumps(data).encode("utf-8")


def key_serializer(key: str) -> bytes:
    """Serialize the message key (product_id for partitioning)."""
    return key.encode("utf-8") if key else b""


# ─── PRODUCER ─────────────────────────────────────────────────────────────────

class EcommerceProducer:
    """
    Publishes e-commerce events to Kafka.
    Messages are keyed by product_id to ensure all events for the same
    product land on the same partition (enables ordered processing).
    """

    def __init__(self, bootstrap_servers: str, topic: str):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=json_serializer,
            key_serializer=key_serializer,
            # Ensure events are not lost on producer side
            acks="all",
            retries=3,
            retry_backoff_ms=300,
            # Micro-batch for efficiency (wait up to 5ms to batch small messages)
            linger_ms=5,
            batch_size=16_384,
            # Compress with snappy for throughput
            compression_type="gzip",
        )
        log.info(f"Producer connected to {bootstrap_servers}, topic={topic}")

    def enrich_event(self, event: dict) -> dict:
        """
        Enrich each event with real-time metadata before publishing.
        In production: add user session data, A/B test group, etc.
        """
        event["produced_at"] = datetime.utcnow().isoformat() + "Z"
        event["pipeline_version"] = "1.0"
        # Simulate occasional anomalies for DQ testing (~0.5%)
        if random.random() < 0.005:
            event["price"] = -999          # bad price
        return event

    def delivery_report(self, err, msg):
        """Callback called once per message after delivery."""
        if err:
            log.error(f"Delivery failed for key={msg.key()}: {err}")
        else:
            log.debug(
                f"Delivered event to {msg.topic()} "
                f"partition={msg.partition()} offset={msg.offset()}"
            )

    def publish(self, event: dict):
        """Publish a single event to Kafka."""
        enriched = self.enrich_event(event)
        key = enriched.get("product_id", "unknown")
        self.producer.send(
            self.topic,
            key=key,
            value=enriched,
        ).add_errback(lambda e: log.error(f"Send error: {e}"))

    def flush(self):
        self.producer.flush()

    def close(self):
        self.producer.flush()
        self.producer.close()
        log.info("Producer closed.")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def load_events(path: str) -> list:
    """Load sample events from JSON file."""
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Sample data not found: {path}\n"
            "Run: python scripts/generate_sample_data.py"
        )
    with open(path) as f:
        events = json.load(f)
    log.info(f"Loaded {len(events):,} events from {path}")
    return events


def run(topic: str, rate: float, loop: bool = True):
    """
    Main publish loop.
    rate: target events/second
    loop: if True, loop through events indefinitely
    """
    producer = EcommerceProducer(KAFKA_BOOTSTRAP, topic)
    events   = load_events(SAMPLE_DATA)
    interval = 1.0 / rate
    total_published = 0

    log.info(f"Starting stream at {rate} events/sec → topic: {topic}")
    try:
        while True:
            for event in events:
                producer.publish(event)
                total_published += 1

                if total_published % 100 == 0:
                    log.info(f"Published {total_published:,} events")

                time.sleep(interval)

            if not loop:
                break

            log.info("Reached end of events file — restarting loop")
            random.shuffle(events)          # Shuffle on each cycle for variety

    except KeyboardInterrupt:
        log.info(f"Shutting down. Total published: {total_published:,}")
    finally:
        producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="E-commerce Kafka Producer")
    parser.add_argument("--rate",  type=float, default=10.0,
                        help="Events per second (default: 10)")
    parser.add_argument("--topic", type=str,   default=DEFAULT_TOPIC,
                        help="Kafka topic name")
    parser.add_argument("--no-loop", action="store_true",
                        help="Send events once without looping")
    args = parser.parse_args()

    run(topic=args.topic, rate=args.rate, loop=not args.no_loop)
