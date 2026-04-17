"""
init_kafka_topics.py
---------------------
Creates required Kafka topics with correct partition counts
and retention settings. Run once before starting the producer/consumer.

Usage:
    python scripts/init_kafka_topics.py
    python scripts/init_kafka_topics.py --bootstrap localhost:29092
"""

import argparse
import logging
import os

log = logging.getLogger("init_kafka")
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

TOPICS = [
    {
        "name":               "ecommerce_events",
        "num_partitions":     6,        # 6 = allows 6 parallel consumers
        "replication_factor": 1,        # 1 for dev; use 3 in prod
        "config": {
            "retention.ms":         str(24 * 60 * 60 * 1000),   # 24 hours
            "compression.type":     "gzip",
            "min.insync.replicas":  "1",
        },
    },
    {
        "name":               "ecommerce_events_dlq",
        "num_partitions":     2,
        "replication_factor": 1,
        "config": {
            "retention.ms":  str(7 * 24 * 60 * 60 * 1000),      # 7 days for DLQ
        },
    },
    {
        "name":               "pipeline_alerts",
        "num_partitions":     1,
        "replication_factor": 1,
        "config": {
            "retention.ms":  str(3 * 24 * 60 * 60 * 1000),
        },
    },
]


def create_topics(bootstrap_servers: str):
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    admin = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id="retail-pipeline-admin",
        request_timeout_ms=15_000,
    )

    existing = set(admin.list_topics())
    log.info(f"Existing topics: {existing}")

    new_topics = []
    for t in TOPICS:
        if t["name"] in existing:
            log.info(f"Topic already exists — skipping: {t['name']}")
            continue
        new_topics.append(NewTopic(
            name=t["name"],
            num_partitions=t["num_partitions"],
            replication_factor=t["replication_factor"],
            topic_configs=t["config"],
        ))

    if new_topics:
        try:
            admin.create_topics(new_topics, validate_only=False)
            for t in new_topics:
                log.info(f"✅ Created topic: {t.name} "
                         f"(partitions={t.num_partitions})")
        except TopicAlreadyExistsError:
            log.info("Topics already exist")
    else:
        log.info("All topics already exist — nothing to create")

    admin.close()
    log.info("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--bootstrap",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap server address",
    )
    args = parser.parse_args()
    create_topics(args.bootstrap)
