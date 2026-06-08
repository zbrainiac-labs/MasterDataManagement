"""Kafka consumer loop: single-message processing orchestration.

Polls one message at a time and delegates to the shared pipeline.
"""

import json
import logging
import os
from datetime import datetime, timezone

import psycopg
from confluent_kafka import Consumer, Producer, KafkaError

from nrt_mdm.mappers import TOPIC_MAPPER
from nrt_mdm.pipeline import process_event

logger = logging.getLogger(__name__)

# Map Kafka topic names to source_system keys used by pipeline
TOPIC_TO_SOURCE = {
    "topic.crm.a": "crm_a",
    "topic.crm.b": "crm_b",
    "topic.crm.c": "crm_c",
}


def _create_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "group.id": os.environ.get("KAFKA_GROUP_ID", "nrt-mdm-consumer"),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })


def _create_producer() -> Producer:
    return Producer({
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "acks": "all",
        "enable.idempotence": True,
        "retries": 3,
    })


def _get_pg_conn():
    dsn = os.environ.get("POSTGRES_DSN", "postgresql://mdm:mdm@localhost:5432/mdm")
    conn = psycopg.connect(dsn, autocommit=False)
    return conn


def process_message(msg, pg_conn, producer) -> None:
    """Process a single Kafka message through the shared pipeline."""
    topic = msg.topic()
    source_system = TOPIC_TO_SOURCE.get(topic)

    if source_system is None:
        logger.warning("No mapper for topic %s, skipping", topic)
        return

    # Parse payload
    try:
        payload = json.loads(msg.value())
    except (json.JSONDecodeError, TypeError) as e:
        logger.error("Malformed JSON on topic %s: %s", topic, e)
        return

    # Extract event_timestamp from Kafka message timestamp
    ts_type, ts_ms = msg.timestamp()
    event_ts = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)

    # Delegate to shared pipeline
    process_event(
        source_system=source_system,
        payload=payload,
        event_ts=event_ts,
        pg_conn=pg_conn,
        producer=producer,
    )


def run_consumer():
    """Main consumer loop. Polls one message at a time."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    consumer = _create_consumer()
    producer = _create_producer()
    pg_conn = _get_pg_conn()

    topics = list(TOPIC_MAPPER.keys())
    consumer.subscribe(topics)
    logger.info("Subscribed to topics: %s", topics)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("Consumer error: %s", msg.error())
                continue

            try:
                process_message(msg, pg_conn, producer)
                consumer.commit(message=msg)
            except Exception:
                logger.exception("Error processing message from %s", msg.topic())
                pg_conn.rollback()

    except KeyboardInterrupt:
        logger.info("Shutting down consumer")
    finally:
        consumer.close()
        pg_conn.close()


if __name__ == "__main__":
    run_consumer()
