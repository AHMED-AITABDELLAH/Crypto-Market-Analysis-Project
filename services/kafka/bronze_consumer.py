"""Persist raw Kafka market events and their Kafka coordinates in Bronze."""

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from confluent_kafka import Consumer, KafkaException
from psycopg.types.json import Jsonb


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "market.raw.spot.klines")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "bronze-market-ingestors")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DATABASE_URL = os.getenv("DATABASE_URL")
SQL_FILE = Path(
    os.getenv("BRONZE_INSERT_SQL_PATH", "/app/sql/insert_raw_market_event.sql")
)


def event_time_from_payload(event):
    """Convert Binance's millisecond event timestamp to a UTC datetime."""
    timestamp_ms = event.get("E")
    if timestamp_ms is None:
        return None
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)


def build_event_parameters(message, event):
    """Add common source and Kafka metadata without changing the JSON payload."""
    symbol = event.get("s")
    kline = event.get("k")
    if symbol is None and isinstance(kline, dict):
        symbol = kline.get("s")

    return {
        "event_type": event.get("e", "unknown"),
        "symbol": symbol,
        "source": "binance_spot",
        "event_time": event_time_from_payload(event),
        "ingestion_time": datetime.now(timezone.utc),
        "payload": Jsonb(event),
        "kafka_topic": message.topic(),
        "kafka_partition": message.partition(),
        "kafka_offset": message.offset(),
    }


def connect_to_database_with_retry(database_url):
    """Wait for PostgreSQL during startup; runtime write failures stop the service."""
    while True:
        try:
            return psycopg.connect(database_url)
        except psycopg.OperationalError as error:
            logger.warning("PostgreSQL is not ready (%s); retrying in 5 seconds.", error)
            time.sleep(5)


def store_event(database, insert_statement, message, event):
    """Insert one raw event and commit its database transaction."""
    with database.cursor() as cursor:
        cursor.execute(insert_statement, build_event_parameters(message, event))
    database.commit()


def main():
    """Consume continuously; acknowledge Kafka only after the Bronze insert."""
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL must contain the PostgreSQL connection URL.")
    if not SQL_FILE.is_file():
        raise FileNotFoundError(f"Bronze insert SQL file not found: {SQL_FILE}")

    # Load the query from database/; keep SQL out of this Python service.
    insert_statement = SQL_FILE.read_text(encoding="utf-8")
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": "earliest",
            # Manual commits prevent acknowledging an event before its DB write.
            "enable.auto.commit": False,
        }
    )
    database = None

    try:
        database = connect_to_database_with_retry(DATABASE_URL)
        consumer.subscribe([KAFKA_TOPIC])
        logger.info(
            "Connected to PostgreSQL; consuming raw events from %s.", KAFKA_TOPIC
        )

        stored_count = 0
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())

            raw_value = message.value()
            if not raw_value:
                raise ValueError(
                    "Received an empty Kafka message; leaving it uncommitted."
                )

            # Binance sends JSON objects; retain the decoded object unchanged in JSONB.
            event = json.loads(raw_value.decode("utf-8"))
            if not isinstance(event, dict):
                raise ValueError(
                    "Kafka message must be a JSON object; leaving it uncommitted."
                )

            # A database error exits the process so Compose restarts from the last
            # committed offset instead of continuing past an unstored message.
            store_event(database, insert_statement, message, event)
            consumer.commit(message=message, asynchronous=False)
            stored_count += 1
            if stored_count % 100 == 0:
                logger.info(
                    "Stored %s raw events; latest offset %s in partition %s.",
                    stored_count,
                    message.offset(),
                    message.partition(),
                )

    except KeyboardInterrupt:
        logger.info("Bronze consumer stopped by user.")
    finally:
        if database is not None:
            database.close()
        consumer.close()
        logger.info("Kafka consumer closed.")


if __name__ == "__main__":
    main()
