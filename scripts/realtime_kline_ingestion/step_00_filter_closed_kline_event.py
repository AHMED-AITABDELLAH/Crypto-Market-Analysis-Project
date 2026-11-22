"""Kafka trigger callback that forwards only closed Binance kline events."""

import json
import logging


logger = logging.getLogger(__name__)


def capture_closed_kline(message):
    """Return a JSON-safe event envelope for a closed kline, or ``None``.

    Airflow's Kafka trigger calls this function for each Kafka record. A truthy
    return value becomes the payload available to downstream DAG tasks.
    """
    raw_value = message.value()
    if not raw_value:
        logger.warning("Ignoring empty Kafka message.")
        return None

    try:
        event = json.loads(raw_value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        logger.warning("Ignoring non-JSON Kafka message at offset %s.", message.offset())
        return None

    if not isinstance(event, dict):
        return None

    kline = event.get("k")
    if event.get("e") != "kline" or not isinstance(kline, dict):
        return None

    # Binance sends repeated updates during a minute. Only x=True means the
    # one-minute candle is final and should start this workflow.
    if kline.get("x") is not True:
        return None

    return {
        "payload": event,
        "kafka_topic": message.topic(),
        "kafka_partition": message.partition(),
        "kafka_offset": message.offset(),
    }
