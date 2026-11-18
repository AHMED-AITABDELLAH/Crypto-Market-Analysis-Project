"""Forward Binance BTCUSDT one-minute kline updates to Kafka unchanged."""

import asyncio
import logging
import os

import websockets
from confluent_kafka import Producer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@kline_1m"
KAFKA_TOPIC = "market.raw.spot.klines"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Idempotence lets Kafka safely retry a produce request without duplicating it.
producer = Producer(
    {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "enable.idempotence": True,
    }
)


def delivery_report(error, message):
    """Log delivery failures; keep successful per-message logs at debug level."""
    if error is not None:
        logger.error("Kafka delivery failed: %s", error)
    else:
        logger.debug(
            "Delivered to %s partition %s offset %s",
            message.topic(),
            message.partition(),
            message.offset(),
        )


async def publish_message(message):
    """Publish one original message without blocking the WebSocket event loop."""
    backpressured = False
    while True:
        try:
            producer.produce(
                topic=KAFKA_TOPIC,
                key="BTCUSDT",
                value=message,
                callback=delivery_report,
            )
            if backpressured:
                logger.info("Kafka producer queue has room again.")
            break
        except BufferError:
            # Poll callbacks and yield so WebSocket keepalive messages are serviced.
            if not backpressured:
                logger.warning("Kafka producer queue is full; applying backpressure.")
                backpressured = True
            producer.poll(0)
            await asyncio.sleep(0.1)

    # Service delivery callbacks without blocking the WebSocket loop.
    producer.poll(0)


async def main():
    """Connect to Binance, reconnect after interruptions, and publish updates."""
    retry_delay = 1

    try:
        while True:
            try:
                logger.info("Connecting to Binance WebSocket...")
                async with websockets.connect(
                    BINANCE_WS_URL,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                ) as websocket:
                    logger.info(
                        "Connected; forwarding %s to Kafka topic %s",
                        BINANCE_WS_URL,
                        KAFKA_TOPIC,
                    )
                    retry_delay = 1

                    async for message in websocket:
                        await publish_message(message)

                    # Treat a clean server-side close like any other interruption.
                    raise ConnectionError("Binance WebSocket closed the connection.")

            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "Binance connection failed; retrying in %s second(s).",
                    retry_delay,
                )
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 30)
    finally:
        # Give queued Kafka records a bounded time to finish during shutdown.
        remaining = producer.flush(10)
        if remaining:
            logger.warning("Shutdown with %s Kafka message(s) still pending.", remaining)


if __name__ == "__main__":
    asyncio.run(main())
