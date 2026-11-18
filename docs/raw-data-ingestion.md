# Raw market data ingestion

## Current flow

`services/binance/websocket_client.py` connects to Binance's BTCUSDT 1-minute
kline stream and publishes each original message to Kafka topic
`market.raw.spot.klines`. It reconnects with a capped backoff after connection
failures and keeps successful per-message delivery logs at debug level to avoid
filling normal logs with repetitive entries.

`services/kafka/bronze_consumer.py` consumes that topic and stores each original
JSON event plus shared metadata in PostgreSQL Bronze. It loads its parameterized
insert statement from `database/insert_raw_market_event.sql`; the Python file
contains no SQL statement. This stage does not normalize candle fields or write
to Silver or Gold.

## Bronze destination

On the first initialization of an empty PostgreSQL data volume, Compose mounts
`database/001_initialize_schemas_and_bronze_table.sql`. It creates the `bronze`,
`silver`, and `gold` schemas and the Bronze raw-events table. It does not create
Silver or Gold tables yet; this phase stores raw events only.

`bronze.raw_market_events` contains these event and traceability columns:

- `event_type`
- `symbol`
- `source`
- `event_time`
- `ingestion_time`
- `payload` (JSONB)
- `kafka_topic`
- `kafka_partition`
- `kafka_offset`

The table has a unique constraint on `(kafka_topic, kafka_partition,
kafka_offset)`. The insert statement in `database/insert_raw_market_event.sql`
uses that key to ignore a repeated event if the database write succeeded but
Kafka did not record the offset commit. PostgreSQL runs initialization scripts
only when its data volume is first created. If the volume already exists, the
script must be applied manually for that database.

The consumer stores each Kafka message in a database transaction, then commits
that message's Kafka offset. If a database write fails after startup, the
consumer exits so Compose can restart it from the last committed offset. The
unique Kafka coordinates prevent the already-written event from becoming a
duplicate during that retry.

## Configuration and startup

In Compose, PostgreSQL receives `POSTGRES_DB`, `POSTGRES_USER`, and
`POSTGRES_PASSWORD` from `.env`. Compose builds the consumer's `DATABASE_URL`
from those same values and connects it to the `postgres` service. Keep the local
password URL-safe (letters, numbers, and underscores) so it works in the
connection URL without encoding. For a direct Python run, set `DATABASE_URL`
yourself. The consumer also accepts `KAFKA_BOOTSTRAP_SERVERS` and `KAFKA_TOPIC`;
their defaults are `localhost:9092` and `market.raw.spot.klines`. Compose uses
`kafka:29092` for the broker.

Compose starts Kafka and PostgreSQL first and waits for both health checks.
The Binance producer depends on both services being healthy. The optional
`kafka-bronze-consumer` service is enabled by the `database-ingestion` profile;
it waits for Kafka and PostgreSQL to be healthy and for the producer container
to start. Copy `.env.example` to `.env` if needed and change the local
development password before starting the stack:

```powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
docker compose --profile database-ingestion up --build
```

The profile keeps the consumer out of the normal Kafka and WebSocket startup.
Compose's `service_started` condition confirms that the producer container
started; it does not verify that Binance has connected or published its first event.
