# Raw market data ingestion

## Current flow

`services/binance/websocket_client.py` connects to Binance's BTCUSDT 1-minute
kline stream and publishes each original message to Kafka topic
`market.raw.spot.klines`. It reconnects after connection failures and keeps
successful per-message delivery logs at debug level.

Airflow's Kafka Triggerer will watch that topic using an `AssetWatcher`. Its
message filter accepts only final kline updates (`k.x == true`) and forwards the
original JSON with Kafka topic, partition, and offset. The three callable tasks
under `scripts/realtime_kline_ingestion/` validate the candle,
store the unchanged event in Bronze, then transform it into `silver.candles`.
The DAG itself has not been added yet.

Task scripts are grouped by DAG purpose. The current package is
`realtime_kline_ingestion`; a future scheduled historical workflow can have a
separate package such as `historical_kline_ingestion`. Compose mounts the root
`scripts/` directory at `/opt/airflow/scripts`, so Airflow can import each
purpose-specific package while the scripts stay outside the Airflow folder.
The filter that runs before the DAG is `step_00_filter_closed_kline_event.py`.
The tasks are prefixed `task_01`, `task_02`, and `task_03` to show validation,
Bronze storage, and Silver ETL order. `database.py` is a shared helper for this
package.

## Ordered SQL files

There is one database initializer: `database/001_initialize_database.sql`. It
creates the `bronze`, `silver`, `gold`, and `airflow_metadata` schemas and the
Bronze raw-events table. Add future schema and table initialization statements
to this same file, in the order they should be applied. The first Silver table,
`silver.candles`, is currently described there as a TODO for the database
developer.

The numbered task SQL files follow the processing order:

1. `database/002_insert_raw_market_event.sql` — insert the unchanged event in
   Bronze and return its row ID, including when the task is retried.
2. `database/003_select_bronze_events_by_ids.sql` — retrieve the Bronze events
   returned by step 1 for ETL.
3. `database/004_upsert_silver_candle.sql` — store the normalized candle in
   Silver using `(symbol, interval, open_time)` as its idempotency key.

The three task SQL files currently contain comments only for the database
developer. Python task code contains no SQL queries. Bronze and Silver task
writes will not work until those SQL statements and the Silver table are
implemented.

`bronze.raw_market_events` stores event type, symbol, source, event and
ingestion times, original JSONB payload, and Kafka topic/partition/offset. The
initialization script is safe to rerun because schema, table, and index creation
uses `IF NOT EXISTS`. PostgreSQL only runs mounted initialization scripts when
it first creates an empty data volume. For an existing database, apply it
manually without deleting the volume:

```powershell
docker compose up -d postgres
docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /docker-entrypoint-initdb.d/001_initialize_database.sql'
```

The database developer should add the `silver.candles` table definition to
`001_initialize_database.sql`. It needs `symbol`, `interval`, `open_time`,
`close_time`, OHLC price columns, `volume`, and `is_closed`; use `TIMESTAMPTZ`
for the candle times, exact numeric types for prices and volume, and a unique
or primary key on `(symbol, interval, open_time)`.

## Compose startup

Copy `.env.example` to `.env` if needed and set a local PostgreSQL password
using URL-safe characters (letters, numbers, and underscores):

```powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
docker compose up --build
```

This starts Kafka, PostgreSQL, and the Binance WebSocket producer. It does not
write events to Bronze until an event-driven DAG is added and enabled.

The optional `airflow` profile adds Airflow 3.4.0 with Kafka provider 3.0.0. It
includes the API server, scheduler, DAG processor, and Triggerer. `LocalExecutor`
keeps this local setup free of Redis and a separate Celery worker. Airflow
metadata is stored in `airflow_metadata`; project task queries use `bronze` and
`silver`.

Start Airflow alongside the producer with:

```powershell
docker compose --profile airflow up --build
```

Open the Airflow UI at `http://localhost:8080`. The Simple Auth Manager creates
a development password; find it in the API server logs:

```powershell
docker compose logs airflow-api-server
```

The DAG is intentionally not included yet. When added, it should use
`KafkaMessageQueueTrigger` with callback
`scripts.realtime_kline_ingestion.step_00_filter_closed_kline_event.capture_closed_kline`,
then chain the three task functions. The trigger consumes Kafka; the tasks
process the event payload Airflow provides. The standalone Bronze consumer is
no longer part of this workflow.
