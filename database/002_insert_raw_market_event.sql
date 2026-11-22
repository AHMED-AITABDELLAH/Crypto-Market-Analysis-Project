-- Insert one validated Binance event into bronze.raw_market_events.
-- The original JSON object is kept unchanged in the JSONB payload column.
-- Named parameters are supplied by
-- scripts/realtime_kline_ingestion/task_02_store_raw_events.py.
--
-- Safe to retry: the unique key (kafka_topic, kafka_partition, kafka_offset)
-- prevents duplicate rows. RETURNING id gives the Bronze row id both for a
-- new insert and when that Kafka coordinate already exists; the task passes
-- this id to the Silver ETL task.
INSERT INTO bronze.raw_market_events (
    event_type,
    symbol,
    source,
    event_time,
    ingestion_time,
    payload,
    kafka_topic,
    kafka_partition,
    kafka_offset
)
VALUES (
    %(event_type)s,
    %(symbol)s,
    %(source)s,
    %(event_time)s,
    %(ingestion_time)s,
    %(payload)s,
    %(kafka_topic)s,
    %(kafka_partition)s,
    %(kafka_offset)s
)
ON CONFLICT (kafka_topic, kafka_partition, kafka_offset)

DO UPDATE SET kafka_offset = EXCLUDED.kafka_offset
RETURNING id;