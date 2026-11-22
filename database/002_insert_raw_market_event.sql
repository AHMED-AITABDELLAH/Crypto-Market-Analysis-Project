-- Insert the original Binance event and its ingestion/Kafka traceability data.
-- If this Kafka coordinate was already inserted, return its existing Bronze ID
-- so Airflow retries can continue the Silver ETL without duplicating raw data.
WITH inserted AS (
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
    ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING
    RETURNING id
)
SELECT id
FROM inserted
UNION ALL
SELECT raw_event.id
FROM bronze.raw_market_events AS raw_event
WHERE raw_event.kafka_topic = %(kafka_topic)s
  AND raw_event.kafka_partition = %(kafka_partition)s
  AND raw_event.kafka_offset = %(kafka_offset)s
  AND NOT EXISTS (SELECT 1 FROM inserted)
LIMIT 1;
