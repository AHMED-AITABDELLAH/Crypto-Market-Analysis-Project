-- Insert one unchanged market event and its shared ingestion metadata into Bronze.
-- Keep the unique Kafka coordinate constraint on the target table so retries are safe.
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
ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING;
