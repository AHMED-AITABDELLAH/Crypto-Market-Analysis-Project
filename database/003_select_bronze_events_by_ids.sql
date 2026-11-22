-- Select id and payload from bronze.raw_market_events for the supplied
-- raw_event_ids array parameter. Returns one row per matching id, ordered by
-- id, so scripts/realtime_kline_ingestion/task_03_etl_klines_to_silver.py can
-- parse the original kline JSON payloads. Unrelated Bronze events are not
-- selected.
SELECT id, payload
FROM bronze.raw_market_events
WHERE id = ANY(%(raw_event_ids)s::bigint[])
ORDER BY id;