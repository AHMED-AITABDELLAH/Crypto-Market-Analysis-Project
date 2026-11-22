-- Retrieve only the Bronze events passed from the preceding task.
SELECT id, payload
FROM bronze.raw_market_events
WHERE id = ANY(%(raw_event_ids)s)
ORDER BY id;
