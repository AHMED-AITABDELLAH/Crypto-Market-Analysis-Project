-- Insert or update one candle in silver.candles.
-- The primary key (symbol, interval, open_time) identifies a candle, so
-- reprocessing the same candle updates it instead of creating a duplicate.
-- Guard: a candle that is already closed is never overwritten by a
-- still-forming update that arrives later.
INSERT INTO silver.candles (
    symbol,
    interval,
    open_time,
    close_time,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    is_closed
)
VALUES (
    %(symbol)s,
    %(interval)s,
    %(open_time)s,
    %(close_time)s,
    %(open_price)s,
    %(high_price)s,
    %(low_price)s,
    %(close_price)s,
    %(volume)s,
    %(is_closed)s
)
ON CONFLICT (symbol, interval, open_time)
DO UPDATE SET
    close_time = EXCLUDED.close_time,
    open_price = EXCLUDED.open_price,
    high_price = EXCLUDED.high_price,
    low_price = EXCLUDED.low_price,
    close_price = EXCLUDED.close_price,
    volume = EXCLUDED.volume,
    is_closed = EXCLUDED.is_closed
WHERE silver.candles.is_closed = FALSE
   OR EXCLUDED.is_closed = TRUE;
