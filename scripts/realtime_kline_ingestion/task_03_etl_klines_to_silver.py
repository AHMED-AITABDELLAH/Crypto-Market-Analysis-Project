"""Airflow task that reads newly stored Bronze klines and upserts Silver."""

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from airflow.sdk import task

from .database import connect_to_project_database, read_sql_file


def _decimal(value, name, raw_event_id):
    """Parse numeric strings exactly as Decimal values."""
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as error:
        raise ValueError(f"Invalid {name} in Bronze event {raw_event_id}.") from error
    if not result.is_finite():
        raise ValueError(f"Non-finite {name} in Bronze event {raw_event_id}.")
    return result


def _utc_datetime(milliseconds, name, raw_event_id):
    """Convert a Binance millisecond timestamp to a timezone-aware UTC value."""
    if not isinstance(milliseconds, int) or isinstance(milliseconds, bool):
        raise ValueError(f"Invalid {name} timestamp in Bronze event {raw_event_id}.")
    return datetime.fromtimestamp(milliseconds / 1000, tz=timezone.utc)


@task
def etl_klines_to_silver(raw_event_ids):
    """Normalize stored kline payloads and upsert rows in ``silver.candles``.
    """
    if not raw_event_ids:
        return 0

    select_statement = read_sql_file("003_select_bronze_events_by_ids.sql")
    upsert_statement = read_sql_file("004_upsert_silver_candle.sql")
    database = connect_to_project_database()
    processed_count = 0
    try:
        with database.cursor() as cursor:
            cursor.execute(select_statement, {"raw_event_ids": raw_event_ids})
            raw_rows = cursor.fetchall()

            for raw_event_id, payload in raw_rows:
                kline = payload["k"]
                cursor.execute(
                    upsert_statement,
                    {
                        "symbol": payload["s"].strip().upper(),
                        "interval": kline["i"],
                        "open_time": _utc_datetime(kline.get("t"), "open", raw_event_id),
                        "close_time": _utc_datetime(kline.get("T"), "close", raw_event_id),
                        "open_price": _decimal(kline.get("o"), "open price", raw_event_id),
                        "high_price": _decimal(kline.get("h"), "high price", raw_event_id),
                        "low_price": _decimal(kline.get("l"), "low price", raw_event_id),
                        "close_price": _decimal(kline.get("c"), "close price", raw_event_id),
                        "volume": _decimal(kline.get("v"), "volume", raw_event_id),
                        "is_closed": kline.get("x") is True,
                    },
                )
                processed_count += 1

        database.commit()
        return processed_count
    except Exception:
        database.rollback()
        raise
    finally:
        database.close()
