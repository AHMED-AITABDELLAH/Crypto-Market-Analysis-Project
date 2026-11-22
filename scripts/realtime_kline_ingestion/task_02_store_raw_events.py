"""Airflow task that persists validated original messages to Bronze."""

from datetime import datetime, timezone

from airflow.sdk import task
from psycopg.types.json import Jsonb

from .database import connect_to_project_database, read_sql_file


@task
def store_raw_events(validated_envelopes):
    """Insert validated original JSON plus Kafka coordinates into Bronze.

    Returns Bronze row IDs so the next task can ETL precisely these events.
    """
    if not validated_envelopes:
        return []

    statement = read_sql_file("002_insert_raw_market_event.sql")
    database = connect_to_project_database()
    raw_event_ids = []
    try:
        with database.cursor() as cursor:
            for envelope in validated_envelopes:
                payload = envelope["payload"]
                kline = payload["k"]
                event_time_ms = payload.get("E")
                event_time = (
                    datetime.fromtimestamp(event_time_ms / 1000, tz=timezone.utc)
                    if isinstance(event_time_ms, int) and not isinstance(event_time_ms, bool)
                    else None
                )
                parameters = {
                    "event_type": payload.get("e", "unknown"),
                    "symbol": payload.get("s") or kline.get("s"),
                    "source": "binance_spot",
                    "event_time": event_time,
                    "ingestion_time": datetime.now(timezone.utc),
                    "payload": Jsonb(payload),
                    "kafka_topic": envelope["kafka_topic"],
                    "kafka_partition": envelope["kafka_partition"],
                    "kafka_offset": envelope["kafka_offset"],
                }
                cursor.execute(statement, parameters)
                row = cursor.fetchone()
                if row is None:
                    raise RuntimeError(
                        "Bronze insert did not return the event ID; check its SQL file."
                    )
                raw_event_ids.append(row[0])
        database.commit()
        # Duplicate events may share an ID; pass each Bronze row once to ETL.
        return list(dict.fromkeys(raw_event_ids))
    except Exception:
        database.rollback()
        raise
    finally:
        database.close()
