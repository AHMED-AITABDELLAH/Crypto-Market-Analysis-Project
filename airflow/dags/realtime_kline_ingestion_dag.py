"""Run Bronze-to-Silver processing when Binance closes a Spot kline."""

from datetime import datetime, timedelta, timezone

from airflow.providers.apache.kafka.triggers.msg_queue import (
    KafkaMessageQueueTrigger,
)
from airflow.sdk import Asset, AssetWatcher, DAG

from scripts.realtime_kline_ingestion.task_01_validate_closed_klines import (
    validate_closed_klines,
)
from scripts.realtime_kline_ingestion.task_02_store_raw_events import (
    store_raw_events,
)
from scripts.realtime_kline_ingestion.task_03_etl_klines_to_silver import (
    etl_klines_to_silver,
)


# The callback only emits an event for a finalized Binance kline (k.x is true).
closed_kline_trigger = KafkaMessageQueueTrigger(
    topics=["market.raw.spot.klines"],
    kafka_config_id="kafka_default",
    apply_function=(
        "scripts.realtime_kline_ingestion."
        "step_00_filter_closed_kline_event.capture_closed_kline"
    ),
    poll_timeout=1,
    poll_interval=2,
)

closed_kline_asset = Asset(
    "kafka://market.raw.spot.klines/closed",
    watchers=[
        AssetWatcher(
            name="binance_closed_kline_watcher",
            trigger=closed_kline_trigger,
        )
    ],
)


with DAG(
    dag_id="realtime_kline_ingestion",
    description=(
        "Validate each closed Binance kline, store the raw event in Bronze, "
        "then upsert the structured candle in Silver."
    ),
    schedule=[closed_kline_asset],
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
    },
    tags=["realtime", "binance", "klines"],
) as dag:
    validated_envelopes = validate_closed_klines()
    raw_event_ids = store_raw_events(validated_envelopes)
    etl_klines_to_silver(raw_event_ids)
