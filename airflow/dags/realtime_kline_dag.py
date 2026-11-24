"""Real-time Binance kline ingestion DAG.

Flux :
    Kafka (kline fermée) -> validate -> store Bronze -> ETL Silver

Le DAG est déclenché par un Asset surveillé par un AssetWatcher Kafka.
Le filtre `capture_closed_kline` ne laisse passer que les bougies finales
(x=true) ; chaque événement retenu déclenche un run du DAG.

Arborescence (montée dans Docker via docker-compose.yaml) :

    airflow/dags/realtime_kline_dag.py              -> /opt/airflow/dags
    scripts/realtime_kline_ingestion/               -> /opt/airflow/scripts/...
    ├── __init__.py
    ├── database.py
    ├── step_00_filter_closed_kline_event.py
    ├── task_01_validate_closed_klines.py
    ├── task_02_store_raw_events.py
    └── task_03_etl_klines_to_silver.py

PYTHONPATH=/opt/airflow rend le package importable sous `scripts.`.
"""

import os
from datetime import timedelta

from airflow.providers.apache.kafka.triggers.await_message import AwaitMessageTrigger
from airflow.sdk import DAG, Asset, AssetWatcher

from scripts.realtime_kline_ingestion.task_01_validate_closed_klines import validate_closed_klines
from scripts.realtime_kline_ingestion.task_02_store_raw_events import store_raw_events
from scripts.realtime_kline_ingestion.task_03_etl_klines_to_silver import etl_klines_to_silver


KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC_RAW_KLINES", "market.raw.spot.klines")
KAFKA_CONNECTION_ID = os.environ.get("KAFKA_CONN_ID", "kafka_default")

# Chemin pointé (dotted path) résolu par le triggerer, pas un import Python.
KAFKA_FILTER_FUNCTION = (
    "scripts.realtime_kline_ingestion.step_00_filter_closed_kline_event.capture_closed_kline"
)

closed_kline_asset = Asset(
    name="binance_closed_kline",
    watchers=[
        AssetWatcher(
            name="binance_closed_kline_watcher",
            trigger=AwaitMessageTrigger(
                topics=[KAFKA_TOPIC],
                apply_function=KAFKA_FILTER_FUNCTION,
                kafka_config_id=KAFKA_CONNECTION_ID,
                poll_timeout=1,
                poll_interval=5,
            ),
        )
    ],
)

default_args = {
    "owner": "ahmed",
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
}

with DAG(
    dag_id="realtime_kline_ingestion",
    description="Kafka closed klines -> validation -> Bronze -> Silver candles",
    schedule=[closed_kline_asset],
    start_date=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["binance", "kafka", "bronze", "silver"],
) as dag:

    # `triggering_asset_events` est injecté automatiquement depuis le contexte
    # Airflow : pas besoin de le passer explicitement.
    validated_envelopes = validate_closed_klines()
    raw_event_ids = store_raw_events(validated_envelopes)
    etl_klines_to_silver(raw_event_ids)
