"""Collecte historique Binance (REST) -> Bronze, puis declenche l'ETL."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from common.db import check_connection
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    "owner": "workflow-engineer",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}


def collect_historical(**context):
    # TODO (Data Engineer) : appeler services/binance/historical_collector.py
    # Exemple : collect(symbol="BTCUSDT", interval="1m", start=..., end=...)
    # Doit etre IDEMPOTENT : relancer ne cree pas de doublons dans Bronze.
    print("TODO: collecte BTCUSDT 1m -> bronze.raw_klines")


with DAG(
    dag_id="historical_ingestion",
    description="Collecte REST Binance vers Bronze, puis lance l'ETL",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["ingestion", "btcusdt"],
) as dag:
    collect = PythonOperator(task_id="collect_historical", python_callable=collect_historical)

    trigger_etl = TriggerDagRunOperator(
        task_id="trigger_etl_pipeline",
        trigger_dag_id="etl_pipeline",
        wait_for_completion=False,
    )

    check_db = PythonOperator(task_id="check_db_connection", python_callable=check_connection)

    check_db >> collect >> trigger_etl
