"""Backfill manuel d'une plage de dates -> Bronze. Lance a la main avec parametres."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from common.db import check_connection
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    "owner": "workflow-engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def backfill(**context):
    p = context["params"]
    # TODO (Data Engineer) : appeler services/binance/historical_collector.py
    # avec pagination et gestion des limites de l'API Binance.
    print(f"TODO: backfill {p['symbol']} {p['interval']} de {p['start_date']} a {p['end_date']}")


with DAG(
    dag_id="historical_backfill",
    description="Backfill manuel d'une plage de dates (Declencher avec parametres)",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # uniquement manuel
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={
        "symbol": Param("BTCUSDT", type="string"),
        "interval": Param("1m", type="string"),
        "start_date": Param("2026-01-01", type="string", description="YYYY-MM-DD (UTC)"),
        "end_date": Param("2026-01-31", type="string", description="YYYY-MM-DD (UTC)"),
    },
    tags=["ingestion", "backfill"],
) as dag:
    run_backfill = PythonOperator(task_id="backfill_to_bronze", python_callable=backfill)

    trigger_etl = TriggerDagRunOperator(
        task_id="trigger_etl_pipeline",
        trigger_dag_id="etl_pipeline",
        wait_for_completion=False,
    )

    check_db = PythonOperator(task_id="check_db_connection", python_callable=check_connection)

    check_db >> run_backfill >> trigger_etl
