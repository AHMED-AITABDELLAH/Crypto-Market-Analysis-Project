"""ETL : Bronze -> Silver -> controles qualite -> Gold. Declenche par l'ingestion."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from common.db import check_connection

default_args = {
    "owner": "workflow-engineer",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}


def bronze_to_silver(**context):
    # TODO (Database/ETL Engineer) : validation, deduplication, normalisation UTC
    print("TODO: bronze.raw_klines -> silver.clean_klines")


def quality_checks(**context):
    # TODO : timestamps manquants, doublons, prix invalides, volume negatif, hors-ordre
    # Si un controle echoue : lever une exception -> Gold n'est pas execute.
    print("TODO: controles qualite sur Silver")


def silver_to_gold(**context):
    # TODO (Database/ETL Engineer) : agregats, indicateurs, features
    print("TODO: silver -> gold (market_features, technical_indicators)")


with DAG(
    dag_id="etl_pipeline",
    description="Bronze -> Silver -> controles qualite -> Gold",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # declenche par historical_ingestion / historical_backfill
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["etl", "quality"],
) as dag:
    t_silver = PythonOperator(task_id="bronze_to_silver", python_callable=bronze_to_silver)
    t_quality = PythonOperator(task_id="quality_checks", python_callable=quality_checks)
    t_gold = PythonOperator(task_id="silver_to_gold", python_callable=silver_to_gold)

    check_db = PythonOperator(task_id="check_db_connection", python_callable=check_connection)

    check_db >> t_silver >> t_quality >> t_gold
