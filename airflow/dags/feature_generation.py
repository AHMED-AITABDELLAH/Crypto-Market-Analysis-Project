"""Generation quotidienne des indicateurs techniques et features ML (Silver -> Gold)."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from common.db import check_connection

default_args = {
    "owner": "workflow-engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def compute_indicators(**context):
    # TODO (ML / ETL) : rendements, moyennes mobiles, RSI, MACD, volatilite
    print("TODO: gold.technical_indicators")


def build_ml_features(**context):
    # TODO (ML) : features ML-ready, SANS fuite de donnees futures (data leakage)
    # N'utiliser que les bougies FERMEES.
    print("TODO: gold.market_features")


with DAG(
    dag_id="feature_generation",
    description="Indicateurs techniques et features ML",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["features", "gold"],
) as dag:
    indicators = PythonOperator(task_id="compute_indicators", python_callable=compute_indicators)
    features = PythonOperator(task_id="build_ml_features", python_callable=build_ml_features)

    check_db = PythonOperator(task_id="check_db_connection", python_callable=check_connection)

    check_db >> indicators >> features
