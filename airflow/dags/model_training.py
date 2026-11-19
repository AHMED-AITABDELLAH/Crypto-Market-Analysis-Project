"""Entrainement et evaluation des modeles (classification + anomalies), log MLflow."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from common.db import check_connection

default_args = {
    "owner": "workflow-engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


def check_data_ready(**context):
    # TODO : verifier fraicheur et qualite des donnees Gold avant d'entrainer.
    # Si insuffisant : lever une exception -> pas d'entrainement.
    print("TODO: verification des donnees Gold")


def train_direction_model(**context):
    # TODO (ML + MLOps) : UP / DOWN / SIDEWAYS, split chronologique, log MLflow
    print("TODO: ml/direction_classification + MLflow")


def train_anomaly_model(**context):
    # TODO (ML + MLOps) : Isolation Forest / LOF, log MLflow
    print("TODO: ml/anomaly_detection + MLflow")


def evaluate_models(**context):
    # TODO : accuracy, precision, recall, F1, matrice de confusion ; metriques -> MLflow
    print("TODO: ml/evaluation")


def store_predictions(**context):
    # TODO : ecrire gold.market_predictions et gold.market_anomalies (avec version du modele)
    print("TODO: ecriture des predictions")


with DAG(
    dag_id="model_training",
    description="Entrainement, evaluation, MLflow, predictions",
    start_date=datetime(2026, 1, 1),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["ml", "mlflow"],
) as dag:
    ready = PythonOperator(task_id="check_data_ready", python_callable=check_data_ready)
    direction = PythonOperator(task_id="train_direction_model", python_callable=train_direction_model)
    anomaly = PythonOperator(task_id="train_anomaly_model", python_callable=train_anomaly_model)
    evaluate = PythonOperator(task_id="evaluate_models", python_callable=evaluate_models)
    predictions = PythonOperator(task_id="store_predictions", python_callable=store_predictions)

    check_db = PythonOperator(task_id="check_db_connection", python_callable=check_connection)

    check_db >> ready >> [direction, anomaly] >> evaluate >> predictions
