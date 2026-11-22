FROM apache/airflow:3.4.0

COPY infrastructure/airflow/requirements.txt /requirements.txt
# On fixe la version d'Airflow pour que pip ne la modifie pas
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
