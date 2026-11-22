# Use the current stable Airflow release's slim Python 3.12 image.
FROM apache/airflow:slim-3.4.0-python3.12

# Keep Airflow core at the exact version provided by the base image while
# installing only the Kafka event-trigger provider and its messaging extra.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir \
    "apache-airflow==3.4.0" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.4.0/constraints-3.12.txt" \
    -r /requirements.txt
