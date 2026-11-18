# Build the Kafka consumer as a focused Python service image.
FROM python:3.13-slim

WORKDIR /app

# Install only dependencies used by Kafka consumers that write to PostgreSQL.
COPY services/kafka/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Include the consumer code and its external SQL statement file.
COPY services/kafka/bronze_consumer.py ./bronze_consumer.py
COPY database/insert_raw_market_event.sql ./sql/insert_raw_market_event.sql

# Run the Kafka-to-Bronze consumer.
CMD ["python", "bronze_consumer.py"]
