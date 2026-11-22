# Use a slim Python image to keep the application container small.
FROM python:3.13-slim

# Use an explicit working directory inside the container.
WORKDIR /app

# Install Python dependencies before copying source for better build caching.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the project files; Docker Compose sends only files not excluded by .dockerignore.
COPY websocket_client.py .

# Start the Binance WebSocket-to-Kafka publisher when the container runs.
CMD ["python", "websocket_client.py"]


