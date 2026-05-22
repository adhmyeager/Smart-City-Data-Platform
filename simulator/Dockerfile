FROM python:3.11-slim

LABEL maintainer="Smart City Team"
LABEL description="Vehicle telemetry simulator for Smart City platform"

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl netcat-traditional \
    && rm -rf /var/lib/apt/lists/*

# Python deps first (layer cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App code
COPY . .

# Wait for Kafka to be ready, then start producer
CMD ["sh", "-c", "echo 'Waiting for Kafka...' && \
     until nc -z ${KAFKA_BOOTSTRAP_SERVERS%%:*} ${KAFKA_BOOTSTRAP_SERVERS##*:}; do sleep 2; done && \
     echo 'Kafka ready. Starting simulator...' && \
     python kafka_producer.py"]
