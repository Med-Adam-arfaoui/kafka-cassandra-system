FROM python:3.11-slim

WORKDIR /app

# Copy producer code
COPY producer.py .

# Install dependencies
RUN pip install --no-cache-dir kafka-python

# Set default broker environment variable (can be overridden)
ENV BROKER=kafka:9092

CMD ["python", "producer.py"]
