FROM python:3.10-bullseye

USER root

# Install system dependencies including OpenJDK (required for Spark)
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set Java home
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Install Python packages
RUN pip install --no-cache-dir \
    streamlit==1.28.0 \
    pandas==2.1.0 \
    cassandra-driver==3.28.0 \
    pyspark==3.5.0

# Working directory
WORKDIR /app

# Copy application
COPY app.py /app/

# Expose port
EXPOSE 8501

# Health check for Cassandra
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "from cassandra.cluster import Cluster; Cluster(['cassandra']).connect()"

# Start Streamlit
CMD ["sh", "-c", "echo 'Waiting for Cassandra to be ready...'; sleep 45; streamlit run app.py --server.port=8501 --server.address=0.0.0.0"]