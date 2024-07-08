FROM bitnami/spark:latest

# Create repository for Spark application
RUN mkdir -p /opt/spark-apps

# Copy Spark Streaming script to the container
COPY spark_app/spark_streaming.py /opt/spark-apps/

ENTRYPOINT ["/opt/bitnami/spark/bin/spark-submit", "/opt/spark-apps/spark_streaming.py"]