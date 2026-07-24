# Dockerfile
# Extends the official Airflow 3.2.0 image with all pipeline dependencies.
# Build once with: docker compose build
# All Airflow services (api-server, scheduler, dag-processor, triggerer) share
# this image, so packages are available everywhere without runtime pip installs.

FROM apache/airflow:3.2.1

# Install Java (required by PySpark) as root, then switch back to airflow user
USER root
RUN apt-get update -qq && \
    apt-get install -y -qq --no-install-recommends openjdk-17-jdk-headless && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# Install Python dependencies as the airflow user
RUN pip install --no-cache-dir \
      apache-airflow-providers-amazon \
      apache-airflow-providers-fab \
      kafka-python \
      pandas \
      geopandas \
      pyarrow \
      pyiceberg[s3fs,pyarrow] \
      boto3
