FROM apache/airflow:2.10.4

USER root

# Install OpenJDK-17
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# Install PySpark and Psycopg2
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    psycopg2-binary
