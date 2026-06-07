FROM apache/airflow:3.0.2

USER root

# Java cần thiết để chạy spark-submit từ bên trong Airflow container
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# pyspark: đi kèm spark-submit binary
# apache-airflow-providers-apache-spark: cung cấp SparkSubmitOperator cho DAG
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==5.0.0 \
    pyspark==3.5.1
