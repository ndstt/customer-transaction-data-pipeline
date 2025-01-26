FROM apache/airflow:2.10.3

USER root

RUN apt-get update && apt-get install -y \
    default-jdk \
    wget \
    curl \
    && apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

USER airflow

RUN pip install --no-cache-dir \
    google-cloud-storage \
    pyspark \
    faker