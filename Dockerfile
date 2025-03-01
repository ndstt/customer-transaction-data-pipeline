FROM apache/airflow:2.10.3 

USER root

RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    wget \
    curl \
    && apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

RUN mkdir -p /opt/spark && \
    wget -O /tmp/spark.tgz https://archive.apache.org/dist/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt/spark --strip-components=1 && \
    rm /tmp/spark.tgz

RUN mkdir -p /opt/spark/jars && \
    wget -O /opt/spark/jars/gcs-connector-hadoop3-latest.jar \
    https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar && \
    wget -O /opt/spark/jars/postgresql-42.7.4.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.4.jar

ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV PYTHONPATH="$SPARK_HOME/python:$PYTHONPATH"

RUN chmod -R 755 /opt/spark/bin /opt/spark/sbin

USER airflow

RUN pip install --no-cache-dir \
    google-cloud-storage \
    pyspark \
    faker