FROM apache/airflow:2.9.3-python3.11

USER root

# Java нужен для Spark (local mode)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_VERSION=3.5.1
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    -o /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark && \
    rm /tmp/spark.tgz

# postgresql-jdbc driver для spark.read.jdbc(...) — качаем ещё от root,
# пока владелец /opt/spark ещё root, иначе airflow не сможет сюда писать
RUN curl -fsSL https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
    -o /opt/spark/jars/postgresql-42.7.3.jar

# отдаём Spark пользователю airflow, чтобы spark-submit не упирался в права
RUN chown -R airflow: /opt/spark

# создаём директорию для parquet-выходных данных Spark
RUN mkdir -p /opt/data/clean && chown -R airflow: /opt/data

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
