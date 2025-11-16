FROM bitnamilegacy/spark:3.5.6 AS spark-base
USER root

RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

# JDBC برای ClickHouse
COPY ./scripts/clickhouse-jdbc-0.7.2-all.jar /opt/bitnami/spark/jars/

# Kafka + Hadoop + Snappy + LZ4 + commons-logging + JSR305
RUN wget -P /opt/bitnami/spark/jars/ \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    wget -P /opt/bitnami/spark/jars/ \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    wget -P /opt/bitnami/spark/jars/ \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    wget -P /opt/bitnami/spark/jars/ \
    https://repo1.maven.org/maven2/com/google/code/findbugs/jsr305/3.0.0/jsr305-3.0.0.jar && \
    wget -P /opt/bitnami/spark/jars/ \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    wget -P /opt/bitnami/spark/jars/ \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.4/hadoop-client-runtime-3.3.4.jar && \
    wget -P /opt/bitnami/spark/jars/ \
    https://repo1.maven.org/maven2/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar && \
    wget -P /opt/bitnami/spark/jars/ \
    https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.10.3/snappy-java-1.1.10.3.jar && \
    wget -P /opt/bitnami/spark/jars/ \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.4/hadoop-client-api-3.3.4.jar && \
    wget -P /opt/bitnami/spark/jars/ \
    https://repo1.maven.org/maven2/commons-logging/commons-logging/1.1.3/commons-logging-1.1.3.jar

# جلوگیری از دانلود JAR در runtime
ENV SPARK_JARS_IVY_RESOLVE_ENABLED=false

# اضافه کردن فایل تنظیمات
COPY ./conf/spark-defaults.conf /opt/bitnami/spark/conf/
