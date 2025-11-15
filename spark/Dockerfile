FROM bitnamilegacy/spark:3.5.6 AS spark-base
USER root
RUN mkdir -p /var/lib/apt/lists/partial \
    && apt-get update \
    && apt-get install -y wget \
    && rm -rf /var/lib/apt/lists/*

# فولدر JARها
# RUN mkdir -p /opt/bitnami/spark/jars
# اضافه کردن ClickHouse JDBC
COPY ./scripts/clickhouse-jdbc-0.7.2-all.jar /opt/bitnami/spark/jars/

# دانلود JARهای Spark/Kafka/Hadoop
RUN wget -O /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    wget -O /opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    wget -O /opt/bitnami/spark/jars/kafka-clients-3.4.1.jar \
        https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    wget -O /opt/bitnami/spark/jars/jsr305-3.0.0.jar \
        https://repo1.maven.org/maven2/com/google/code/findbugs/jsr305/3.0.0/jsr305-3.0.0.jar && \
    wget -O /opt/bitnami/spark/jars/commons-pool2-2.11.1.jar \
        https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    wget -O /opt/bitnami/spark/jars/hadoop-client-runtime-3.3.4.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.4/hadoop-client-runtime-3.3.4.jar && \
    wget -O /opt/bitnami/spark/jars/lz4-java-1.8.0.jar \
        https://repo1.maven.org/maven2/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar && \
    wget -O /opt/bitnami/spark/jars/snappy-java-1.1.10.3.jar \
        https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.10.3/snappy-java-1.1.10.3.jar && \
    # wget -O /opt/bitnami/spark/jars/slf4j-api-2.0.7.jar \
    #     https://repo1.maven.org/maven2/org/slf4j/slf4j-api/2.0.7/slf4j-api.jar && \
    wget -O /opt/bitnami/spark/jars/hadoop-client-api-3.3.4.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.4/hadoop-client-api-3.3.4.jar && \
    wget -O /opt/bitnami/spark/jars/commons-logging-1.1.3.jar \
        https://repo1.maven.org/maven2/commons-logging/commons-logging/1.1.3/commons-logging-1.1.3.jar

ENV SPARK_CLASSPATH="/opt/bitnami/spark/jars/*"
