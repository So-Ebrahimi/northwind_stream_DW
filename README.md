# Data-pipeline-for-northwind
docker network create ProjectHost
docker-compose -f ./postgres/docker-compose.yml up -d
docker-compose -f ./kafka/docker-compose.yml up -d
docker-compose -f ./debezium/docker-compose.yml up -d
docker-compose -f ./clickhouse/docker-compose.yml up -d
spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/spark/work-dir/test.py
/opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /scripts/etl.py