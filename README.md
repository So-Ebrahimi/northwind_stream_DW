# Data-pipeline-for-northwind
docker network create ProjectHost
docker-compose -f ./postgres/docker-compose.yml up -d
docker-compose -f ./kafka/docker-compose.yml up -d
docker-compose -f ./debezium/docker-compose.yml up -d
docker-compose -f ./clickhouse/docker-compose.yml up -d
