# Data-pipeline-for-northwind
docker network create ProjectHost
docker-compose -f ./1-postgres/docker-compose.yml up -d 
docker-compose -f ./3-kafka/docker-compose.yml up -d
docker-compose -f ./4-debezium/docker-compose.yml up -d
docker-compose -f ./5-clickhouse/docker-compose.yml up -d
docker-compose -f ./6-spark/docker-compose.yml up -d
