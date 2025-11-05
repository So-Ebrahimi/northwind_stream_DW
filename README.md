# Data-pipeline-for-northwind
docker network create ProjectHost
docker-compose -f ./postgres/docker-compose.yml up -d
docker-compose -f ./kafka/docker-compose.yml up -d
docker-compose -f ./debezium/docker-compose.yml up -d


# ClickHouse (replicated) and wiring Debezium topics to ClickHouse
docker-compose -f ./clickhouse/docker-compose.yml up -d

# Apply ingestion DDLs (creates Kafka sources, MVs, and target tables)
curl -sS 'http://localhost:9123/?query=' --data-binary @clickhouse/northwind_to_clickhouse.sql

# Verify data arriving (examples)
curl -s 'http://localhost:9123/?query=SELECT%20count()%20FROM%20northwind.customers'
curl -s 'http://localhost:9123/?query=SELECT%20count()%20FROM%20northwind.orders'

clickhouse-client --multiquery < /tmp/init-db/init.sql