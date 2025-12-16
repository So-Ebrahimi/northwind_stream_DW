# Northwind Pipeline Test Checklist

Quick, ordered checks to validate each layer of the stack. Commands assume PowerShell on Windows; adjust paths as needed.

## 0) Preflight
- Docker + Docker Compose installed; >=16GB RAM free.
- Ports free: 15432, 18083, 39092/29092, 19000/18123, 18085, 12345.
- Create network once: `docker network create ProjectHost` (ignore error if exists).

## 1) Bring Up Stack
- From repo root: `docker-compose up -d --force-recreate`.
- Verify containers: `docker ps` (expect postgres, zookeeper, kafka, debezium-connect, clickhouse1, spark master/worker, pyspark-job-cdc, pyspark-job-dw, grafana).

## 2) Service Health Checks
- PostgreSQL: `docker exec postgres pg_isready`.
- Debezium connector status:  
  `docker exec debezium-connect curl -s http://localhost:8083/connectors/postgres-northwind-connector/status | jq '.state'` (expect `"RUNNING"`).
- Kafka topics exist:  
  `docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list | findstr northwind.public`.
- ClickHouse reachability:  
  `docker exec clickhouse1 clickhouse-client --query "SELECT 1"`.
- Spark UIs reachable: open `http://localhost:18085`.
- Grafana reachable: open `http://localhost:12345` (admin/admin).

## 3) Staging Pipeline (CDC → ClickHouse)
- Sample row count:  
  `docker exec clickhouse1 clickhouse-client --query "SELECT count() FROM northwind.northwind_orders"`.
- CDC liveness smoke: insert and expect growth
  1. Insert in Postgres:  
     `docker exec -i postgres psql -U postgres -d northwind -c "INSERT INTO orders (orderid, customerid, employeeid, orderdate, requireddate, shippeddate, shipvia, freight, shipname, shipaddress, shipcity, shipregion, shippostalcode, shipcountry) VALUES (99999, 'ALFKI', 1, now(), now(), now(), 1, 0, 'test', 'addr', 'city', 'region', '00000', 'country');"`
  2. Wait ~20s, then recheck ClickHouse count increases:  
     `docker exec clickhouse1 clickhouse-client --query "SELECT count() FROM northwind.northwind_orders"`.

## 4) DW Build (Spark DW Job)
- Fact table count not zero:  
  `docker exec clickhouse1 clickhouse-client --query "SELECT count() FROM FactOrders"`.
- Dimensions populated:  
  `docker exec clickhouse1 clickhouse-client --query "SELECT count() FROM DimCustomers"` (repeat for other dims as needed).
- Confirm last_run.txt advancing:  
  `docker exec pyspark-job-dw cat /opt/spark/work-dir/last_run.txt`.

## 5) Monitoring & Logs
- Spark CDC job logs: `docker logs --tail 50 pyspark-job-cdc`.
- Spark DW job logs: `docker logs --tail 50 pyspark-job-dw`.
- Debezium connector logs: `docker logs --tail 50 debezium-connect`.
- Kafka broker logs (optional): `docker logs --tail 50 kafka`.

## 6) Grafana Dashboards
- Open Grafana `http://localhost:12345`, login admin/admin.
- Check ClickHouse datasource status (provisioned).
- Load provided dashboards under `northwind-*`; verify panels show data (row counts, trends).

## 7) Teardown
- From repo root: `docker-compose down`.
- Optional per-service down (reverse order) if debugging.

## 8) Reset/Test Data Cleanup (optional)
- Remove test order from Postgres (example):  
  `docker exec -i postgres psql -U postgres -d northwind -c "DELETE FROM orders WHERE orderid = 99999;"`.
- Allow CDC + DW to reconcile; verify staging and facts drop back (counts decrease after a few minutes).

