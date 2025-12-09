# CDC Pipeline Monitoring and Data Tracking Guide

This guide explains how to monitor and track data flow through your real-time CDC pipeline without modifying any code or configuration.

## Pipeline Architecture Overview

```
PostgreSQL (OLTP) 
    ↓ [Debezium CDC]
Kafka (Message Broker)
    ↓ [Spark Streaming]
ClickHouse Staging (northwind.*)
    ↓ [Spark ETL]
ClickHouse Data Warehouse (Star Schema)
    ↓
Grafana (Visualization)
```

## Quick Start

### Option 1: Python Script (Cross-platform)

```bash
# Insert a test customer
python pipeline_monitor.py --insert-customer --customer-id TEST1

# Monitor Kafka topics
python pipeline_monitor.py --monitor-kafka

# Check staging tables
python pipeline_monitor.py --check-staging

# Check data warehouse
python pipeline_monitor.py --check-dw

# Complete tracking (insert + monitor all stages)
python pipeline_monitor.py --track-all --customer-id TEST1
```

### Option 2: PowerShell Script (Windows)

```powershell
# Insert a test customer
.\pipeline_monitor.ps1 -Action insert-customer -CustomerId TEST1

# Monitor Kafka topics
.\pipeline_monitor.ps1 -Action monitor-kafka

# Check staging tables
.\pipeline_monitor.ps1 -Action check-staging

# Check data warehouse
.\pipeline_monitor.ps1 -Action check-dw

# Complete tracking
.\pipeline_monitor.ps1 -Action track-all -CustomerId TEST1
```

## Manual Monitoring Commands

### 1. Insert Test Data into PostgreSQL

#### Insert a Customer
```powershell
docker exec postgres psql -U postgres -d northwind -c "INSERT INTO customers (customer_id, company_name, contact_name, city, country) VALUES ('TEST1', 'Test Company', 'Test Contact', 'Test City', 'Test Country');"
```

#### Insert an Order
```powershell
docker exec postgres psql -U postgres -d northwind -c "INSERT INTO orders (order_id, customer_id, employee_id, order_date, required_date, ship_via, freight) VALUES (99999, 'TEST1', 1, '2024-01-15', '2024-01-20', 1, 10.50);"
docker exec postgres psql -U postgres -d northwind -c "INSERT INTO order_details (order_id, product_id, quantity, unit_price, discount) VALUES (99999, 1, 10, 25.50, 0.0);"
```

#### Update a Customer
```powershell
docker exec postgres psql -U postgres -d northwind -c "UPDATE customers SET company_name = 'Updated Company Name' WHERE customer_id = 'TEST1';"
```

#### Delete a Customer
```powershell
docker exec postgres psql -U postgres -d northwind -c "DELETE FROM customers WHERE customer_id = 'TEST1';"
```

### 2. Monitor Kafka Topics

#### List All Topics
```powershell
docker exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092
```

#### View Messages from a Topic
```powershell
# View recent messages from customers topic
docker exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic northwind.public.customers --from-beginning --max-messages 5 --timeout-ms 5000
```

#### Check Topic Details
```powershell
docker exec kafka kafka-topics.sh --describe --bootstrap-server localhost:9092 --topic northwind.public.customers
```

### 3. Check ClickHouse Staging Tables

#### Count Records in Staging
```powershell
docker exec clickhouse1 clickhouse-client --query "SELECT COUNT(*) FROM northwind.northwind_customers"
docker exec clickhouse1 clickhouse-client --query "SELECT COUNT(*) FROM northwind.northwind_orders"
docker exec clickhouse1 clickhouse-client --query "SELECT COUNT(*) FROM northwind.northwind_order_details"
```

#### View Recent Records
```powershell
docker exec clickhouse1 clickhouse-client --query "SELECT * FROM northwind.northwind_customers ORDER BY updatedate DESC LIMIT 5 FORMAT PrettyCompact"
```

#### Check for Specific Customer
```powershell
docker exec clickhouse1 clickhouse-client --query "SELECT customer_id, company_name, operation, updatedate FROM northwind.northwind_customers WHERE customer_id = 'TEST1' ORDER BY updatedate DESC"
```

### 4. Check ClickHouse Data Warehouse

#### Count Records in DW Tables
```powershell
docker exec clickhouse1 clickhouse-client --query "SELECT COUNT(*) FROM DimCustomer"
docker exec clickhouse1 clickhouse-client --query "SELECT COUNT(*) FROM DimProducts"
docker exec clickhouse1 clickhouse-client --query "SELECT COUNT(*) FROM FactOrders"
```

#### View Recent Records
```powershell
docker exec clickhouse1 clickhouse-client --query "SELECT * FROM DimCustomer ORDER BY updatedate DESC LIMIT 5 FORMAT PrettyCompact"
docker exec clickhouse1 clickhouse-client --query "SELECT * FROM FactOrders ORDER BY updatedate DESC LIMIT 5 FORMAT PrettyCompact"
```

#### Check for Specific Customer in DW
```powershell
docker exec clickhouse1 clickhouse-client --query "SELECT CustomerAlternateKey, CompanyName, updatedate FROM DimCustomer WHERE CustomerAlternateKey = 'TEST1' ORDER BY updatedate DESC"
```

### 5. Monitor Debezium Connector

#### Check Connector Status
```powershell
docker exec debezium-connect curl -s http://localhost:8083/connectors/postgres-northwind-connector/status
```

#### List All Connectors
```powershell
docker exec debezium-connect curl -s http://localhost:8083/connectors
```

### 6. Monitor Spark Jobs

#### Check CDC Streaming Job Logs
```powershell
docker logs pyspark-job-cdc --tail 50
```

#### Check DW ETL Job Logs
```powershell
docker logs pyspark-job-dw --tail 50
```

#### Follow Logs in Real-time
```powershell
docker logs -f pyspark-job-cdc
docker logs -f pyspark-job-dw
```

## Data Flow Tracking Workflow

### Complete End-to-End Tracking

1. **Insert Test Data** (PostgreSQL)
   ```powershell
   docker exec postgres psql -U postgres -d northwind -c "INSERT INTO customers (customer_id, company_name, contact_name, city, country) VALUES ('TEST1', 'Test Company', 'Test Contact', 'Test City', 'Test Country');"
   ```

2. **Wait for Debezium Capture** (~2-5 seconds)
   ```powershell
   Start-Sleep -Seconds 5
   ```

3. **Check Kafka Topic** (Verify CDC event captured)
   ```powershell
   docker exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic northwind.public.customers --from-beginning --max-messages 3 --timeout-ms 5000
   ```

4. **Wait for Spark Processing** (~5-10 seconds)
   ```powershell
   Start-Sleep -Seconds 10
   ```

5. **Check ClickHouse Staging** (Verify data in staging)
   ```powershell
   docker exec clickhouse1 clickhouse-client --query "SELECT customer_id, company_name, operation, updatedate FROM northwind.northwind_customers WHERE customer_id = 'TEST1' ORDER BY updatedate DESC LIMIT 1"
   ```

6. **Wait for ETL Job** (~20-30 seconds)
   ```powershell
   Start-Sleep -Seconds 30
   ```

7. **Check Data Warehouse** (Verify data in DW)
   ```powershell
   docker exec clickhouse1 clickhouse-client --query "SELECT CustomerAlternateKey, CompanyName, updatedate FROM DimCustomer WHERE CustomerAlternateKey = 'TEST1' ORDER BY updatedate DESC LIMIT 1"
   ```

## Understanding Operation Types

The pipeline tracks three types of operations:

- **'c' (Create/Insert)**: New records inserted into PostgreSQL
- **'u' (Update)**: Existing records updated in PostgreSQL
- **'d' (Delete)**: Records deleted from PostgreSQL

You can filter by operation type in ClickHouse:

```powershell
# View only inserts
docker exec clickhouse1 clickhouse-client --query "SELECT * FROM northwind.northwind_customers WHERE operation = 'c' ORDER BY updatedate DESC LIMIT 5"

# View only updates
docker exec clickhouse1 clickhouse-client --query "SELECT * FROM northwind.northwind_customers WHERE operation = 'u' ORDER BY updatedate DESC LIMIT 5"

# View only deletes
docker exec clickhouse1 clickhouse-client --query "SELECT * FROM northwind.northwind_customers WHERE operation = 'd' ORDER BY updatedate DESC LIMIT 5"
```

## Timing Considerations

The pipeline has different latency at each stage:

- **PostgreSQL → Kafka**: ~2-5 seconds (Debezium capture)
- **Kafka → ClickHouse Staging**: ~5-10 seconds (Spark streaming)
- **ClickHouse Staging → DW**: ~20-30 seconds (Spark ETL job runs every 20 seconds)

**Total end-to-end latency**: ~30-45 seconds for new data to appear in the data warehouse.

## Troubleshooting

### Data Not Appearing in Kafka
1. Check Debezium connector status
2. Verify PostgreSQL logical replication is enabled
3. Check Debezium logs: `docker logs debezium-connect`

### Data Not Appearing in Staging
1. Check Spark CDC job logs: `docker logs pyspark-job-cdc`
2. Verify Kafka topics have messages
3. Check Spark checkpoint directory

### Data Not Appearing in DW
1. Check Spark DW job logs: `docker logs pyspark-job-dw`
2. Verify staging tables have data
3. Check for ETL errors in logs

## Best Practices

1. **Use unique test IDs**: Always use unique customer/order IDs for testing to avoid conflicts
2. **Wait between operations**: Allow time for each stage to process (see timing above)
3. **Monitor logs**: Keep an eye on Spark job logs for errors
4. **Check operation types**: Verify that inserts, updates, and deletes are all captured correctly
5. **Verify end-to-end**: Always check all stages (Kafka → Staging → DW) to ensure complete flow

## Example Test Scenario

```powershell
# 1. Insert a new customer
docker exec postgres psql -U postgres -d northwind -c "INSERT INTO customers (customer_id, company_name, contact_name, city, country) VALUES ('TEST1', 'Test Company', 'Test Contact', 'Test City', 'Test Country');"

# 2. Wait and check Kafka
Start-Sleep -Seconds 5
docker exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic northwind.public.customers --from-beginning --max-messages 1 --timeout-ms 5000

# 3. Wait and check staging
Start-Sleep -Seconds 10
docker exec clickhouse1 clickhouse-client --query "SELECT customer_id, company_name, operation FROM northwind.northwind_customers WHERE customer_id = 'TEST1'"

# 4. Update the customer
docker exec postgres psql -U postgres -d northwind -c "UPDATE customers SET company_name = 'Updated Company' WHERE customer_id = 'TEST1';"

# 5. Wait and check for update in staging
Start-Sleep -Seconds 15
docker exec clickhouse1 clickhouse-client --query "SELECT customer_id, company_name, operation FROM northwind.northwind_customers WHERE customer_id = 'TEST1' ORDER BY updatedate DESC"

# 6. Wait and check DW
Start-Sleep -Seconds 30
docker exec clickhouse1 clickhouse-client --query "SELECT CustomerAlternateKey, CompanyName, updatedate FROM DimCustomer WHERE CustomerAlternateKey = 'TEST1'"
```

## Notes

- All monitoring is **read-only** - no code or configuration is modified
- The pipeline processes changes in **real-time** using CDC
- Spark jobs use **checkpointing** for fault tolerance
- ClickHouse uses **ReplacingMergeTree** for handling updates
- The ETL job runs **incrementally**, processing only changed records

