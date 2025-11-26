# Data Pipeline Diagnostic Review Checklist

## Pipeline Architecture Overview
**Flow:** PostgreSQL (OLTP) → Debezium (CDC) → Kafka → Spark → ClickHouse (DW)

**Components:**
- PostgreSQL: Port 15432 (Source OLTP database)
- Debezium Connect: CDC connector (Container: debezium-connect)
- Kafka + Zookeeper: Ports 39092, 29092 (Message broker)
- Spark: Master/Worker + 2 jobs (CDC streaming + DW ETL)
- ClickHouse: 2 replicas, Ports 18123, 28123 (Data warehouse)

---

## 1. STEP-BY-STEP VALIDATION CHECKLIST

### Stage 1: Source Database (PostgreSQL) - INGESTION

#### 1.1 Container Health
```powershell
# Check container status
docker ps --filter "name=postgres" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# Check container logs for errors
docker logs postgres --tail 100

# Check if PostgreSQL is accepting connections
docker exec postgres pg_isready -U postgres
```

**Expected Results:**
- Container status: `Up` (healthy)
- No ERROR or FATAL messages in logs
- `pg_isready` returns: `postgres:5432 - accepting connections`

#### 1.2 Database Connectivity & Data Validation
```powershell
# Connect to PostgreSQL and verify database exists
docker exec -it postgres psql -U postgres -d northwind -c "\dt"

# Check replication slot status (critical for Debezium)
docker exec -it postgres psql -U postgres -d northwind -c "SELECT * FROM pg_replication_slots WHERE slot_name = 'debezium';"

# Verify WAL level is set to logical replication
docker exec -it postgres psql -U postgres -d northwind -c "SHOW wal_level;"

# Check recent activity (sample query)
docker exec -it postgres psql -U postgres -d northwind -c "SELECT COUNT(*) FROM orders;"
docker exec -it postgres psql -U postgres -d northwind -c "SELECT MAX(order_id) FROM orders;"
```

**Expected Results:**
- Tables visible: `orders`, `order_details`, `customers`, `products`, `employees`, etc.
- Replication slot `debezium` exists with `active = true`
- `wal_level = logical` (required for CDC)
- Row counts > 0 for key tables

#### 1.3 Publication Status (for Debezium)
```powershell
# Check PostgreSQL publication (Debezium uses filtered publication)
docker exec -it postgres psql -U postgres -d northwind -c "SELECT * FROM pg_publication;"
docker exec -it postgres psql -U postgres -d northwind -c "SELECT * FROM pg_publication_tables WHERE pubname LIKE '%northwind%';"
```

**Expected Results:**
- Publication exists for northwind tables
- Tables are included in publication

---

### Stage 2: CDC Connector (Debezium) - INGESTION

#### 2.1 Container Health
```powershell
# Check Debezium Connect container
docker ps --filter "name=debezium" --format "table {{.Names}}\t{{.Status}}"

# Check logs for connector registration and errors
docker logs debezium-connect --tail 200

# Check register-connector container (one-time setup)
docker logs <register-connector-container-id> --tail 50
```

**Expected Results:**
- Container status: `Up`
- No ERROR or EXCEPTION messages
- Connector registration successful (check for `201 Created` or `200 OK`)

#### 2.2 Connector Status via REST API
```powershell
# Note: Port 8083 is commented out in docker-compose, so use exec
docker exec debezium-connect curl -s http://localhost:8083/connectors

# Check specific connector status
docker exec debezium-connect curl -s http://localhost:8083/connectors/postgres-northwind-connector/status | ConvertFrom-Json | ConvertTo-Json -Depth 10

# Check connector configuration
docker exec debezium-connect curl -s http://localhost:8083/connectors/postgres-northwind-connector/config | ConvertFrom-Json | ConvertTo-Json -Depth 10
```

**Expected Results:**
- Connector `postgres-northwind-connector` exists
- Status: `"state": "RUNNING"` (not `FAILED` or `PAUSED`)
- Task state: `"state": "RUNNING"`
- No error messages in status

#### 2.3 Replication Slot Monitoring
```powershell
# Monitor replication slot lag (should be minimal)
docker exec -it postgres psql -U postgres -d northwind -c "
SELECT 
    slot_name,
    active,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) as lag_size,
    confirmed_flush_lsn
FROM pg_replication_slots 
WHERE slot_name = 'debezium';"
```

**Expected Results:**
- `active = true`
- Lag size should be small (< 1MB ideally, < 100MB acceptable)
- `confirmed_flush_lsn` is advancing

---

### Stage 3: Kafka Message Broker - INGESTION/TRANSFORMATION

#### 3.1 Container Health
```powershell
# Check Kafka and Zookeeper containers
docker ps --filter "name=kafka" --format "table {{.Names}}\t{{.Status}}"
docker ps --filter "name=zookeeper" --format "table {{.Names}}\t{{.Status}}"

# Check Kafka logs
docker logs kafka --tail 100
docker logs zookeeper --tail 50
```

**Expected Results:**
- Both containers: `Up`
- No ERROR or EXCEPTION messages
- Kafka broker started successfully

#### 3.2 Kafka Topics Validation
```powershell
# List all topics (should include northwind.* topics)
docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list

# Check topic details for a sample topic
docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic northwind.public.orders

# Check consumer groups (Spark jobs should be consuming)
docker exec kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
```

**Expected Results:**
- Topics exist: `northwind.public.orders`, `northwind.public.order_details`, `northwind.public.customers`, etc.
- Topic partitions > 0
- Replication factor = 1 (as configured)
- Consumer groups exist for Spark jobs

#### 3.3 Message Flow Validation
```powershell
# Check message count in a topic (sample)
docker exec kafka kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic northwind.public.orders --time -1

# Monitor real-time message consumption (run for 30 seconds)
docker exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic northwind.public.orders --from-beginning --max-messages 5

# Check consumer lag (if consumer groups exist)
docker exec kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group <spark-consumer-group> --all-topics
```

**Expected Results:**
- Message offsets increasing over time
- Messages are readable JSON (Debezium format)
- Consumer lag is minimal (< 1000 messages)

#### 3.4 Kafka Connect Internal Topics
```powershell
# Check Connect internal topics
docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list | Select-String "connect"

# Verify offsets are being committed
docker exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic connect-offsets --from-beginning --max-messages 10
```

**Expected Results:**
- Topics exist: `connect-configs`, `connect-offsets`, `connect-statuses`
- Offsets are being committed regularly

---

### Stage 4: Spark Processing - TRANSFORMATION

#### 4.1 Container Health
```powershell
# Check all Spark containers
docker ps --filter "name=spark" --format "table {{.Names}}\t{{.Status}}"

# Check Spark Master
docker logs spark-master --tail 100

# Check Spark Worker
docker logs spark-worker --tail 100

# Check Spark jobs
docker logs pyspark-job-cdc --tail 200
docker logs pyspark-job-dw --tail 200
```

**Expected Results:**
- All containers: `Up`
- Spark Master: Web UI accessible (if port exposed)
- Spark Worker: Connected to master
- Job containers: Running without exceptions

#### 4.2 Spark Job Status
```powershell
# Check if jobs are actively running
docker exec spark-master ps aux | Select-String "spark-submit"

# Check Spark application status (if UI port is exposed)
# Otherwise, check logs for "Streaming query" status
docker logs pyspark-job-cdc --tail 50 | Select-String -Pattern "Streaming|Batch|ERROR|Exception"

# Check checkpoint locations (should exist if streaming is working)
docker exec pyspark-job-cdc ls -la /tmp/spark_checkpoints/ 2>$null
```

**Expected Results:**
- Spark-submit processes running
- Logs show: `Streaming query made progress` or batch processing messages
- No `StreamingQueryException` or `AnalysisException`
- Checkpoint directories exist

#### 4.3 Spark Streaming Metrics
```powershell
# Monitor CDC job logs for batch processing
docker logs pyspark-job-cdc --tail 100 | Select-String -Pattern "Batch|rows|updated"

# Monitor DW ETL job logs
docker logs pyspark-job-dw --tail 100 | Select-String -Pattern "wrote|rows|DimCustomer|FactOrders"
```

**Expected Results:**
- CDC job: Regular batch messages with row counts
- DW job: Processing messages for dimensions and facts
- No repeated error patterns

#### 4.4 Spark-Kafka Integration
```powershell
# Verify Spark can connect to Kafka (check logs)
docker logs pyspark-job-cdc | Select-String -Pattern "kafka|Kafka|bootstrap"

# Check for connection errors
docker logs pyspark-job-cdc | Select-String -Pattern "Connection|Timeout|Failed"
```

**Expected Results:**
- No connection errors
- Successful Kafka consumer initialization

---

### Stage 5: Data Warehouse (ClickHouse) - LOADING

#### 5.1 Container Health
```powershell
# Check ClickHouse containers
docker ps --filter "name=clickhouse" --format "table {{.Names}}\t{{.Status}}"

# Check ClickHouse logs
docker logs clickhouse1 --tail 100
docker logs clickhouse2 --tail 100

# Check ClickHouse Zookeeper
docker logs clickhouse-zookeeper --tail 50
```

**Expected Results:**
- All containers: `Up`
- No ERROR or EXCEPTION messages
- Replication working (if configured)

#### 5.2 ClickHouse Connectivity
```powershell
# Test connection to ClickHouse
docker exec clickhouse1 clickhouse-client --query "SELECT version();"

# Check database exists
docker exec clickhouse1 clickhouse-client --query "SHOW DATABASES;"

# Verify tables exist
docker exec clickhouse1 clickhouse-client --query "SHOW TABLES FROM northwind;"
```

**Expected Results:**
- Connection successful
- Database `northwind` exists
- Tables exist: `northwind_orders`, `northwind_order_details`, `northwind_customers`, etc.

#### 5.3 Staging Tables Data Validation
```powershell
# Check row counts in staging tables
docker exec clickhouse1 clickhouse-client --query "SELECT 'orders' as table_name, count(*) as row_count FROM northwind.northwind_orders UNION ALL SELECT 'order_details', count(*) FROM northwind.northwind_order_details UNION ALL SELECT 'customers', count(*) FROM northwind.northwind_customers UNION ALL SELECT 'products', count(*) FROM northwind.northwind_products;"

# Check recent updates (should have recent updatedate)
docker exec clickhouse1 clickhouse-client --query "SELECT MAX(updatedate) as latest_update FROM northwind.northwind_orders;"

# Check for data freshness (compare with current time)
docker exec clickhouse1 clickhouse-client --query "SELECT MAX(updatedate) as latest, now() as current_time, dateDiff('second', MAX(updatedate), now()) as seconds_ago FROM northwind.northwind_orders;"
```

**Expected Results:**
- Row counts > 0 (should match or be close to PostgreSQL source)
- Latest `updatedate` is recent (< 5 minutes for active pipeline)
- Data is flowing from Spark CDC job

#### 5.4 Data Warehouse Tables Validation
```powershell
# Check dimension tables
docker exec clickhouse1 clickhouse-client --query "SELECT 'DimCustomer' as table_name, count(*) as row_count FROM DimCustomer UNION ALL SELECT 'DimProducts', count(*) FROM DimProducts UNION ALL SELECT 'DimEmployees', count(*) FROM DimEmployees UNION ALL SELECT 'DimSuppliers', count(*) FROM DimSuppliers UNION ALL SELECT 'DimShippers', count(*) FROM DimShippers;"

# Check fact table
docker exec clickhouse1 clickhouse-client --query "SELECT count(*) as fact_rows FROM FactOrders;"

# Check geography dimension
docker exec clickhouse1 clickhouse-client --query "SELECT count(*) as geo_rows FROM DimGeography;"
```

**Expected Results:**
- Dimension tables have data
- Fact table has rows
- Row counts are reasonable (dimensions < facts typically)

#### 5.5 Data Quality Checks
```powershell
# Check for null keys in fact table (should be minimal)
docker exec clickhouse1 clickhouse-client --query "SELECT count(*) as null_customer_key FROM FactOrders WHERE CustomerKey = 0 OR CustomerKey IS NULL;"

# Check referential integrity (sample)
docker exec clickhouse1 clickhouse-client --query "
SELECT 
    COUNT(DISTINCT f.CustomerKey) as fact_customers,
    COUNT(DISTINCT d.CustomerKey) as dim_customers,
    COUNT(DISTINCT f.CustomerKey) - COUNT(DISTINCT d.CustomerKey) as orphaned_keys
FROM FactOrders f
LEFT JOIN DimCustomer d ON f.CustomerKey = d.CustomerKey;"

# Check for duplicate orders (should be handled by ReplacingMergeTree)
docker exec clickhouse1 clickhouse-client --query "SELECT OrderAlternateKey, count(*) as cnt FROM FactOrders GROUP BY OrderAlternateKey HAVING cnt > 1 LIMIT 10;"
```

**Expected Results:**
- Minimal null keys (0 or very few)
- Orphaned keys = 0 (all fact keys exist in dimensions)
- Duplicates handled by table engine

#### 5.6 Replication Status (if applicable)
```powershell
# Check replication status
docker exec clickhouse1 clickhouse-client --query "SELECT * FROM system.replicas WHERE database = 'northwind';"

# Check for replication errors
docker exec clickhouse1 clickhouse-client --query "SELECT * FROM system.replication_queue WHERE database = 'northwind' LIMIT 10;"
```

**Expected Results:**
- Replicas are in sync
- No errors in replication queue

---

## 2. COMMANDS & LOG LOCATIONS

### Container Health Commands
```powershell
# All containers status
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# Container resource usage
docker stats --no-stream

# Network connectivity test
docker network inspect ProjectHost
```

### Log Locations & Commands

#### PostgreSQL Logs
```powershell
# Real-time logs
docker logs -f postgres

# Last 100 lines
docker logs postgres --tail 100

# Log location in container: /var/lib/postgresql/data/log/
```

#### Debezium Logs
```powershell
# Real-time logs
docker logs -f debezium-connect

# Last 200 lines (most relevant)
docker logs debezium-connect --tail 200

# Connector registration logs
docker ps -a --filter "name=register" --format "{{.ID}}" | ForEach-Object { docker logs $_ }
```

#### Kafka Logs
```powershell
# Kafka broker logs
docker logs -f kafka

# Zookeeper logs
docker logs -f zookeeper

# Last 100 lines
docker logs kafka --tail 100
```

#### Spark Logs
```powershell
# Master logs
docker logs -f spark-master

# Worker logs
docker logs -f spark-worker

# CDC job logs (most critical)
docker logs -f pyspark-job-cdc

# DW ETL job logs
docker logs -f pyspark-job-dw

# Search for errors across all Spark containers
docker logs pyspark-job-cdc 2>&1 | Select-String -Pattern "ERROR|Exception|Failed"
docker logs pyspark-job-dw 2>&1 | Select-String -Pattern "ERROR|Exception|Failed"
```

#### ClickHouse Logs
```powershell
# ClickHouse server logs
docker logs -f clickhouse1
docker logs -f clickhouse2

# Query logs (if enabled)
docker exec clickhouse1 clickhouse-client --query "SELECT * FROM system.query_log WHERE type = 'QueryFinish' ORDER BY event_time DESC LIMIT 10;"

# Error logs
docker exec clickhouse1 clickhouse-client --query "SELECT * FROM system.query_log WHERE exception != '' ORDER BY event_time DESC LIMIT 10;"
```

### Job Schedule Inspection

#### Spark Job Execution
```powershell
# Check if jobs are running (process check)
docker exec spark-master ps aux | Select-String "spark-submit"

# Check job start time
docker inspect pyspark-job-cdc --format "{{.State.StartedAt}}"
docker inspect pyspark-job-dw --format "{{.State.StartedAt}}"

# Check job restart count (indicates failures)
docker inspect pyspark-job-cdc --format "{{.RestartCount}}"
docker inspect pyspark-job-dw --format "{{.RestartCount}}"
```

#### ETL Schedule (test.py)
- **Schedule Type:** Continuous loop with 20-second intervals (`POLL_INTERVAL_SEC = 20`)
- **Check:** Look for `last_run.txt` file updates in container
```powershell
# Check if last_run file exists and is being updated
docker exec pyspark-job-dw ls -la /opt/bitnami/spark/scripts/last_run.txt 2>$null
docker exec pyspark-job-dw cat /opt/bitnami/spark/scripts/last_run.txt 2>$null
```

---

## 3. KEY METRICS & SUCCESS INDICATORS

### Ingestion Metrics

#### PostgreSQL → Debezium
- **Replication Slot Lag:** < 1MB (ideal), < 100MB (acceptable)
- **Connector State:** `RUNNING` (not `FAILED` or `PAUSED`)
- **Task State:** `RUNNING` with no errors
- **WAL Level:** `logical` (required)

#### Debezium → Kafka
- **Message Production Rate:** Messages appearing in topics within seconds of PostgreSQL changes
- **Topic Message Count:** Increasing over time
- **Consumer Lag:** < 1000 messages (for Spark consumers)

### Transformation Metrics

#### Kafka → Spark (CDC Job)
- **Batch Processing Frequency:** Regular batches (every few seconds to minutes)
- **Rows Processed per Batch:** > 0 when source data changes
- **Stream Status:** `ACTIVE` (check logs for "Streaming query made progress")
- **Checkpoint Progress:** Checkpoint files updating in `/tmp/spark_checkpoints/`

#### Spark Staging → DW (ETL Job)
- **ETL Execution Frequency:** Every 20 seconds (as configured in test.py)
- **Dimension Processing:** Logs show "wrote X rows" for each dimension
- **Fact Processing:** Logs show "FactOrders: wrote X rows"
- **Last Run Timestamp:** `last_run.txt` file updating regularly

### Loading Metrics

#### ClickHouse Staging Tables
- **Data Freshness:** `MAX(updatedate)` within last 5 minutes (for active pipeline)
- **Row Count Growth:** Increasing over time
- **Data Completeness:** Staging table counts should match or be close to source PostgreSQL tables

#### ClickHouse DW Tables
- **Dimension Completeness:** All expected dimensions populated (Customer, Product, Employee, Supplier, Shipper, Geography)
- **Fact Table Growth:** FactOrders row count increasing
- **Referential Integrity:** No orphaned keys (all fact foreign keys exist in dimensions)
- **Data Quality:** Minimal null values in key columns

### End-to-End Latency
- **Source to Staging:** < 1 minute (from PostgreSQL INSERT to ClickHouse staging)
- **Staging to DW:** < 1 minute (from staging update to DW update, depends on ETL schedule)

### Overall Health Indicators
✅ **Pipeline Healthy When:**
1. All containers are `Up` and not restarting
2. Debezium connector state is `RUNNING`
3. Kafka topics have messages and consumer lag is low
4. Spark jobs show active batch processing in logs
5. ClickHouse staging tables have recent `updatedate` timestamps
6. ClickHouse DW tables are populated and growing
7. No repeated errors in any component logs
8. Resource usage (CPU, memory) is within acceptable limits

---

## 4. POSSIBLE FAILURE POINTS

### Stage 1: PostgreSQL Issues

#### Failure: Replication Slot Not Created
- **Symptoms:** Debezium connector fails with "replication slot does not exist"
- **Check:** `SELECT * FROM pg_replication_slots WHERE slot_name = 'debezium';`
- **Impact:** CDC cannot capture changes

#### Failure: WAL Level Incorrect
- **Symptoms:** Debezium cannot start, errors about logical replication
- **Check:** `SHOW wal_level;` (must be `logical`)
- **Impact:** CDC completely blocked

#### Failure: Database Connection Issues
- **Symptoms:** Debezium connector shows connection errors
- **Check:** Network connectivity, credentials, port accessibility
- **Impact:** No data capture

#### Failure: Publication Missing Tables
- **Symptoms:** Some tables not appearing in Kafka topics
- **Check:** `SELECT * FROM pg_publication_tables;`
- **Impact:** Partial data loss

### Stage 2: Debezium Issues

#### Failure: Connector Not Registered
- **Symptoms:** Connector not in list, registration container failed
- **Check:** `curl http://localhost:8083/connectors`
- **Impact:** No CDC capture

#### Failure: Connector in FAILED State
- **Symptoms:** Status shows `"state": "FAILED"`
- **Check:** Connector status endpoint, logs for specific error
- **Impact:** Pipeline stopped

#### Failure: Task Errors
- **Symptoms:** Task state shows errors, exception messages in logs
- **Check:** Task status, Debezium logs
- **Impact:** Partial or complete data loss

#### Failure: Replication Slot Lag Growing
- **Symptoms:** Lag size increasing, not being consumed
- **Check:** `pg_wal_lsn_diff()` query
- **Impact:** Risk of WAL overflow, potential data loss

### Stage 3: Kafka Issues

#### Failure: Broker Not Available
- **Symptoms:** Spark jobs fail to connect, Debezium cannot produce
- **Check:** Container status, port accessibility, logs
- **Impact:** Complete pipeline failure

#### Failure: Topic Not Created
- **Symptoms:** Topics missing, Spark jobs fail on subscribe
- **Check:** `kafka-topics.sh --list`
- **Impact:** Data not reaching Spark

#### Failure: Consumer Lag Growing
- **Symptoms:** Spark not consuming fast enough, lag increasing
- **Check:** `kafka-consumer-groups.sh --describe`
- **Impact:** Stale data, potential memory issues

#### Failure: Message Format Errors
- **Symptoms:** Spark parsing errors, malformed JSON
- **Check:** Sample messages from topic, schema compatibility
- **Impact:** Data loss or transformation errors

### Stage 4: Spark Issues

#### Failure: Job Container Crashed
- **Symptoms:** Container status `Exited`, restart count increasing
- **Check:** Container logs, exit codes
- **Impact:** Pipeline stopped

#### Failure: Streaming Query Stopped
- **Symptoms:** Logs show "Streaming query terminated", no batch messages
- **Check:** `stream.isActive` status, checkpoint errors
- **Impact:** No data flowing to ClickHouse

#### Failure: Checkpoint Corruption
- **Symptoms:** Errors about checkpoint, streaming cannot resume
- **Check:** Checkpoint directory contents, permissions
- **Impact:** Data loss or duplicate processing

#### Failure: Memory/Resource Exhaustion
- **Symptoms:** OOM errors, job killed, slow processing
- **Check:** Container stats, Spark executor memory settings
- **Impact:** Job failures, pipeline instability

#### Failure: Kafka Connection Timeout
- **Symptoms:** Connection errors, timeouts in logs
- **Check:** Network connectivity, Kafka broker status
- **Impact:** No data consumption

#### Failure: ClickHouse Write Failures
- **Symptoms:** JDBC errors, write exceptions in logs
- **Check:** ClickHouse connectivity, table schema compatibility
- **Impact:** Data not loaded to warehouse

### Stage 5: ClickHouse Issues

#### Failure: Container Not Accessible
- **Symptoms:** Spark jobs fail on JDBC connection
- **Check:** Container status, port accessibility, network
- **Impact:** No data loading

#### Failure: Table Schema Mismatch
- **Symptoms:** Insert errors, column type mismatches
- **Check:** Table schemas, Spark DataFrame schemas
- **Impact:** Data load failures

#### Failure: Replication Not Working
- **Symptoms:** Data only in one replica, replication errors
- **Check:** Replication status, Zookeeper connectivity
- **Impact:** Data inconsistency, potential data loss

#### Failure: Disk Space Full
- **Symptoms:** Write errors, "No space left" messages
- **Check:** Disk usage, ClickHouse data directory
- **Impact:** Complete pipeline failure

#### Failure: Table Engine Issues
- **Symptoms:** Duplicate rows, merge not happening
- **Check:** Table engine type (ReplacingMergeTree), merge operations
- **Impact:** Data quality issues

### Cross-Stage Failures

#### Failure: Network Connectivity
- **Symptoms:** Containers cannot communicate, connection refused
- **Check:** Docker network `ProjectHost`, DNS resolution
- **Impact:** Complete pipeline failure

#### Failure: Resource Exhaustion (Host)
- **Symptoms:** Containers killed, OOM, high CPU
- **Check:** Host resources, container limits
- **Impact:** Unstable pipeline, frequent failures

#### Failure: Time Synchronization
- **Symptoms:** Timestamp issues, data ordering problems
- **Check:** Container time, host time sync
- **Impact:** Data quality, ordering issues

---

## 5. RECOMMENDED ACTIONS IF ISSUES DETECTED

### Immediate Actions (Without Code Changes)

#### If Debezium Connector is FAILED:
1. **Check Connector Status:**
   ```powershell
   docker exec debezium-connect curl -s http://localhost:8083/connectors/postgres-northwind-connector/status
   ```
2. **Review Error Details:** Check the `trace` field in status response
3. **Check PostgreSQL:** Verify replication slot exists and WAL level
4. **Restart Connector (if safe):**
   ```powershell
   docker exec debezium-connect curl -X POST http://localhost:8083/connectors/postgres-northwind-connector/restart
   ```
5. **Monitor Logs:** Watch for recurring errors

#### If Kafka Consumer Lag is High:
1. **Identify Lagging Consumer:**
   ```powershell
   docker exec kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups
   ```
2. **Check Spark Job Status:** Verify Spark jobs are running and processing
3. **Review Resource Usage:** Check if Spark has sufficient memory/CPU
4. **Check Network:** Verify Spark can reach Kafka broker
5. **Monitor Progress:** Watch lag decrease over time

#### If Spark Job Container Crashed:
1. **Check Exit Code:**
   ```powershell
   docker inspect pyspark-job-cdc --format "{{.State.ExitCode}}"
   ```
2. **Review Logs:** Check last 200 lines for error messages
3. **Check Dependencies:** Verify Kafka and ClickHouse are accessible
4. **Check Checkpoints:** Verify checkpoint directory is not corrupted
5. **Restart Container (if needed):**
   ```powershell
   docker restart pyspark-job-cdc
   docker restart pyspark-job-dw
   ```

#### If ClickHouse Tables Have No Recent Data:
1. **Check Data Freshness:**
   ```powershell
   docker exec clickhouse1 clickhouse-client --query "SELECT MAX(updatedate), now() FROM northwind.northwind_orders;"
   ```
2. **Verify Spark Jobs:** Check if CDC job is writing to ClickHouse
3. **Check Connection:** Test Spark → ClickHouse connectivity
4. **Review Table Schema:** Ensure schema matches Spark output
5. **Check for Errors:** Review ClickHouse logs for insert failures

#### If Data Quality Issues Detected:
1. **Orphaned Keys:** Check if dimension tables are populated before fact loads
2. **Null Values:** Verify source data quality, check transformation logic in logs
3. **Duplicates:** Check if ReplacingMergeTree is merging (may need manual OPTIMIZE)
4. **Count Mismatches:** Compare source PostgreSQL counts with ClickHouse staging

### Diagnostic Queries for Troubleshooting

#### End-to-End Data Flow Check:
```powershell
# 1. Source count
docker exec postgres psql -U postgres -d northwind -c "SELECT COUNT(*) FROM orders;"

# 2. Kafka message count (approximate)
docker exec kafka kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic northwind.public.orders --time -1

# 3. ClickHouse staging count
docker exec clickhouse1 clickhouse-client --query "SELECT COUNT(*) FROM northwind.northwind_orders;"

# 4. ClickHouse DW count
docker exec clickhouse1 clickhouse-client --query "SELECT COUNT(*) FROM FactOrders;"
```

#### Component Health Summary:
```powershell
# Create a health check script
@"
# Container Status
docker ps --format 'table {{.Names}}\t{{.Status}}'

# Debezium Connector
docker exec debezium-connect curl -s http://localhost:8083/connectors/postgres-northwind-connector/status | ConvertFrom-Json | Select-Object -ExpandProperty connector | Select-Object state

# Kafka Topics
docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list

# ClickHouse Data Freshness
docker exec clickhouse1 clickhouse-client --query "SELECT 'orders' as table, MAX(updatedate) as latest FROM northwind.northwind_orders UNION ALL SELECT 'FactOrders', MAX(updatedate) FROM FactOrders;"
"@ | Out-File -FilePath health_check.ps1
```

### Monitoring Recommendations

1. **Set Up Log Aggregation:** Use `docker logs` with filtering to monitor errors across all containers
2. **Create Health Check Script:** Automate the diagnostic queries above
3. **Monitor Resource Usage:** Use `docker stats` to track CPU/memory
4. **Track Key Metrics:** 
   - Replication slot lag
   - Kafka consumer lag
   - ClickHouse data freshness
   - Spark job batch processing frequency
5. **Alert on Thresholds:**
   - Replication lag > 100MB
   - Consumer lag > 10,000 messages
   - Data freshness > 10 minutes
   - Container restart count > 0

### Recovery Procedures

#### If Pipeline Completely Stopped:
1. **Verify All Containers Running:** `docker ps`
2. **Check Network:** `docker network inspect ProjectHost`
3. **Start in Order:**
   - PostgreSQL
   - Zookeeper
   - Kafka
   - Debezium
   - ClickHouse
   - Spark (master, worker, jobs)
4. **Verify Connector:** Check Debezium connector is registered and running
5. **Monitor Initial Data Flow:** Watch logs for first batches

#### If Data Needs to be Reprocessed:
1. **Note:** This may require code changes (resetting offsets), but for diagnosis:
2. **Check Kafka Offsets:** Current consumer group positions
3. **Check Spark Checkpoints:** Current processing position
4. **Verify Source Data:** Ensure PostgreSQL has the expected data
5. **Monitor Recovery:** Watch for data flowing through all stages

---

## Summary

This diagnostic checklist provides a comprehensive approach to validate each stage of your CDC pipeline from PostgreSQL through ClickHouse. Regular execution of these checks will help identify issues early and maintain pipeline health.

**Priority Checks (Daily):**
- Container status
- Debezium connector state
- Spark job logs for errors
- ClickHouse data freshness

**Detailed Checks (Weekly):**
- End-to-end data flow validation
- Data quality checks
- Resource usage analysis
- Replication and consumer lag monitoring

