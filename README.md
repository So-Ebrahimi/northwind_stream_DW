# Northwind Stream Data Warehouse

A real-time data pipeline that streams data from PostgreSQL (Northwind OLTP database) to ClickHouse data warehouse using Change Data Capture (CDC) technology.

## Architecture

```
PostgreSQL (OLTP) → Debezium (CDC) → Kafka → Spark → ClickHouse (DW)
```

### Pipeline Flow

1. **PostgreSQL**: Source OLTP database containing the Northwind sample database
2. **Debezium**: Captures database changes using PostgreSQL logical replication
3. **Kafka**: Message broker that stores CDC events as topics
4. **Spark**: Processes streaming data and performs ETL transformations
   - **CDC Streaming Job**: Streams data from Kafka to ClickHouse staging tables
   - **DW ETL Job**: Builds star schema dimensions and facts from staging tables
5. **ClickHouse**: Data warehouse with star schema design for analytical queries

## Technology Stack

- **PostgreSQL 18**: Source database (OLTP)
- **Debezium**: Change Data Capture connector
- **Apache Kafka**: Distributed streaming platform
- **Apache Spark**: Distributed data processing engine
- **ClickHouse**: Column-oriented database for analytics (with replication)

## Prerequisites

- Docker and Docker Compose installed
- At least 8GB of available RAM
- Windows PowerShell (for Windows) or Bash (for Linux/Mac)

## Quick Start

### 1. Create Docker Network

```powershell
docker network create ProjectHost
```

### 2. Start Services (in order)

```powershell
# Start PostgreSQL with Northwind database
docker-compose -f ./1-postgres/docker-compose.yml up -d --force-recreate

# Start Kafka and Zookeeper
docker-compose -f ./3-kafka/docker-compose.yml up -d --force-recreate

# Start Debezium Connect
docker-compose -f ./4-debezium/docker-compose.yml up -d --force-recreate

# Start ClickHouse (2 replicas)
docker-compose -f ./5-clickhouse/docker-compose.yml up -d --force-recreate

# Start Spark (Master, Worker, and ETL jobs)
docker-compose -f ./6-spark/docker-compose.yml up -d --force-recreate

# Start grafana (Dashboard)
docker-compose -f ./7-grafana/docker-compose.yml up -d --force-recreate
```

### 3. Verify Pipeline Status

Check container health:
```powershell
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

All containers should show status `Up` (healthy).

## Project Structure

```
northwind_stream_DW/
├── 0-info/                   # Documentation and diagrams
├── 1-postgres/               # PostgreSQL setup with Northwind database
├── 2-zookeeper/              # Zookeeper configuration
├── 3-kafka/                  # Kafka broker setup
├── 4-debezium/               # Debezium Connect CDC connector
├── 5-clickhouse/             # ClickHouse data warehouse (2 replicas)
│   └── init-db/
│       └── init.sql          # Star schema table definitions
├── 6-spark/                  # Spark cluster and ETL jobs
│   └── scripts/
│       ├── northwind-ch-stg.py    # CDC streaming job
│       ├── northwind-dw.py        # Star schema builder (incremental processing)
│       └── norhwind_schemas.py    # Debezium schema definitions
├── checks.md                 # Pipeline diagnostic checklist
├── pipeline_review_report.md # Comprehensive architecture review
└── test_data_verification_summary.md # Test data validation results
```

## Components

### PostgreSQL (Port 15432)
- Source OLTP database with Northwind sample data
- Configured with logical replication for CDC
- Replication slot: `debezium`

### Debezium Connect
- Captures INSERT, UPDATE, DELETE operations
- Publishes changes to Kafka topics
- Topic naming: `northwind.public.<table_name>`

### Kafka
- Ports: 39092, 29092
- Stores CDC events as topics
- Topics are consumed by Spark streaming jobs

### Spark Cluster
- **Master**: Coordinates jobs
- **Worker**: Executes tasks
- **pyspark-job-cdc**: Streams CDC data from Kafka to ClickHouse staging
- **pyspark-job-dw**: Incremental ETL from staging to data warehouse

### ClickHouse (Ports 18123, 28123)
- **Staging Tables**: `northwind.*` (raw CDC data)
- **Data Warehouse**: Star schema with dimensions and facts
  - **Dimensions**: DimGeography, DimCustomer, DimEmployees, DimSuppliers, DimProducts, DimShippers, DimTerritories, DimDate
  - **Facts**: FactOrders, FactEmployeeTerritories

## Data Warehouse Schema

The data warehouse implements a **star schema** design:

- **Dimension Tables**: Store descriptive attributes (customers, products, employees, etc.)
- **Fact Tables**: Store business metrics and transactions (orders, employee territories)
- **Date Dimension**: Pre-populated date dimension (1970-2050)

For detailed schema information, see `pipeline_review_report.md`.

## Verification

### Check Debezium Connector Status
```powershell
docker exec debezium-connect curl -s http://localhost:8083/connectors/postgres-northwind-connector/status
```

### Check Kafka Topics
```powershell
docker exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092
```

### Query ClickHouse Staging
```powershell
docker exec clickhouse1 clickhouse-client --query "SELECT COUNT(*) FROM northwind.northwind_orders"
```

### Query Data Warehouse
```powershell
docker exec clickhouse1 clickhouse-client --query "SELECT COUNT(*) FROM FactOrders"
```

For comprehensive verification steps, see `checks.md`.

## Monitoring

### View Spark Job Logs
```powershell
# CDC streaming job
docker logs pyspark-job-cdc --tail 50

# DW ETL job
docker logs pyspark-job-dw --tail 50
```

### Check Container Health
```powershell
docker ps --filter "name=postgres|kafka|debezium|clickhouse|spark" --format "table {{.Names}}\t{{.Status}}"
```

## Documentation

- **[Pipeline Review Report](pipeline_review_report.md)**: Comprehensive architecture review, code analysis, and recommendations
- **[Diagnostic Checklist](checks.md)**: Step-by-step validation procedures for each pipeline component
- **[Test Data Verification](test_data_verification_summary.md)**: Test data insertion and pipeline validation results

## Stopping the Pipeline

To stop all services:
```powershell
docker-compose -f ./6-spark/docker-compose.yml down
docker-compose -f ./5-clickhouse/docker-compose.yml down
docker-compose -f ./4-debezium/docker-compose.yml down
docker-compose -f ./3-kafka/docker-compose.yml down
docker-compose -f ./1-postgres/docker-compose.yml down
```

## Notes

- The pipeline processes changes in real-time using CDC technology
- Spark jobs use checkpointing for fault tolerance
- ClickHouse uses ReplacingMergeTree for handling updates
- The ETL job runs incrementally, processing only changed records based on `updatedate`

## License

See [LICENSE](LICENSE) file for details.

## Todo List

### Code Quality
- [ ] Standardize date handling: Review and fix date transformation logic in `northwind-ch-stg.py` - currently only handles employees and orders, should handle all date columns consistently
- [ ] Fix SupplierKey mapping: In `northwind-dw.py` `process_dim_products()`, SupplierKey is set to NULL - need to properly map supplier_id to SupplierKey from DimSuppliers
- [ ] Add error handling: Implement comprehensive try-catch blocks and error logging in both Spark jobs (`northwind-ch-stg.py` and `northwind-dw.py`)
- [ ] Fix employee_id type mismatch: In `northwind-dw.py` `process_fact_orders()`, orders.employee_id is String but DimEmployees.EmployeeAlternateKey expects Integer - add type conversion
- [ ] Remove hardcoded credentials: Move ClickHouse credentials (user, password) to environment variables or secrets management
- [ ] Add data validation: Implement data quality checks before writing to ClickHouse (null checks, type validation, referential integrity)
- [ ] Fix last_run file persistence: In `northwind-dw.py`, `last_run.txt` is stored locally - should use shared storage or database for multi-instance deployments

### Architecture
- [ ] Implement FactEmployeeTerritories: Create ETL process for FactEmployeeTerritories fact table (currently missing from `northwind-dw.py`)
- [ ] Add DimTerritories dimension: Implement DimTerritories dimension table processing in ETL pipeline
- [ ] Handle DELETE operations: Review CDC delete handling - currently using ReplacingMergeTree but need to verify DELETE operations are properly processed
- [ ] Implement proper CDC operation handling: In `northwind-ch-stg.py`, operation field is captured but not used - need to handle INSERT/UPDATE/DELETE differently

### Monitoring
- [ ] Add monitoring and alerting: Implement health checks, metrics collection, and alerting for Spark jobs, Kafka topics, and ClickHouse
- [ ] Add data quality monitoring: Implement checks for data freshness, record counts, and data quality metrics
- [ ] Add logging improvements: Enhance logging with structured logging, log levels, and log aggregation (ELK stack or similar)

### Performance
- [ ] Optimize Spark checkpointing: Review checkpoint locations and ensure they are on persistent storage, not ephemeral
- [ ] Optimize ClickHouse writes: Review batch sizes and write strategies for better performance in `northwind-ch-stg.py`
- [ ] Add connection pooling: Implement connection pooling for ClickHouse JDBC connections in Spark jobs
- [ ] Optimize incremental ETL: Review and optimize the incremental ETL logic in `northwind-dw.py` for better performance with large datasets

### Testing
- [ ] Add unit tests: Create unit tests for ETL transformations, data validation functions, and utility functions
- [ ] Add integration tests: Create integration tests for end-to-end pipeline validation (PostgreSQL → ClickHouse)
- [ ] Add data validation tests: Create tests to verify data consistency between source and target, referential integrity checks

### Documentation
- [ ] Update README: Remove references to deleted files (`northwind-etl-dw.py`, `test.py`, `checks.md`, `pipeline_review_report.md`, `test_data_verification_summary.md`)
- [ ] Add architecture diagrams: Create detailed architecture diagrams showing data flow, components, and interactions
- [ ] Add data dictionary: Document all dimension and fact tables, their schemas, and relationships
- [ ] Add deployment guide: Create detailed deployment guide with prerequisites, step-by-step instructions, and troubleshooting
- [ ] Add runbook: Create operational runbook with common issues, recovery procedures, and maintenance tasks

### Deployment
- [ ] Add health checks: Implement Docker health checks for all containers in docker-compose files
- [ ] Add resource limits: Set appropriate CPU and memory limits for all containers in docker-compose files
- [ ] Add restart policies: Configure restart policies for all containers to ensure high availability
- [ ] Add environment variable support: Replace hardcoded values with environment variables in docker-compose files
- [ ] Create startup script: Create a single script to start all services in correct order with dependency checks
- [ ] Add graceful shutdown: Implement graceful shutdown procedures for Spark streaming jobs

### Security
- [ ] Secure credentials: Move all passwords and sensitive data to environment variables or secrets management
- [ ] Add network security: Review and restrict network access between containers using Docker networks
- [ ] Add SSL/TLS: Implement SSL/TLS for Kafka, ClickHouse, and PostgreSQL connections

### Maintenance
- [ ] Add data retention policies: Implement TTL and data retention policies for staging tables in ClickHouse
- [ ] Add cleanup scripts: Create scripts for cleaning up old checkpoints, logs, and temporary files
- [ ] Add backup procedures: Implement backup and restore procedures for ClickHouse data warehouse
- [ ] Add maintenance scripts: Create scripts for common maintenance tasks (vacuum, optimize, etc.)

### Schema
- [ ] Review ClickHouse schema: Ensure all staging tables use ReplacingMergeTree with updatedate for proper CDC handling
- [ ] Add missing indexes: Review and add appropriate indexes/ORDER BY clauses for better query performance
- [ ] Fix schema inconsistencies: Review data type mismatches between staging tables and dimension/fact tables
- [ ] Add constraints: Add CHECK constraints where appropriate for data validation at database level