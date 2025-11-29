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
- **Zookeeper**: Coordination service for Kafka and ClickHouse
- **Debezium**: Change Data Capture connector
- **Apache Kafka**: Distributed streaming platform
- **Apache Spark**: Distributed data processing engine
- **ClickHouse**: Column-oriented database for analytics (with replication)
- **Grafana**: Monitoring and visualization platform

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
Data-pipeline-for-northwind/

├── 1-postgres/               # PostgreSQL setup with Northwind database
│   ├── docker-compose.yml     # PostgreSQL service configuration
│   ├── Dockerfile            # PostgreSQL image definition
│   ├── northwind.sql         # Northwind database schema and data
│   └── postgresql.conf       # PostgreSQL configuration
├── 2-zookeeper/              # Zookeeper configuration
│   └── Dockerfile            # Zookeeper image definition
├── 3-kafka/                  # Kafka broker setup
│   ├── docker-compose.yml    # Kafka and Zookeeper services
│   └── Dockerfile            # Kafka image definition
├── 4-debezium/               # Debezium Connect CDC connector
│   ├── docker-compose.yml    # Debezium Connect service
│   └── Dockerfile            # Debezium image definition
├── 5-clickhouse/             # ClickHouse data warehouse (2 replicas)
│   ├── docker-compose.yml    # ClickHouse cluster configuration
│   ├── Dockerfile            # ClickHouse image definition
│   ├── config_replica1.xml   # ClickHouse replica 1 configuration
│   ├── config_replica2.xml   # ClickHouse replica 2 configuration
│   └── init-db/
│       └── init.sql          # Star schema table definitions
├── 6-spark/                  # Spark cluster and ETL jobs
│   ├── docker-compose.yml    # Spark cluster services
│   ├── Dockerfile            # Spark image definition
│   ├── conf/
│   │   └── spark-defaults.conf # Spark configuration
│   └── scripts/
│       ├── northwind-ch-stg.py    # CDC streaming job (Kafka → ClickHouse staging)
│       ├── northwind-dw.py        # Star schema builder (incremental ETL)
│       ├── northwind_schemas.py   # Debezium schema definitions
│       └── clickhouse-jdbc-0.7.2-all.jar # ClickHouse JDBC driver
├── 7-grafana/                # Grafana monitoring and visualization
│   ├── docker-compose.yml    # Grafana service configuration
│   ├── Dockerfile            # Grafana image definition
│   ├── northwind_queries.sql # Sample analytical queries
│   ├── dashboards/           # Grafana dashboard JSON files
│   └── provisioning/         # Grafana provisioning configuration
│       ├── dashboards/
│       └── datasources/
├── LICENSE                   # Project license
└── README.md                 # This file
```

## Components

### PostgreSQL (Port 15432)
- Source OLTP database with Northwind sample data
- Configured with logical replication for CDC
- Replication slot: `debezium`
- Database: `northwind`
- User: `postgres` / Password: `postgres`

### Zookeeper (Port 12181)
- Coordination service for Kafka
- Started as part of Kafka docker-compose
- Also used by ClickHouse for replication coordination

### Kafka (Ports 39092, 29092)
- Distributed streaming platform
- Stores CDC events as topics
- Topics are consumed by Spark streaming jobs
- Topic naming: `northwind.public.<table_name>`
- Depends on Zookeeper for coordination

### Debezium Connect
- Captures INSERT, UPDATE, DELETE operations from PostgreSQL
- Publishes changes to Kafka topics
- Connector name: `postgres-northwind-connector`
- Uses PostgreSQL logical replication (pgoutput plugin)

### Spark Cluster
- **Master**: Coordinates jobs (spark-master)
- **Worker**: Executes tasks (spark-worker)
- **pyspark-job-cdc**: Streams CDC data from Kafka to ClickHouse staging tables
- **pyspark-job-dw**: Incremental ETL from staging to data warehouse (star schema)

### ClickHouse (Ports 18123, 28123, 19000, 29000)
- **Replication**: 2 replicas (clickhouse1, clickhouse2) with Zookeeper coordination
- **Cluster**: `replicated_cluster`
- **Staging Tables**: `northwind.*` (raw CDC data with ReplacingMergeTree engine)
- **Data Warehouse**: Star schema with dimensions and facts
  - **Dimensions**: DimGeography, DimCustomer, DimEmployees, DimSuppliers, DimProducts, DimShippers, DimTerritories, DimDate
  - **Facts**: FactOrders, FactEmployeeTerritories
- **User**: `default` / Password: `123456`

### Grafana (Port 3000)
- **Dashboard**: Pre-configured dashboards for ClickHouse monitoring
- **Data Source**: ClickHouse data warehouse
- **Queries**: Sample analytical queries in `northwind_queries.sql`
- **Default Credentials**: `admin` / `admin`
- **URL**: http://localhost:3000

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

### Access Grafana Dashboard
Open your browser and navigate to:
```
http://localhost:3000
```
Login with default credentials: `admin` / `admin`

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
docker ps 
```

## Stopping the Pipeline

To stop all services (in reverse order):
```powershell
docker-compose -f ./7-grafana/docker-compose.yml down
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
- [x] Update README: Remove references to deleted files and update project structure
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