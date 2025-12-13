# Northwind Stream Data Warehouse

A real-time data pipeline that streams data from PostgreSQL (Northwind OLTP database) to ClickHouse data warehouse using Change Data Capture (CDC) technology.

## Architecture

```
PostgreSQL (OLTP) → Debezium (CDC) → Kafka → Spark (ETL) → ClickHouse (DW) → Grafana (Dashboards)
```

### Pipeline Flow

1. **PostgreSQL**: Source OLTP database containing the Northwind sample database
2. **Debezium**: Captures database changes using PostgreSQL logical replication
3. **Kafka**: Message broker that stores CDC events as topics
4. **Spark**: Processes streaming data and performs ETL transformations
   - **CDC Streaming Job**: Streams data from Kafka to ClickHouse staging tables
   - **DW ETL Job**: Builds star schema dimensions and facts from staging tables (incremental)(SCD2)
5. **ClickHouse**: Data warehouse with star schema design for analytical queries
6. **Grafana**: Visualization and monitoring dashboards

## Technology Stack

- **PostgreSQL 18**: Source database (OLTP)
- **Zookeeper**: Coordination service for Kafka and ClickHouse 
- **Debezium**: Change Data Capture connector
- **Apache Kafka**: Distributed streaming platform
- **Apache Spark**: Distributed data processing engine
- **ClickHouse**: Column-oriented database for analytics
- **Grafana**: Monitoring and visualization platform

## Prerequisites

- Docker and Docker Compose installed
- At least 8GB of available RAM
- Windows PowerShell (for Windows) or Bash (for Linux/Mac)

## Quick Start

### Option 1: Deploy All Services at Once (Recommended)

A consolidated `docker-compose.yml` file is available at the root of the project that includes all services:

```powershell
# Create Docker network
docker network create ProjectHost

# Start all services
docker-compose up -d --force-recreate
```

### Option 2: Deploy Services Individually

For more control, you can start services individually in the correct order:

```powershell
# 1. Create Docker network
docker network create ProjectHost

# 2. Start PostgreSQL with Northwind database
docker-compose -f ./1-postgres/docker-compose.yml up -d --force-recreate

# 3. Start Kafka and Zookeeper
docker-compose -f ./3-kafka/docker-compose.yml up -d --force-recreate

# 4. Start Debezium Connect
docker-compose -f ./4-debezium/docker-compose.yml up -d --force-recreate

# 5. Start ClickHouse
docker-compose -f ./5-clickhouse/docker-compose.yml up -d --force-recreate

# 6. Start Spark (Master, Workers, and ETL jobs)
docker-compose -f ./6-spark/docker-compose.yml up -d --force-recreate

# 7. Start Grafana
docker-compose -f ./7-grafana/docker-compose.yml up -d --force-recreate
```

### Verify Pipeline Status

Check container health:
```powershell
docker ps
```

All containers should show status `Up` (healthy).

## Project Structure

```
Data-pipeline-for-northwind/
├── 0-info/                      # Project documentation and diagrams
│   ├── dash.png                 # Dashboard screenshot
│   ├── DW-sem.PNG               # Data warehouse schema diagram
│   ├── ETL Data Pipelines.png   # ETL pipeline diagram
│   └── northwind_structure.csv  # Database structure reference
├── 1-postgres/                  # PostgreSQL setup with Northwind database
│   ├── docker-compose.yml       # PostgreSQL service configuration
│   ├── Dockerfile               # PostgreSQL image definition
│   ├── .env                     # Environment variables for PostgreSQL
│   ├── northwind.sql            # Northwind database schema and data
│   └── postgresql.conf          # PostgreSQL configuration
├── 2-zookeeper/                 # Zookeeper configuration
│   └── Dockerfile              # Zookeeper image definition
├── 3-kafka/                     # Kafka broker setup
│   ├── docker-compose.yml       # Kafka and Zookeeper services
│   ├── Dockerfile              # Kafka image definition
│   └── .env                    # Environment variables for Kafka
├── 4-debezium/                 # Debezium Connect CDC connector
│   ├── docker-compose.yml      # Debezium Connect service
│   ├── Dockerfile              # Debezium image definition
│   └── .env                    # Environment variables for Debezium
├── 5-clickhouse/               # ClickHouse data warehouse
│   ├── docker-compose.yml      # ClickHouse cluster configuration
│   ├── Dockerfile              # ClickHouse image definition
│   ├── .env                    # Environment variables for ClickHouse
│   ├── config_replica1.xml     # ClickHouse replica 1 configuration
│   ├── config_replica2.xml     # ClickHouse replica 2 configuration
│   └── init-db/
│       └── init.sql            # Star schema table definitions
├── 6-spark/                    # Spark cluster and ETL jobs
│   ├── docker-compose.yml      # Spark cluster services
│   ├── Dockerfile              # Spark image definition
│   ├── .env                    # Environment variables for Spark
│   ├── conf/
│   │   └── spark-defaults.conf # Spark configuration
│   └── scripts/
│       ├── northwind-ch-stg.py      # CDC streaming job (Kafka → ClickHouse staging)
│       ├── northwind-dw.py          # Star schema builder (incremental ETL)
│       ├── northwind_schemas.py     # Debezium schema definitions
│       └── clickhouse-jdbc-0.7.2-all.jar # ClickHouse JDBC driver
├── 7-grafana/                  # Grafana monitoring and visualization
│   ├── docker-compose.yml     # Grafana service configuration
│   ├── Dockerfile             # Grafana image definition
│   ├── .env                   # Environment variables for Grafana
│   ├── northwind_queries.sql  # Sample analytical queries
│   ├── dashboards/            # Grafana dashboard JSON files
│   └── provisioning/          # Grafana provisioning configuration
│       ├── dashboards/
│       └── datasources/
├── docker-compose.yml         # Consolidated docker-compose (all services)
├── LICENSE                    # Project license
└── README.md                  # This file
```

## Components

### PostgreSQL
- **Port**: `15432` (host) → `5432` (container)
- **Source**: OLTP database with Northwind sample data
- **Configuration**: Logical replication enabled for CDC
- **Replication Slot**: `debezium`
- **Database**: `northwind`
- **Credentials**: `postgres` / `postgres`

### Zookeeper
- **Port**: `12181` (host) → `2181` (container)
- **Purpose**: Coordination service for Kafka
- **Note**: Also used by ClickHouse for replication coordination (separate instance)

### Kafka
- **Ports**: 
  - `39092` (host) → `9092` (container) - Internal broker
  - `29092` (host) → `29092` (container) - External access
- **Purpose**: Distributed streaming platform
- **Topics**: CDC events stored as `northwind.public.<table_name>`
- **Dependencies**: Zookeeper for coordination

### Debezium Connect
- **Port**: `18083` (host) → `8083` (container)
- **Purpose**: Captures INSERT, UPDATE, DELETE operations from PostgreSQL
- **Connector Name**: `postgres-northwind-connector`
- **Replication Plugin**: `pgoutput` (PostgreSQL logical replication)
- **Features**: 
  - Automatic schema change detection
  - Initial snapshot mode
  - Unwraps change events for easier processing

### Spark Cluster
- **Master UI Port**: `18085` (host) → `8080` (container) - Access at http://localhost:18085
- **Master RPC Port**: `7077`
- **Components**:
  - **spark-master**: Coordinates jobs
  - **spark-worker1**: Executes tasks
  - **spark-worker2**: Executes tasks
  - **pyspark-job-cdc**: Streams CDC data from Kafka to ClickHouse staging tables
  - **pyspark-job-dw**: Incremental ETL from staging to data warehouse (star schema)

### ClickHouse
- **Ports**: 
  - Native: `19000` (host) → `9000` (container)
  - HTTP: `18123` (host) → `8123` (container)
- **Replication**: Currently running single replica (clickhouse1). Replica 2 is configured but commented out.
- **Cluster**: `replicated_cluster` (when replication enabled)
- **Databases**:
  - **Staging**: `northwind.*` - Raw CDC data with ReplacingMergeTree engine
  - **Data Warehouse**: Star schema with dimensions and facts
- **Schema**:
  - **Dimensions**: DimGeography, DimCustomer, DimEmployees, DimSuppliers, DimProducts, DimShippers, DimTerritories, DimDate
  - **Facts**: FactOrders, FactEmployeeTerritories
- **Credentials**: `default` / `123456`

### Grafana
- **Port**: `12345` (host) → `3000` (container)
- **URL**: http://localhost:12345
- **Purpose**: Pre-configured dashboards for ClickHouse monitoring and Northwind analytics
- **Data Source**: ClickHouse data warehouse
- **Credentials**: `admin` / `admin` (default)
- **Features**: 
  - Sample analytical queries in `northwind_queries.sql`
  - Pre-provisioned dashboards and datasources

## Data Warehouse Schema

The data warehouse implements a **star schema** design:

- **Dimension Tables**: Store descriptive attributes (customers, products, employees, geography, etc.)
- **Fact Tables**: Store business metrics and transactions (orders, employee territories)
- **Date Dimension**: Pre-populated date dimension (1970-2050)

### Key Features

- **Incremental ETL**: The DW ETL job processes only changed records based on `updatedate` timestamp
- **ReplacingMergeTree**: ClickHouse staging tables use ReplacingMergeTree engine to handle updates
- **Real-time Processing**: Changes are streamed in near real-time from source to data warehouse

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

### Access Service UIs

- **Grafana Dashboard**: http://localhost:12345
- **Spark Master UI**: http://localhost:18085
- **Debezium Connect API**: http://localhost:18083

## Monitoring

### View Spark Job Logs
```powershell
# CDC streaming job
docker logs pyspark-job-cdc --tail 50 -f

# DW ETL job
docker logs pyspark-job-dw --tail 50 -f
```

### Check Container Health
```powershell
docker ps
```

### Monitor Spark Jobs
Access the Spark Master UI at http://localhost:18085 to view:
- Running applications
- Worker nodes status
- Job execution details

## Stopping the Pipeline

### Option 1: Stop All Services (if using root docker-compose.yml)
```powershell
docker-compose down
```

### Option 2: Stop Services Individually (in reverse order)
```powershell
docker-compose -f ./7-grafana/docker-compose.yml down
docker-compose -f ./6-spark/docker-compose.yml down
docker-compose -f ./5-clickhouse/docker-compose.yml down
docker-compose -f ./4-debezium/docker-compose.yml down
docker-compose -f ./3-kafka/docker-compose.yml down
docker-compose -f ./1-postgres/docker-compose.yml down
```

## Key Features

- **Real-time CDC**: Changes are captured and processed in near real-time
- **Fault Tolerance**: Spark jobs use checkpointing for recovery
- **Incremental Processing**: ETL jobs process only changed records
- **Scalable Architecture**: Distributed processing with Spark and Kafka
- **Data Quality**: ReplacingMergeTree handles updates and deletes correctly

## Notes

- The pipeline processes changes in real-time using CDC technology
- Spark jobs use checkpointing for fault tolerance
- ClickHouse uses ReplacingMergeTree for handling updates
- The ETL job runs incrementally, processing only changed records based on `updatedate`
- ClickHouse replica 2 is currently disabled (commented out in docker-compose files)
- All services use the `ProjectHost` Docker network for communication
- Environment variables can be customized via `.env` files in each service directory

## Troubleshooting

### Services Not Starting
1. Ensure Docker network `ProjectHost` exists: `docker network ls`
2. Check container logs: `docker logs <container-name>`
3. Verify port availability: Ensure ports are not already in use

### Debezium Connector Issues
1. Check PostgreSQL replication slot: `docker exec postgres psql -U postgres -d northwind -c "SELECT * FROM pg_replication_slots;"`
2. Verify connector status: `docker exec debezium-connect curl -s http://localhost:8083/connectors/postgres-northwind-connector/status`

### Spark Jobs Not Processing
1. Check Spark Master UI: http://localhost:18085
2. Verify Kafka topics have data: `docker exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic northwind.public.orders --from-beginning`
3. Review job logs: `docker logs pyspark-job-cdc` or `docker logs pyspark-job-dw`

## License

See [LICENSE](LICENSE) file for details.
