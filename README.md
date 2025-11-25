# Data-pipeline-for-northwind
POSTGRESQL(NORTHWIND OLTP) -> DEBEZIUM -> kafka -> SPARK -> CLICKHOUSE(DW)


docker network create ProjectHost
docker-compose -f ./1-postgres/docker-compose.yml up -d --force-recreate
docker-compose -f ./3-kafka/docker-compose.yml up -d --force-recreate
docker-compose -f ./4-debezium/docker-compose.yml up -d --force-recreate
docker-compose -f ./5-clickhouse/docker-compose.yml up -d --force-recreate
docker-compose -f ./6-spark/docker-compose.yml up -d --force-recreate

INSERT INTO orders VALUES (11079, 'RATTC', 1, '1998-05-06', '1998-06-03', NULL, 2, 8.52999973, 'Rattlesnake Canyon Grocery', '2817 Milton Dr.', 'Albuquerque', 'NM', '87110', 'USA');
INSERT INTO order_details VALUES (11079, 7, 30, 1, 0.0500000007);
INSERT INTO order_details VALUES (11079, 8, 40, 2, 0.100000001);
INSERT INTO order_details VALUES (11079, 10, 31, 1, 0);



SELECT DISTINCT
        Country,
        Region,
        City,
        postal_code ,
        address 
FROM
(
    -- From Customers
    SELECT
        Country,
        Region,
        City,
        postal_code ,
        address 
    FROM Customers
    
    UNION ALL
    
    -- From Suppliers
    SELECT
        Country,
        Region,
        City,
        postal_code ,
        address 
    FROM Suppliers

    UNION ALL
    
    -- From Employees
    SELECT
        Country,
        Region,
        City,
        postal_code ,
        address 
    FROM Employees
)
WHERE Country IS NOT NULL;


🚀 Steps to Implement Star Schema DW with CDC + Spark + ClickHouse
1️⃣ Ingest CDC data (current step)

Continue streaming from Kafka → Spark → raw staging tables.

These tables store unprocessed CDC data.

2️⃣ Create Staging Layer

Store raw CDC output in ClickHouse.

Used for traceability and source-of-truth.

3️⃣ Design Star Schema

Define Fact tables (events, transactions, e.g., orders).

Define Dimension tables (entities, e.g., customers, products).

Assign primary keys and relationships.

4️⃣ Develop Transformation Layer (ETL in Spark)

Read staging data.

Apply cleaning, mapping, data standardization.

Split into Fact and Dimension structures.

5️⃣ Handle Slowly Changing Dimensions (SCD)

Typically SCD Type 2: add versioned rows.

Mark current vs historical values.

6️⃣ Surrogate Key Generation

Create DW-specific IDs (not using CDC primary keys directly).

Example: customer_key, order_key.

7️⃣ Load Dimension Tables

Insert new data or update existing versioned rows.

Use Spark → ClickHouse.

8️⃣ Load Fact Tables

Use keys from dimensions.

Insert transaction records with references.

9️⃣ Schedule & Automate

Trigger ETL with micro-batches or scheduled jobs.

Monitor & validate loads.

🔟 Optimize ClickHouse Tables

Use correct engines: MergeTree, ReplacingMergeTree, etc.

Index on keys for fast joins.

1️⃣1️⃣ Implement Data Quality Checks

Validate counts, referential integrity, null checks.

1️⃣2️⃣ Use BI Tool or Dashboard

Connect ClickHouse to analytics tool (Metabase, Grafana, PowerBI, etc.)