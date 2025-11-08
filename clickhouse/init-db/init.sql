-- Create database
CREATE DATABASE IF NOT EXISTS northwind ON CLUSTER replicated_cluster;

-- Common settings
SET allow_experimental_object_type = 1;


-- Helper: creates a Kafka source table that reads full JSON as String
-- Debezium topics: northwind.public.<table>

-- Customers
CREATE TABLE IF NOT EXISTS northwind.kafka_customers
(
    data String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'northwind.public.customers',
    kafka_group_name = 'clickhouse_northwind',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS northwind.customers_local ON CLUSTER replicated_cluster
(
    CustomerID Int32,
    CompanyName String,
    ContactName Nullable(String),
    ContactTitle Nullable(String),
    Address Nullable(String),
    City Nullable(String),
    Region Nullable(String),
    PostalCode Nullable(String),
    Country Nullable(String),
    Phone Nullable(String),
    Fax Nullable(String),
    _op LowCardinality(String),
    _ts_ms DateTime64(3)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/customers','{replica}')
ORDER BY CustomerID;

CREATE MATERIALIZED VIEW IF NOT EXISTS northwind.mv_customers TO northwind.customers_local AS
SELECT
    toInt32OrZero(JSONExtractString(data,'after','customer_id')) AS CustomerID,
    coalesce(JSONExtractString(data,'after','company_name'), '') AS CompanyName,
    JSONExtractString(data,'after','contact_name') AS ContactName,
    JSONExtractString(data,'after','contact_title') AS ContactTitle,
    JSONExtractString(data,'after','address') AS Address,
    JSONExtractString(data,'after','city') AS City,
    JSONExtractString(data,'after','region') AS Region,
    JSONExtractString(data,'after','postal_code') AS PostalCode,
    JSONExtractString(data,'after','country') AS Country,
    JSONExtractString(data,'after','phone') AS Phone,
    JSONExtractString(data,'after','fax') AS Fax,
    JSONExtractString(data,'op') AS _op,
    toDateTime64(JSONExtractUInt(data,'ts_ms')/1000, 3) AS _ts_ms
FROM northwind.kafka_customers
WHERE JSONExtractString(data,'op') IN ('c','r','u');

-- Products
CREATE TABLE IF NOT EXISTS northwind.kafka_products
(
    data String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'northwind.public.products',
    kafka_group_name = 'clickhouse_northwind',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS northwind.products_local ON CLUSTER replicated_cluster
(
    ProductID Int32,
    ProductName String,
    SupplierID Nullable(Int32),
    CategoryID Nullable(Int32),
    QuantityPerUnit Nullable(String),
    UnitPrice Nullable(Decimal(12,2)),
    UnitsInStock Nullable(Int32),
    UnitsOnOrder Nullable(Int32),
    ReorderLevel Nullable(Int32),
    Discontinued Nullable(Int8),
    _op LowCardinality(String),
    _ts_ms DateTime64(3)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/products','{replica}')
ORDER BY ProductID;

CREATE MATERIALIZED VIEW IF NOT EXISTS northwind.mv_products TO northwind.products_local AS
SELECT
    toInt32OrZero(JSONExtractString(data,'after','product_id')) AS ProductID,
    coalesce(JSONExtractString(data,'after','product_name'), '') AS ProductName,
    nullIf(toInt32OrNull(JSONExtractString(data,'after','supplier_id')), 0) AS SupplierID,
    nullIf(toInt32OrNull(JSONExtractString(data,'after','category_id')), 0) AS CategoryID,
    JSONExtractString(data,'after','quantity_per_unit') AS QuantityPerUnit,
    toDecimal128OrNull(JSONExtractString(data,'after','unit_price'), 2) AS UnitPrice,
    toInt32OrNull(JSONExtractString(data,'after','units_in_stock')) AS UnitsInStock,
    toInt32OrNull(JSONExtractString(data,'after','units_on_order')) AS UnitsOnOrder,
    toInt32OrNull(JSONExtractString(data,'after','reorder_level')) AS ReorderLevel,
    toInt8OrNull(JSONExtractString(data,'after','discontinued')) AS Discontinued,
    JSONExtractString(data,'op') AS _op,
    toDateTime64(JSONExtractUInt(data,'ts_ms')/1000, 3) AS _ts_ms
FROM northwind.kafka_products
WHERE JSONExtractString(data,'op') IN ('c','r','u');

-- Orders
CREATE TABLE IF NOT EXISTS northwind.kafka_orders
(
    data String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'northwind.public.orders',
    kafka_group_name = 'clickhouse_northwind',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS northwind.orders_local ON CLUSTER replicated_cluster
(
    OrderID Int32,
    CustomerID Nullable(Int32),
    EmployeeID Nullable(Int32),
    OrderDate Nullable(Date),
    RequiredDate Nullable(Date),
    ShippedDate Nullable(Date),
    ShipVia Nullable(Int32),
    Freight Nullable(Decimal(12,2)),
    ShipName Nullable(String),
    ShipAddress Nullable(String),
    ShipCity Nullable(String),
    ShipRegion Nullable(String),
    ShipPostalCode Nullable(String),
    ShipCountry Nullable(String),
    _op LowCardinality(String),
    _ts_ms DateTime64(3)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/orders','{replica}')
ORDER BY (OrderID);

CREATE MATERIALIZED VIEW IF NOT EXISTS northwind.mv_orders TO northwind.orders_local AS
SELECT
    toInt32OrZero(JSONExtractString(data,'after','order_id')) AS OrderID,
    toInt32OrNull(JSONExtractString(data,'after','customer_id')) AS CustomerID,
    toInt32OrNull(JSONExtractString(data,'after','employee_id')) AS EmployeeID,
    toDateOrNull(JSONExtractString(data,'after','order_date')) AS OrderDate,
    toDateOrNull(JSONExtractString(data,'after','required_date')) AS RequiredDate,
    toDateOrNull(JSONExtractString(data,'after','shipped_date')) AS ShippedDate,
    toInt32OrNull(JSONExtractString(data,'after','ship_via')) AS ShipVia,
    toDecimal128OrNull(JSONExtractString(data,'after','freight'), 2) AS Freight,
    JSONExtractString(data,'after','ship_name') AS ShipName,
    JSONExtractString(data,'after','ship_address') AS ShipAddress,
    JSONExtractString(data,'after','ship_city') AS ShipCity,
    JSONExtractString(data,'after','ship_region') AS ShipRegion,
    JSONExtractString(data,'after','ship_postal_code') AS ShipPostalCode,
    JSONExtractString(data,'after','ship_country') AS ShipCountry,
    JSONExtractString(data,'op') AS _op,
    toDateTime64(JSONExtractUInt(data,'ts_ms')/1000, 3) AS _ts_ms
FROM northwind.kafka_orders
WHERE JSONExtractString(data,'op') IN ('c','r','u');

-- Order Details
CREATE TABLE IF NOT EXISTS northwind.kafka_order_details
(
    data String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'northwind.public.order_details',
    kafka_group_name = 'clickhouse_northwind',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS northwind.order_details_local ON CLUSTER replicated_cluster
(
    OrderID Int32,
    ProductID Int32,
    UnitPrice Nullable(Decimal(12,2)),
    Quantity Nullable(Int32),
    Discount Nullable(Float32),
    _op LowCardinality(String),
    _ts_ms DateTime64(3)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/order_details','{replica}')
ORDER BY (OrderID, ProductID);

CREATE MATERIALIZED VIEW IF NOT EXISTS northwind.mv_order_details TO northwind.order_details_local AS
SELECT
    toInt32OrZero(JSONExtractString(data,'after','order_id')) AS OrderID,
    toInt32OrZero(JSONExtractString(data,'after','product_id')) AS ProductID,
    toDecimal128OrNull(JSONExtractString(data,'after','unit_price'), 2) AS UnitPrice,
    toInt32OrNull(JSONExtractString(data,'after','quantity')) AS Quantity,
    toFloat32OrNull(JSONExtractString(data,'after','discount')) AS Discount,
    JSONExtractString(data,'op') AS _op,
    toDateTime64(JSONExtractUInt(data,'ts_ms')/1000, 3) AS _ts_ms
FROM northwind.kafka_order_details
WHERE JSONExtractString(data,'op') IN ('c','r','u');

-- Optional: create Distributed tables for querying both replicas
CREATE TABLE IF NOT EXISTS northwind.customers AS northwind.customers_local
ENGINE = Distributed(replicated_cluster, northwind, customers_local, rand());

CREATE TABLE IF NOT EXISTS northwind.products AS northwind.products_local
ENGINE = Distributed(replicated_cluster, northwind, products_local, rand());

CREATE TABLE IF NOT EXISTS northwind.orders AS northwind.orders_local
ENGINE = Distributed(replicated_cluster, northwind, orders_local, rand());

CREATE TABLE IF NOT EXISTS northwind.order_details AS northwind.order_details_local
ENGINE = Distributed(replicated_cluster, northwind, order_details_local, rand());

