CREATE DATABASE IF NOT EXISTS northwind ON CLUSTER replicated_cluster;

CREATE TABLE IF NOT EXISTS northwind.northwind_categories
ON CLUSTER replicated_cluster
(
    category_id Int16,
    category_name Nullable(String),
    description Nullable(String),
    picture Nullable(String),
    operation CHAR(1),
    updatedate DateTime
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/northwind/northwind_categories/{replica}',
    '{replica}',
     updatedate
    
)
ORDER BY category_id;

CREATE TABLE IF NOT EXISTS northwind.northwind_customer_customer_demo
ON CLUSTER replicated_cluster
(
    customer_id String,
    customer_type_id String,
    operation CHAR(1),
    updatedate DateTime
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/northwind/northwind_customer_customer_demo/{replica}',
    '{replica}',
     updatedate
)
ORDER BY (customer_id, customer_type_id);

CREATE TABLE IF NOT EXISTS northwind.northwind_customer_demographics
ON CLUSTER replicated_cluster
(
    customer_type_id String,
    customer_desc Nullable(String),
    operation CHAR(1),
    updatedate DateTime
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/northwind/northwind_customer_demographics/{replica}',
    '{replica}',
     updatedate
)
ORDER BY customer_type_id;

CREATE TABLE IF NOT EXISTS northwind.northwind_customers
ON CLUSTER replicated_cluster
(
    customer_id String,
    company_name Nullable(String),
    contact_name Nullable(String),
    contact_title Nullable(String),
    address Nullable(String),
    city Nullable(String),
    region Nullable(String),
    postal_code Nullable(String),
    country Nullable(String),
    phone Nullable(String),
    fax Nullable(String),
    operation CHAR(1),
    updatedate DateTime
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/northwind/northwind_customers/{replica}',
    '{replica}',
     updatedate
)
ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS northwind.northwind_employees
ON CLUSTER replicated_cluster
(
    employee_id Int16,
    last_name Nullable(String),
    first_name Nullable(String),
    title Nullable(String),
    title_of_courtesy Nullable(String),
    birth_date Nullable(Date),
    hire_date Nullable(Date),
    address Nullable(String),
    city Nullable(String),
    region Nullable(String),
    postal_code Nullable(String),
    country Nullable(String),
    home_phone Nullable(String),
    extension Nullable(String),
    photo Nullable(String),
    notes Nullable(String),
    reports_to Nullable(Int16),
    photo_path Nullable(String),
    operation CHAR(1),
    updatedate DateTime
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/northwind/northwind_employees/{replica}',
    '{replica}',
     updatedate
)
ORDER BY employee_id;

CREATE TABLE IF NOT EXISTS northwind.northwind_employee_territories
ON CLUSTER replicated_cluster
(
    employee_id Int16,
    territory_id String,
    operation CHAR(1),
    updatedate DateTime
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/northwind/northwind_employee_territories/{replica}',
    '{replica}',
     updatedate
)
ORDER BY (employee_id, territory_id);

CREATE TABLE IF NOT EXISTS northwind.northwind_order_details
ON CLUSTER replicated_cluster
(
    order_id Int16,
    product_id Int16,
    unit_price Nullable(Float32),
    quantity Nullable(Int16),
    discount Nullable(Float32),
    operation CHAR(1),
    updatedate DateTime
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/northwind/northwind_order_details/{replica}',
    '{replica}',
     updatedate
)
ORDER BY (order_id, product_id);

CREATE TABLE IF NOT EXISTS northwind.northwind_orders
ON CLUSTER replicated_cluster
(
    order_id Int16,
    customer_id Nullable(String),
    employee_id Nullable(String),
    order_date Nullable(String),
    required_date Nullable(String),
    shipped_date Nullable(String),
    ship_via Nullable(String),
    freight Nullable(Float32),
    ship_name Nullable(String),
    ship_address Nullable(String),
    ship_city Nullable(String),
    ship_region Nullable(String),
    ship_postal_code Nullable(String),
    ship_country Nullable(String),
    operation CHAR(1),
    updatedate DateTime
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/northwind/northwind_orders/{replica}',
    '{replica}',
     updatedate
)
ORDER BY order_id;

CREATE TABLE IF NOT EXISTS northwind.northwind_products
ON CLUSTER replicated_cluster
(
    product_id Int16,
    product_name Nullable(String),
    supplier_id Nullable(Int16),
    category_id Nullable(Int16),
    quantity_per_unit Nullable(String),
    unit_price Nullable(Float32),
    units_in_stock Nullable(Int16),
    units_on_order Nullable(Int16),
    reorder_level Nullable(Int16),
    discontinued Nullable(UInt8),
    operation CHAR(1),
    updatedate DateTime
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/northwind/northwind_products/{replica}',
    '{replica}',
     updatedate
)
ORDER BY product_id;

CREATE TABLE IF NOT EXISTS northwind.northwind_region
ON CLUSTER replicated_cluster
(
    region_id Int16,
    region_description Nullable(String),
    operation CHAR(1),
    updatedate DateTime
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/northwind/northwind_region/{replica}',
    '{replica}',
     updatedate
)
ORDER BY region_id;

CREATE TABLE IF NOT EXISTS northwind.northwind_shippers
ON CLUSTER replicated_cluster
(
    shipper_id Int16,
    company_name Nullable(String),
    phone Nullable(String),
    operation CHAR(1),
    updatedate DateTime
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/northwind/northwind_shippers/{replica}',
    '{replica}',
     updatedate
)
ORDER BY shipper_id;

CREATE TABLE IF NOT EXISTS northwind.northwind_suppliers
ON CLUSTER replicated_cluster
(
    supplier_id Int16,
    company_name Nullable(String),
    contact_name Nullable(String),
    contact_title Nullable(String),
    address Nullable(String),
    city Nullable(String),
    region Nullable(String),
    postal_code Nullable(String),
    country Nullable(String),
    phone Nullable(String),
    fax Nullable(String),
    homepage Nullable(String),
    operation CHAR(1),
    updatedate DateTime
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/northwind/northwind_suppliers/{replica}',
    '{replica}',
     updatedate
)
ORDER BY supplier_id;

CREATE TABLE IF NOT EXISTS northwind.northwind_territories
ON CLUSTER replicated_cluster
(
    territory_id String,
    territory_description Nullable(String),
    region_id Nullable(Int16),
    operation CHAR(1),
    updatedate DateTime
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/northwind/northwind_territories/{replica}',
    '{replica}',
     updatedate
)
ORDER BY territory_id;

CREATE TABLE IF NOT EXISTS northwind.northwind_us_states
ON CLUSTER replicated_cluster
(
    state_id Int16,
    state_name Nullable(String),
    state_abbr Nullable(String),
    state_region Nullable(String),
    operation CHAR(1),
    updatedate DateTime
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/northwind/northwind_us_states/{replica}',
    '{replica}',
     updatedate
)
ORDER BY state_id;
-- ###################################
-- Northwind Data Warehouse Star Schema
-- Includes: All Dimensions, Fact Tables, and ETL Materialized Views

CREATE DATABASE IF NOT EXISTS dwh;
USE dwh;

/* =============================
   DIMENSION: CUSTOMERS
   ============================= */
CREATE TABLE IF NOT EXISTS dwh.dim_customers (
    customer_id String,
    company_name String,
    contact_name String,
    contact_title String,
    city String,
    region String,
    country String,
    phone String,
    updated_at DateTime
) ENGINE = MergeTree()
ORDER BY customer_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.mv_dim_customers
TO dwh.dim_customers AS
SELECT
    customer_id,
    company_name,
    contact_name,
    contact_title,
    city,
    region,
    country,
    phone,
    updatedate AS updated_at
FROM northwind.northwind_customers
WHERE operation != 'd';

/* =============================
   DIMENSION: EMPLOYEES
   ============================= */
CREATE TABLE IF NOT EXISTS dwh.dim_employees (
    employee_id Int32,
    full_name String,
    title String,
    title_of_courtesy String,
    city String,
    region String,
    country String,
    updated_at DateTime
) ENGINE = MergeTree()
ORDER BY employee_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.mv_dim_employees
TO dwh.dim_employees AS
SELECT
    employee_id,
    concat(first_name, ' ', last_name) AS full_name,
    title,
    title_of_courtesy,
    city,
    region,
    country,
    updatedate AS updated_at
FROM northwind.northwind_employees
WHERE operation != 'd';

/* =============================
   DIMENSION: SUPPLIERS
   ============================= */
CREATE TABLE IF NOT EXISTS dwh.dim_suppliers (
    supplier_id Int32,
    company_name String,
    contact_name String,
    contact_title String,
    city String,
    region String,
    country String,
    phone String,
    updated_at DateTime
) ENGINE = MergeTree()
ORDER BY supplier_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.mv_dim_suppliers
TO dwh.dim_suppliers AS
SELECT
    supplier_id,
    company_name,
    contact_name,
    contact_title,
    city,
    region,
    country,
    phone,
    updatedate AS updated_at
FROM northwind.northwind_suppliers
WHERE operation != 'd';

/* =============================
   DIMENSION: CATEGORIES
   ============================= */
CREATE TABLE IF NOT EXISTS dwh.dim_categories (
    category_id Int32,
    category_name String,
    description String,
    updated_at DateTime
) ENGINE = MergeTree()
ORDER BY category_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.mv_dim_categories
TO dwh.dim_categories AS
SELECT
    category_id,
    category_name,
    description,
    updatedate AS updated_at
FROM northwind.northwind_categories
WHERE operation != 'd';

/* =============================
   DIMENSION: PRODUCTS
   ============================= */
CREATE TABLE IF NOT EXISTS dwh.dim_products (
    product_id Int32,
    product_name String,
    category_id Int32,
    category_name String,
    supplier_id Int32,
    supplier_name String,
    unit_price Float32,
    discontinued UInt8,
    updated_at DateTime
) ENGINE = MergeTree()
ORDER BY product_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.mv_dim_products
TO dwh.dim_products AS
SELECT
    p.product_id AS product_id,
    p.product_name AS product_name,
    p.category_id AS category_id,
    c.category_name AS category_name,
    p.supplier_id AS supplier_id,
    s.company_name AS supplier_name,
    p.unit_price AS unit_price,
    p.discontinued AS discontinued,
    p.updatedate AS updated_at
FROM northwind.northwind_products p
LEFT JOIN northwind.northwind_categories c ON p.category_id = c.category_id
LEFT JOIN northwind.northwind_suppliers s ON p.supplier_id = s.supplier_id
WHERE p.operation != 'd';


/* =============================
   DIMENSION: SHIPPERS
   ============================= */
CREATE TABLE IF NOT EXISTS dwh.dim_shippers (
    shipper_id Int32,
    company_name String,
    phone String,
    updated_at DateTime
) ENGINE = MergeTree()
ORDER BY shipper_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.mv_dim_shippers
TO dwh.dim_shippers AS
SELECT
    shipper_id,
    company_name,
    phone,
    updatedate AS updated_at
FROM northwind.northwind_shippers
WHERE operation != 'd';

/* =============================
   DIMENSION: REGION
   ============================= */
CREATE TABLE IF NOT EXISTS dwh.dim_region (
    region_id Int32,
    region_description String,
    updated_at DateTime
) ENGINE = MergeTree()
ORDER BY region_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.mv_dim_region
TO dwh.dim_region AS
SELECT
    region_id,
    region_description,
    updatedate AS updated_at
FROM northwind.northwind_region
WHERE operation != 'd';

/* =============================
   DIMENSION: TERRITORIES
   ============================= */
CREATE TABLE IF NOT EXISTS dwh.dim_territories (
    territory_id String,
    territory_description String,
    region_id Int32,
    updated_at DateTime
) ENGINE = MergeTree()
ORDER BY territory_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.mv_dim_territories
TO dwh.dim_territories AS
SELECT
    territory_id,
    territory_description,
    region_id,
    updatedate AS updated_at
FROM northwind.northwind_territories
WHERE operation != 'd';

/* =============================
   DIMENSION: US STATES
   ============================= */
CREATE TABLE IF NOT EXISTS dwh.dim_us_states (
    state_id Int32,
    state_name String,
    state_abbr String,
    state_region String,
    updated_at DateTime
) ENGINE = MergeTree()
ORDER BY state_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.mv_dim_us_states
TO dwh.dim_us_states AS
SELECT
    state_id,
    state_name,
    state_abbr,
    state_region,
    updatedate AS updated_at
FROM northwind.northwind_us_states
WHERE operation != 'd';

/* =============================
   FACT TABLE: ORDER ITEMS
   ============================= */
CREATE TABLE IF NOT EXISTS dwh.fact_order_items (
    order_id Int32,
    order_date Date,
    customer_id String,
    employee_id Int32,
    product_id Int32,
    quantity Int32,
    unit_price Float32,
    discount Float32,
    line_total Float64,
    ship_country String,
    ship_city String,
    ship_via Int32,
    updated_at DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(order_date)
ORDER BY (order_date, order_id, product_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.mv_fact_order_items
TO dwh.fact_order_items AS
SELECT
    d.order_id,
    o.order_date AS order_date,
    o.customer_id,
    toInt32(o.employee_id) AS employee_id,
    d.product_id,
    d.quantity,
    d.unit_price,
    d.discount,
    d.quantity * d.unit_price * (1 - d.discount) AS line_total,
    o.ship_country,
    o.ship_city,
    toInt32(o.ship_via) AS ship_via,
    d.updatedate AS updated_at
FROM northwind.northwind_order_details d
LEFT JOIN northwind.northwind_orders o USING (order_id)
WHERE d.operation != 'd';



