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



CREATE TABLE IF NOT EXISTS default.fact_sales
(
    order_id Int16,
--    order_date DateTime,
--    shipped_date DateTime,
    customer_id String,
    product_id Int16,
    employee_id Int16,
    shipper_id Int16,
    quantity Int16,
    unit_price Float32,
    discount Float32,
    freight Float32,
    total_amount Float32,
    operation CHAR(1),
    updatedate DateTime
)
ENGINE = ReplacingMergeTree(updatedate)
ORDER BY (order_id, product_id);



CREATE MATERIALIZED VIEW default.mv_fact_sales
TO default.fact_sales
AS
SELECT
    o.order_id,
--    parseDateTimeBestEffort(o.order_date) AS order_date,
--    parseDateTimeBestEffort(o.shipped_date) AS shipped_date,
    o.customer_id,
    od.product_id,
    o.employee_id,
    o.ship_via AS shipper_id,
    od.quantity,
    od.unit_price,
    od.discount,
    o.freight,
    od.unit_price * od.quantity * (1 - od.discount) AS total_amount,
    o.operation,
    o.updatedate
FROM northwind.northwind_orders o
INNER JOIN northwind.northwind_order_details od
ON o.order_id = od.order_id;
