-- ایجاد دیتابیس
CREATE DATABASE IF NOT EXISTS northwind ON CLUSTER replicated_cluster;

-- جدول categories
CREATE TABLE IF NOT EXISTS northwind.northwind_categories
ON CLUSTER replicated_cluster
(
    category_id Int16,
    category_name String,
    description String,
    picture String,
    operation CHAR(1),
    updatedate DateTime

)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/northwind/northwind_categories/{replica}',
    '{replica}'
)
ORDER BY category_id;


-- جدول customer_customer_demo
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
    '{replica}'
)
ORDER BY (customer_id, customer_type_id);


-- جدول customer_demographics
CREATE TABLE IF NOT EXISTS northwind.northwind_customer_demographics
ON CLUSTER replicated_cluster
(
    customer_type_id String,
    customer_desc String,
    operation CHAR(1),
    updatedate DateTime

)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/northwind/northwind_customer_demographics/{replica}',
    '{replica}'
)
ORDER BY customer_type_id;


-- جدول customers
CREATE TABLE IF NOT EXISTS northwind.northwind_customers
ON CLUSTER replicated_cluster
(
    customer_id String,
    company_name String,
    contact_name String,
    contact_title String,
    address String,
    city String,
    region String,
    postal_code String,
    country String,
    phone String,
    fax String,
    operation CHAR(1),
    updatedate DateTime

)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/northwind/northwind_customers/{replica}',
    '{replica}'
)
ORDER BY customer_id;


-- جدول employees
CREATE TABLE IF NOT EXISTS northwind.northwind_employees
ON CLUSTER replicated_cluster
(
    employee_id Int16,
    last_name String,
    first_name String,
    title String,
    title_of_courtesy String,
    birth_date Date,
    hire_date Date,
    address String,
    city String,
    region String,
    postal_code String,
    country String,
    home_phone String,
    extension String,
    photo String,
    notes String,
    reports_to Int16,
    photo_path String,
    operation CHAR(1),
    updatedate DateTime

)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/northwind/northwind_employees/{replica}',
    '{replica}'
)
ORDER BY employee_id;


-- جدول employee_territories
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
    '{replica}'
)
ORDER BY (employee_id, territory_id);


-- جدول order_details
CREATE TABLE IF NOT EXISTS northwind.northwind_order_details
ON CLUSTER replicated_cluster
(
    order_id Int16,
    product_id Int16,
    unit_price Float32,
    quantity Int16,
    discount Float32,
    operation CHAR(1),
    updatedate DateTime

)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/northwind/northwind_order_details/{replica}',
    '{replica}'
)
ORDER BY (order_id, product_id);


-- جدول orders
CREATE TABLE IF NOT EXISTS northwind.northwind_orders
ON CLUSTER replicated_cluster
(
    order_id Int16,
    customer_id String,
    employee_id Int16,
    order_date Date,
    required_date Date,
    shipped_date Date,
    ship_via Int16,
    freight Float32,
    ship_name String,
    ship_address String,
    ship_city String,
    ship_region String,
    ship_postal_code String,
    ship_country String,
    operation CHAR(1),
    updatedate DateTime

)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/northwind/northwind_orders/{replica}',
    '{replica}'
)
ORDER BY order_id;


-- جدول products
CREATE TABLE IF NOT EXISTS northwind.northwind_products
ON CLUSTER replicated_cluster
(
    product_id Int16,
    product_name String,
    supplier_id Int16,
    category_id Int16,
    quantity_per_unit String,
    unit_price Float32,
    units_in_stock Int16,
    units_on_order Int16,
    reorder_level Int16,
    discontinued UInt8,
    operation CHAR(1),
    updatedate DateTime

)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/northwind/northwind_products/{replica}',
    '{replica}'
)
ORDER BY product_id;


-- جدول region
CREATE TABLE IF NOT EXISTS northwind.northwind_region
ON CLUSTER replicated_cluster
(
    region_id Int16,
    region_description String,
    operation CHAR(1),
    updatedate DateTime

)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/northwind/northwind_region/{replica}',
    '{replica}'
)
ORDER BY region_id;


-- جدول shippers
CREATE TABLE IF NOT EXISTS northwind.northwind_shippers
ON CLUSTER replicated_cluster
(
    shipper_id Int16,
    company_name String,
    phone String,
    operation CHAR(1),
    updatedate DateTime

)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/northwind/northwind_shippers/{replica}',
    '{replica}'
)
ORDER BY shipper_id;


-- جدول suppliers
CREATE TABLE IF NOT EXISTS northwind.northwind_suppliers
ON CLUSTER replicated_cluster
(
    supplier_id Int16,
    company_name String,
    contact_name String,
    contact_title String,
    address String,
    city String,
    region String,
    postal_code String,
    country String,
    phone String,
    fax String,
    homepage String,
    operation CHAR(1),
    updatedate DateTime

)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/northwind/northwind_suppliers/{replica}',
    '{replica}'
)
ORDER BY supplier_id;


-- جدول territories
CREATE TABLE IF NOT EXISTS northwind.northwind_territories
ON CLUSTER replicated_cluster
(
    territory_id String,
    territory_description String,
    region_id Int16,
    operation CHAR(1),
    updatedate DateTime

)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/northwind/northwind_territories/{replica}',
    '{replica}'
)
ORDER BY territory_id;


-- جدول us_states
CREATE TABLE IF NOT EXISTS northwind.northwind_us_states
ON CLUSTER replicated_cluster
(
    state_id Int16,
    state_name String,
    state_abbr String,
    state_region String,
    operation CHAR(1),
    updatedate DateTime

)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/northwind/northwind_us_states/{replica}',
    '{replica}'
)
ORDER BY state_id;
