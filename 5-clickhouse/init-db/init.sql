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
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/northwind/northwind_categories/{replica}',
    '{replica}'
)
ORDER BY category_id;

-- CREATE TABLE IF NOT EXISTS northwind.northwind_customer_customer_demo
-- ON CLUSTER replicated_cluster
-- (
--     customer_id String,
--     customer_type_id String,
--     operation CHAR(1),
--     updatedate DateTime
-- )
-- ENGINE = ReplicatedMergeTree(
--     '/clickhouse/tables/northwind/northwind_customer_customer_demo/{replica}',
--     '{replica}'
--      updatedate
-- )
-- ORDER BY (customer_id, customer_type_id);

-- CREATE TABLE IF NOT EXISTS northwind.northwind_customer_demographics
-- ON CLUSTER replicated_cluster
-- (
--     customer_type_id String,
--     customer_desc Nullable(String),
--     operation CHAR(1),
--     updatedate DateTime
-- )
-- ENGINE = ReplicatedMergeTree(
--     '/clickhouse/tables/northwind/northwind_customer_demographics/{replica}',
--     '{replica}'
--      updatedate
-- )
-- ORDER BY customer_type_id;

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
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/northwind/northwind_customers/{replica}',
    '{replica}'
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
    birth_date Nullable(String),
    hire_date Nullable(String),
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
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/northwind/northwind_employees/{replica}',
    '{replica}'
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
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/northwind/northwind_employee_territories/{replica}',
    '{replica}'
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
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/northwind/northwind_order_details/{replica}',
    '{replica}'
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
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/northwind/northwind_orders/{replica}',
    '{replica}'
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
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/northwind/northwind_products/{replica}',
    '{replica}'
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
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/northwind/northwind_region/{replica}',
    '{replica}'
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
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/northwind/northwind_shippers/{replica}',
    '{replica}'
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
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/northwind/northwind_suppliers/{replica}',
    '{replica}'
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
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/northwind/northwind_territories/{replica}',
    '{replica}'
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
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/northwind/northwind_us_states/{replica}',
    '{replica}'
 )
ORDER BY state_id;


CREATE TABLE IF NOT EXISTS DimGeography (
    GeographyKey UInt64,
    country String,
    region String,
    city String,
    postal_code String,
    address String
) ENGINE = MergeTree()
ORDER BY GeographyKey;


CREATE TABLE DimCustomer (
    CustomerKey Int64,
    CustomerAlternateKey String,
    GeographyKey UInt64,
    CompanyName String,
    ContactName String,
    ContactTitle String,
    phone String,
    fax Nullable(String),
    updatedate DateTime
) ENGINE = MergeTree()
ORDER BY CustomerKey;



CREATE TABLE IF NOT EXISTS DimEmployees (
    EmployeeKey Int64,
    EmployeeAlternateKey String,
    ParentEmployeeKey Nullable(Int64),
    GeographyKey UInt64,
    FirstName String,
    LastName String,
    title String,
    title_of_courtesy String,
    birth_date Int64,
    hire_date Int64,
    home_phone String,
    extension String,
    photo String,
    notes String,
    photo_path String,
    updatedate DateTime
) ENGINE = MergeTree()
ORDER BY EmployeeKey;


CREATE TABLE IF NOT EXISTS DimSuppliers (
    SupplierKey Int64,
    SupplierAlternateKey String,
    GeographyKey UInt64,
    company_name String,
    contact_name String,
    contact_title String,
    phone String,
    fax  Nullable(String),
    homepage Nullable(String),
    updatedate DateTime
) ENGINE = MergeTree()
ORDER BY SupplierKey;



CREATE TABLE IF NOT EXISTS DimProducts (
    ProductKey Int64,
    ProductAlternateKey String,
    SupplierKey Nullable(Int64),
    ProductName String,
    CategoryName String,
    quantity_per_unit String,
    unit_price Float32,
    units_in_stock Int32,
    units_on_order Int32,
    reorder_level Int32,
    discontinued UInt8,
    updatedate DateTime
) ENGINE = MergeTree()
ORDER BY ProductKey;


CREATE TABLE IF NOT EXISTS DimShippers (
    ShipperKey Int64,
    ShipperAlternateKey String,
    company_name String,
    phone String,
    updatedate DateTime
) ENGINE = MergeTree()
ORDER BY ShipperKey;



CREATE TABLE IF NOT EXISTS FactOrders (
    FactOrderKey Int64,
    OrderAlternateKey Int64,
    CustomerKey Int64,
    EmployeeKey Int64,
    ProductKey Int64,
    ShipperKey Int64,
    OrderDate Int64,
    DueDate Int64,
    ShipDate Nullable(Int64),
    Quantity Int32,
    UnitPrice Float32,
    TotalAmount Float32,
    Freight Float32,
    updatedate DateTime
)
ENGINE = ReplacingMergeTree(updatedate)
ORDER BY (OrderAlternateKey, ProductKey);


CREATE TABLE IF NOT EXISTS DimDate
(
    DateKey     UInt32,
    Date        Date,
    FullDate    String,
    DayOfWeek   UInt8,
    DayName     String,
    DayOfMonth  UInt8,
    DayOfYear   UInt16,
    WeekOfYear  UInt8,
    Month       UInt8,
    MonthName   String,
    Quarter     UInt8,
    Year        UInt16,
    IsWeekend   UInt8
)
ENGINE = MergeTree()
ORDER BY DateKey;