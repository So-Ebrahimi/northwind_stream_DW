-- Create database
CREATE DATABASE IF NOT EXISTS northwind ON CLUSTER replicated_cluster;

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

