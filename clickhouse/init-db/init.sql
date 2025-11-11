-- ایجاد دیتابیس
CREATE DATABASE IF NOT EXISTS northwind ON CLUSTER replicated_cluster;

-- ایجاد جدول با ReplicatedMergeTree
CREATE TABLE IF NOT EXISTS northwind.northwind_region
ON CLUSTER replicated_cluster
(
    region_id Int16,
    region_description String,
    operation CHAR(1),
    updatedate DateTime,
    is_deleted UInt8
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/northwind/northwind_region/{replica}',
    '{replica}'
)
ORDER BY region_id;


