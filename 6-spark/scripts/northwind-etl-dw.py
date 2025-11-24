# cdc_to_dw.py
# Single-file PySpark pipeline:
#  - Streams Debezium CDC from Kafka -> staging tables in ClickHouse
#  - Provides a batch function `load_star_schema()` to transform staging -> DW (star schema)
#
# Reference diagram (your image): /mnt/data/DW-sem.PNG

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, get_json_object, current_timestamp, lit,
    row_number, concat_ws, sha2
)
from pyspark.sql.window import Window
from pyspark.sql.types import *
import time

# ---------- Configuration ----------
kafka_bootstrap_servers = "kafka:9092"
clickhouse_url = "jdbc:ch://clickhouse1:8123/default"
clickhouse_user = "default"
clickhouse_password = "123456"
clickhouse_driver = "com.clickhouse.jdbc.ClickHouseDriver"

# Mapping of source short names -> (dw_table_name_or_source_table, kafka_topic, schema)
# You already have this in norhwind_schemas; ensure table_mapping exists and contains source names like 'customers','orders','order_items','products','employees', etc.
from norhwind_schemas import table_mapping  # expects dict: short_name: (table_name, topic, schema)

# ---------- Spark setup ----------
spark = SparkSession.builder \
    .appName("cdc_to_star_dw") \
    .master("local[*]") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ---------- Helper: Read Debezium JSON payload ----------
def readDataFromTopic(topic, schema):
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )
    json_df = df.selectExpr("CAST(value AS STRING) as json_value")
    # Debezium typically wraps payload => use get_json_object to extract payload json then parse
    parsed_df = json_df.select(from_json(get_json_object(col("json_value"), "$.payload"), schema).alias("payload"),
                               col("json_value"))
    return parsed_df

def transformDebeziumPayload(parsed_df):
    parsed_df = parsed_df.filter(col("payload").isNotNull())
    # If payload contains "before" and "after", common Debezium pattern, prefer "after" for current row
    # We will check fields in payload schema; to keep generic, if payload has 'after' use that, else use payload.* directly
    # Try to expand payload.after.* if exists
    payload_schema = parsed_df.schema["payload"].dataType
    if "after" in [f.name for f in payload_schema.fields]:
        final_df = parsed_df.select(
            col("payload.after.*"),
            col("payload.op").alias("operation"),
            col("payload.ts_ms").alias("ts_ms"),
            current_timestamp().alias("updatedate"),
            col("json_value")
        )
    else:
        # Flatten payload fields to top-level
        select_exprs = []
        for f in payload_schema.fields:
            # include everything except metadata fields
            if f.name not in ["__deleted", "__op", "__ts_ms"]:
                select_exprs.append(col(f"payload.{f.name}").alias(f.name))
        select_exprs += [
            col("payload.__op").alias("operation"),
            current_timestamp().alias("updatedate"),
            col("json_value")
        ]
        final_df = parsed_df.select(*select_exprs)
    return final_df

# ---------- Streaming: write staging tables ----------
def foreach_batch_to_staging(batch_df, batch_id, table_name=None):
    # table_name passed via closure; write to stg_<table_name>
    stg_table = f"stg_{table_name}"
    # optionally cache if used multiple times
    count = batch_df.count()
    print(f"[staging] Batch {batch_id} -> {stg_table}: {count} rows")
    batch_df.write \
        .format("jdbc") \
        .option("driver", clickhouse_driver) \
        .option("url", clickhouse_url) \
        .option("user", clickhouse_user) \
        .option("password", clickhouse_password) \
        .option("dbtable", stg_table) \
        .mode("append") \
        .save()

# ---------- Utility: read ClickHouse table into DataFrame ----------
def read_ch_table(table_name):
    return spark.read \
        .format("jdbc") \
        .option("driver", clickhouse_driver) \
        .option("url", clickhouse_url) \
        .option("user", clickhouse_user) \
        .option("password", clickhouse_password) \
        .option("dbtable", table_name) \
        .load()

# ---------- SCD2 helper for a generic dimension ----------
def scd2_upsert_dim(stg_df, dim_table_name, business_key_col, dim_columns, alternate_key_col_name, surrogate_key_col):
    """
    stg_df: staging DataFrame that contains the business_key_col and columns listed in dim_columns (source column names)
    dim_table_name: existing dim table in ClickHouse (DimCustomer, etc.)
    business_key_col: column in staging that represents the business key (e.g., CustomerID)
    dim_columns: list of columns to store in dimension (target names)
    alternate_key_col_name: name to store business key in DW (e.g., CustomerAlternateKey)
    surrogate_key_col: name of surrogate key column in dimension (e.g., CustomerKey)
    """
    # Read existing dim
    try:
        dim_df = read_ch_table(dim_table_name)
    except Exception:
        # If table does not exist, create empty frame
        dim_df = spark.createDataFrame([], schema=StructType([]))

    # Normalize names: target columns mapping
    # stg_df must have columns corresponding to dim_columns or aliases are applied prior to calling.

    # Prepare incoming "current" snapshot from staging: keep latest event per business key by updatedate
    stg_snapshot = stg_df.withColumn("row_hash", sha2(concat_ws("||", *[col(c).cast("string") for c in dim_columns]), 256))
    w = Window.partitionBy(business_key_col).orderBy(col("updatedate").desc())
    stg_latest = stg_snapshot.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")

    # Build dimension key mapping: existing business keys with current rows
    if "":  # placeholder to satisfy linter
        pass

    if len(dim_df.columns) == 0:
        # No existing dim -> create initial surrogate keys starting from 1
        existing_max = 0
        existing_current = spark.createDataFrame([], schema=stg_latest.schema)
        dim_existing_current = spark.createDataFrame([], schema=StructType([]))
    else:
        # Ensure dim_df has necessary columns: surrogate_key_col, alternate_key_col_name, is_current, row_hash (if available), start_date, end_date
        dim_existing_current = dim_df.filter(col("is_current") == 1).select(surrogate_key_col, alternate_key_col_name, "is_current", "row_hash", "start_date", "end_date")
        max_key_row = dim_df.select(surrogate_key_col).agg({surrogate_key_col: "max"}).collect()
        existing_max = max_key_row[0][0] if max_key_row and max_key_row[0][0] is not None else 0

    # Produce candidates: join stg_latest to existing_current on business key
    if len(dim_df.columns) == 0:
        # All incoming are new
        new_candidates = stg_latest
        changed = new_candidates.withColumn("is_new", lit(True))
        unchanged = spark.createDataFrame([], schema=new_candidates.schema)
        updates = spark.createDataFrame([], schema=new_candidates.schema)
    else:
        joined = stg_latest.alias("s").join(
            dim_existing_current.alias("d"),
            col(f"s.{business_key_col}") == col(f"d.{alternate_key_col_name}"),
            how="left"
        )

        # If row_hash differs or d.row_hash is null => changed/new
        changed = joined.filter( (col("d.row_hash").isNull()) | (col("s.row_hash") != col("d.row_hash")) )
        unchanged = joined.filter( (col("d.row_hash").isNotNull()) & (col("s.row_hash") == col("d.row_hash")) )

        # changed contains columns from s.* and d.*. Keep columns from s for new version creation.
        changed = changed.select([f"s.{c}" if c in stg_latest.columns else col(c) for c in stg_latest.columns])

    # Now assign surrogate keys for changed/new rows
    if changed.count() == 0:
        print(f"[scd2] No changes detected for {dim_table_name}")
    else:
        # Determine start surrogate_id
        max_key_val = existing_max or 0
        # Assign incremental surrogate keys to changed rows deterministically within this micro-batch
        w2 = Window.orderBy(col(business_key_col))
        changed_with_sk = changed.withColumn("rn_tmp", row_number().over(w2)) \
                                 .withColumn(surrogate_key_col, (col("rn_tmp") + lit(max_key_val)).cast("long")) \
                                 .drop("rn_tmp")
        # Add SCD2 metadata
        changed_final = changed_with_sk \
            .withColumn(alternate_key_col_name, col(business_key_col)) \
            .withColumn("start_date", current_timestamp()) \
            .withColumn("end_date", lit(None).cast("timestamp")) \
            .withColumn("is_current", lit(1)) \
            .withColumn("row_hash", sha2(concat_ws("||", *[col(c).cast("string") for c in dim_columns]), 256)) \
            .select([surrogate_key_col, alternate_key_col_name] + dim_columns + ["start_date", "end_date", "is_current", "row_hash"])

        # For existing current rows that changed, mark them as not current (end_date now)
        if len(dim_df.columns) == 0:
            dim_updates = spark.createDataFrame([], schema=StructType([]))
        else:
            # Build list of business keys that changed
            changed_keys = changed.select(business_key_col).distinct()
            dim_to_close = dim_df.filter(col("is_current") == 1) \
                                 .join(changed_keys, dim_df[alternate_key_col_name] == changed_keys[business_key_col], how="inner") \
                                 .select(dim_df.columns)
            if dim_to_close.count() > 0:
                # Add end_date and is_current=0
                dim_updates = dim_to_close.withColumn("end_date", current_timestamp()).withColumn("is_current", lit(0))
            else:
                dim_updates = spark.createDataFrame([], schema=StructType([]))

        # Write updates (set old rows is_current=0) and then insert new rows
        if len(dim_updates.columns) > 0:
            # ClickHouse doesn't support updates easily via JDBC; one common pattern is:
            # - Insert the updated rows (with same surrogate key) into a table engine that supports replacements (ReplacingMergeTree)
            # Ensure your dim table uses ReplacingMergeTree on row versioning column
            print(f"[scd2] Marking {dim_to_close.count()} old rows as not current in {dim_table_name}")
            dim_updates.write.format("jdbc") \
                .option("driver", clickhouse_driver) \
                .option("url", clickhouse_url) \
                .option("user", clickhouse_user) \
                .option("password", clickhouse_password) \
                .option("dbtable", dim_table_name) \
                .mode("append") \
                .save()

        # Insert new rows
        print(f"[scd2] Inserting {changed_final.count()} new rows into {dim_table_name}")
        changed_final.write.format("jdbc") \
            .option("driver", clickhouse_driver) \
            .option("url", clickhouse_url) \
            .option("user", clickhouse_user) \
            .option("password", clickhouse_password) \
            .option("dbtable", dim_table_name) \
            .mode("append") \
            .save()

# ---------- Build Fact Orders (example) ----------
def build_fact_orders():
    # Read staging orders and order_items
    stg_orders = read_ch_table("stg_orders")
    stg_order_items = read_ch_table("stg_order_items")  # if separate topic/table exists

    # Read dimension current rows (is_current = 1)
    dim_customer = read_ch_table("DimCustomer").filter(col("is_current") == 1).select("CustomerKey", "CustomerAlternateKey")
    dim_product = read_ch_table("DimProducts").filter(col("is_current") == 1).select("ProductKey", "ProductAlternateKey")
    dim_employee = read_ch_table("DimEmployees").filter(col("is_current") == 1).select("EmployeeKey", "EmployeeAlternateKey")
    dim_shippers = read_ch_table("DimShippers").select("ShipperKey", "ShipperAlternateKey")  # maybe no SCD

    # Join to replace alternate keys with surrogate keys
    items = stg_order_items.alias("oi") \
        .join(dim_product.alias("p"), col("oi.ProductID") == col("p.ProductAlternateKey"), how="left") \
        .join(stg_orders.alias("o"), col("oi.OrderID") == col("o.OrderID"), how="inner") \
        .join(dim_customer.alias("c"), col("o.CustomerID") == col("c.CustomerAlternateKey"), how="left") \
        .join(dim_employee.alias("e"), col("o.EmployeeID") == col("e.EmployeeAlternateKey"), how="left") \
        .join(dim_shippers.alias("s"), col("o.ShipVia") == col("s.ShipperAlternateKey"), how="left")

    fact_items = items.select(
        col("oi.OrderID").alias("order_id"),
        col("p.ProductKey").alias("product_key"),
        col("c.CustomerKey").alias("customer_key"),
        col("e.EmployeeKey").alias("employee_key"),
        col("s.ShipperKey").alias("shipper_key"),
        col("oi.UnitPrice").alias("unit_price"),
        col("oi.Quantity").alias("quantity"),
        col("oi.Discount").alias("discount"),
        (col("oi.UnitPrice") * col("oi.Quantity") * (1 - col("oi.Discount"))).alias("line_total"),
        current_timestamp().alias("updated_at")
    )

    # Write fact_order_items
    fact_items.write.format("jdbc") \
        .option("driver", clickhouse_driver) \
        .option("url", clickhouse_url) \
        .option("user", clickhouse_user) \
        .option("password", clickhouse_password) \
        .option("dbtable", "FactOrderItems") \
        .mode("append") \
        .save()

    # Optionally create aggregated fact_orders (per order) or write fact_orders from stg_orders + aggregation
    fact_orders = fact_items.groupBy("order_id", "customer_key", "employee_key", "shipper_key") \
        .agg(
            {"line_total": "sum"}
        ) \
        .withColumnRenamed("sum(line_total)", "total_amount") \
        .withColumn("updated_at", current_timestamp())

    fact_orders.write.format("jdbc") \
        .option("driver", clickhouse_driver) \
        .option("url", clickhouse_url) \
        .option("user", clickhouse_user) \
        .option("password", clickhouse_password) \
        .option("dbtable", "FactOrders") \
        .mode("append") \
        .save()

# ---------- Main DW load entrypoint ----------
def load_star_schema():
    """
    This function performs:
     - dim loads (SCD2) for Customer/Product/Employee (pattern)
     - fact loads (orders + order_items)
    """
    print("[dw] Starting star schema load...")

    # Example: DimCustomer
    stg_customers = read_ch_table("stg_customers")
    # Ensure stg_customers contains columns: CustomerID, CompanyName, ContactName, City, Country, updatedate
    scd2_upsert_dim(
        stg_df=stg_customers.withColumnRenamed("CustomerID", "CustomerID"),  # ensure name
        dim_table_name="DimCustomer",
        business_key_col="CustomerID",
        dim_columns=["CompanyName", "ContactName", "City", "Country"],  # fields to version
        alternate_key_col_name="CustomerAlternateKey",
        surrogate_key_col="CustomerKey"
    )

    # Example: DimProducts
    stg_products = read_ch_table("stg_products")
    scd2_upsert_dim(
        stg_df=stg_products.withColumnRenamed("ProductID", "ProductID"),
        dim_table_name="DimProducts",
        business_key_col="ProductID",
        dim_columns=["ProductName", "Category", "UnitPrice"],
        alternate_key_col_name="ProductAlternateKey",
        surrogate_key_col="ProductKey"
    )

    # Example: DimEmployees
    stg_employees = read_ch_table("stg_employees")
    scd2_upsert_dim(
        stg_df=stg_employees.withColumnRenamed("EmployeeID", "EmployeeID"),
        dim_table_name="DimEmployees",
        business_key_col="EmployeeID",
        dim_columns=["FirstName", "LastName", "Title", "HireDate"],
        alternate_key_col_name="EmployeeAlternateKey",
        surrogate_key_col="EmployeeKey"
    )

    # Add similar SCD2 calls for Suppliers, Shippers, Geography, Territories as needed

    # Build Fact tables
    build_fact_orders()

    print("[dw] Star schema load completed.")

# ---------- Start streaming to staging using original approach ----------
streams = []

for short_name, (table, topic, schema) in table_mapping.items():
    parsed = readDataFromTopic(topic, schema)
    transformed = transformDebeziumPayload(parsed)

    # create a foreachBatch closure bound to the table name
    def make_foreach(table_name):
        def foreach_batch(batch_df, batch_id):
            # write batch to staging table prefix
            foreach_batch_to_staging(batch_df, batch_id, table_name=table_name)
        return foreach_batch

    stream = transformed.writeStream \
        .foreachBatch(make_foreach(table)) \
        .outputMode("append") \
        .option("checkpointLocation", f"/tmp/spark_checkpoints/{table}") \
        .start()

    streams.append(stream)

# ---------- Orchestration ----------
# Option A: Run streaming continuously and periodically trigger DW loads (e.g., every N minutes).
# Option B: Run streaming, then when you want to load DW, call load_star_schema() (manual or via scheduler).

# Example: run streaming in background threads and trigger DW load every X seconds (simple loop).
# WARNING: In production use Airflow / cron / orchestration. This is a simple approach for demonstration.

def run_with_periodic_dw_load(period_seconds=300):
    try:
        print("[main] Streaming started. Periodic DW loads every", period_seconds, "seconds.")
        last_dw = 0
        while True:
            # check streams are active
            for s in streams:
                if not s.isActive:
                    print("[main] stream inactive, stopping.")
                    raise RuntimeError("Stream stopped unexpectedly")

            now = time.time()
            if now - last_dw >= period_seconds:
                print("[main] Triggering DW load...")
                load_star_schema()
                last_dw = now
            time.sleep(5)
    except KeyboardInterrupt:
        print("Stopping streaming by user")
    finally:
        for s in streams:
            s.stop()
        spark.stop()

if __name__ == "__main__":
    # Run with periodic DW loads every 5 minutes (adjust as needed)
    run_with_periodic_dw_load(period_seconds=300)
