# incremental_etl.py
import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import TimestampType
import time 

# ------------- config -------------
CLICKHOUSE_URL = "jdbc:ch://clickhouse1:8123/default"
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASS = "123456"
CLICKHOUSE_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"

LAST_RUN_FILE = "last_run.txt"   # فایل ذخیره زمان آخرین اجرا
POLL_INTERVAL_SEC = 20          # اگر می‌خوای خودت اسکریپت رو loop کنی (پیشنهاد: cron یا scheduler)
# ----------------------------------

spark = SparkSession.builder \
    .appName("northwind_incremental_etl") \
    .master("local[*]") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

def read_ch_table(table, where_clause=None):
    reader = spark.read.format("jdbc") \
        .option("driver", CLICKHOUSE_DRIVER) \
        .option("url", CLICKHOUSE_URL) \
        .option("user", CLICKHOUSE_USER) \
        .option("password", CLICKHOUSE_PASS) \
        .option("dbtable", table)
    if where_clause:
        # use subquery to apply WHERE when driver needs
        sql = f"(SELECT * FROM {table} WHERE {where_clause}) as t"
        reader = reader.option("dbtable", sql)
    return reader.load()

def write_ch_table(df, table_name, mode="append"):
    df.write.format("jdbc") \
        .option("driver", CLICKHOUSE_DRIVER) \
        .option("url", CLICKHOUSE_URL) \
        .option("user", CLICKHOUSE_USER) \
        .option("password", CLICKHOUSE_PASS) \
        .option("dbtable", table_name) \
        .mode(mode) \
        .save()

def table_has_rows(table_name):
    """
    Returns True if the ClickHouse table already contains data.
    """
    try:
        df = spark.read.format("jdbc") \
            .option("driver", CLICKHOUSE_DRIVER) \
            .option("url", CLICKHOUSE_URL) \
            .option("user", CLICKHOUSE_USER) \
            .option("password", CLICKHOUSE_PASS) \
            .option("dbtable", f"(SELECT 1 FROM {table_name} LIMIT 1) t") \
            .load()
        return not df.rdd.isEmpty()
    except Exception as exc:
        print(f"[table_has_rows] Could not query {table_name}: {exc}")
        return False

def initialize_dim_date():
    """
    Populates DimDate with dates 1970-01-01 .. 2050-12-31 on the first run.
    """
    if table_has_rows("DimDate"):
        print("DimDate already populated - skipping initialization")
        return

    start_date = datetime(1970, 1, 1)
    end_date = datetime(2050, 12, 31)
    total_days = (end_date - start_date).days + 1
    start_literal = start_date.strftime("%Y-%m-%d")

    dim_date = spark.range(0, total_days, 1, numPartitions=1) \
        .withColumn("DateValue", expr(f"date_add('{start_literal}', cast(id as int))")) \
        .drop("id") \
        .withColumn(
            "DateKey",
            (year("DateValue") * 10000 + month("DateValue") * 100 + dayofmonth("DateValue")).cast("int")
        ) \
        .withColumn("FullDate", date_format("DateValue", "yyyy-MM-dd")) \
        .withColumn("DayOfWeek", dayofweek("DateValue")) \
        .withColumn("DayName", date_format("DateValue", "EEEE")) \
        .withColumn("DayOfMonth", dayofmonth("DateValue")) \
        .withColumn("DayOfYear", dayofyear("DateValue")) \
        .withColumn("WeekOfYear", weekofyear("DateValue")) \
        .withColumn("Month", month("DateValue")) \
        .withColumn("MonthName", date_format("DateValue", "MMMM")) \
        .withColumn("Quarter", quarter("DateValue")) \
        .withColumn("Year", year("DateValue")) \
        .withColumn("IsWeekend", when(dayofweek("DateValue").isin(1, 7), lit(1)).otherwise(lit(0)))

    dim_date = dim_date.select(
        col("DateKey"),
        col("DateValue").alias("Date"),
        "FullDate",
        "DayOfWeek",
        "DayName",
        "DayOfMonth",
        "DayOfYear",
        "WeekOfYear",
        "Month",
        "MonthName",
        "Quarter",
        "Year",
        "IsWeekend"
    )

    write_ch_table(dim_date, "DimDate", mode="append")
    print(f"DimDate initialized with {dim_date.count()} rows")

def get_last_run():
    if not os.path.exists(LAST_RUN_FILE):
        # اولین اجرا: یک بازه کوتاه قبل تا همه رکوردها را نگیریم (یا می‌توان full load انجام داد)
        start = (datetime.utcnow() - timedelta(days=3650)).strftime("%Y-%m-%d %H:%M:%S")
        return start
    with open(LAST_RUN_FILE, "r") as f:
        return f.read().strip()

def set_last_run(ts):
    with open(LAST_RUN_FILE, "w") as f:
        f.write(ts)

# ---------------- transformations for dimensions ----------------
def process_dim_customers(last_run):
    where = f"updatedate > toDateTime('{last_run}')"  # فرض: updatedate از نوع DateTime در ClickHouse
    df_changed = read_ch_table("northwind.northwind_customers", where)
    if df_changed.rdd.isEmpty():
        print("DimCustomer: no changes")
        return
    # clean geo like in original code
    from pyspark.sql.functions import coalesce, lit
    df_changed = df_changed.withColumn("country", coalesce("country", lit("Unknown"))) \
                           .withColumn("region", coalesce("region", lit("Unknown"))) \
                           .withColumn("city", coalesce("city", lit("Unknown"))) \
                           .withColumn("postal_code", coalesce("postal_code", lit("Unknown"))) \
                           .withColumn("address", coalesce("address", lit("Unknown")))
    # build geography rows (we will write new geography rows too)
    geo = df_changed.select("country","region","city","postal_code","address").dropDuplicates()
    # assign GeographyKey (here: we upsert by writing full rows to DimGeography; using Replace strategy in CH)
    geo_out = geo.withColumn("GeographyKey", expr("hash(country,region,city,postal_code,address)"))
    write_ch_table(geo_out.select("GeographyKey","country","region","city","postal_code","address"), "DimGeography")

    # join to generate dimensional customer rows
    # read existing DimGeography (to resolve keys) - could instead compute same hash
    dim_geo = geo_out.select("GeographyKey","country","region","city","postal_code","address")
    dim_customer = df_changed.join(dim_geo, on=["country","region","city","postal_code","address"], how="left") \
        .withColumn("CustomerKey", expr("hash(customer_id)")) \
        .withColumnRenamed("customer_id","CustomerAlternateKey") \
        .select(
            col("CustomerKey").cast("long"),
            col("CustomerAlternateKey"),
            col("GeographyKey").cast("long"),
            col("company_name").alias("CompanyName"),
            col("contact_name").alias("ContactName"),
            col("contact_title").alias("ContactTitle"),
            col("phone"),
            col("fax"),
            col("updatedate").alias("updatedate")
        )
    # write: append. ClickHouse table should be ReplacingMergeTree(updatedate) and ORDER BY CustomerAlternateKey
    write_ch_table(dim_customer, "DimCustomer")
    print(f"DimCustomer: wrote {dim_customer.count()} rows")

def process_dim_employees(last_run):
    where = f"updatedate > toDateTime('{last_run}')"
    df_changed = read_ch_table("northwind.northwind_employees", where)
    if df_changed.rdd.isEmpty():
        print("DimEmployees: no changes")
        return
    from pyspark.sql.functions import coalesce, lit
    df_changed = df_changed.withColumn("country", coalesce("country", lit("Unknown"))) \
                           .withColumn("region", coalesce("region", lit("Unknown"))) \
                           .withColumn("city", coalesce("city", lit("Unknown"))) \
                           .withColumn("postal_code", coalesce("postal_code", lit("Unknown"))) \
                           .withColumn("address", coalesce("address", lit("Unknown")))
    dim_geo = df_changed.select("country","region","city","postal_code","address").dropDuplicates() \
                        .withColumn("GeographyKey", expr("hash(country,region,city,postal_code,address)"))
    write_ch_table(dim_geo.select("GeographyKey","country","region","city","postal_code","address"), "DimGeography")

    dim_emp = df_changed.join(dim_geo, on=["country","region","city","postal_code","address"], how="left") \
        .withColumn("EmployeeKey", expr("hash(employee_id)")) \
        .withColumnRenamed("employee_id","EmployeeAlternateKey") \
        .withColumnRenamed("reports_to","ParentEmployeeKey") \
        .select(
            col("EmployeeKey").cast("long"),
            col("EmployeeAlternateKey"),
            col("ParentEmployeeKey"),
            col("GeographyKey").cast("long"),
            col("first_name").alias("FirstName"),
            col("last_name").alias("LastName"),
            col("title"),
            col("title_of_courtesy"),
            col("birth_date"),
            col("hire_date"),
            col("home_phone"),
            col("extension"),
            col("photo"),
            col("notes"),
            col("photo_path"),
            col("updatedate").alias("updatedate")
        )
    write_ch_table(dim_emp, "DimEmployees")
    print(f"DimEmployees: wrote {dim_emp.count()} rows")

def process_dim_suppliers(last_run):
    where = f"updatedate > toDateTime('{last_run}')"
    df_changed = read_ch_table("northwind.northwind_suppliers", where)
    if df_changed.rdd.isEmpty():
        print("DimSuppliers: no changes")
        return
    from pyspark.sql.functions import coalesce, lit
    df_changed = df_changed.withColumn("country", coalesce("country", lit("Unknown"))) \
                           .withColumn("region", coalesce("region", lit("Unknown"))) \
                           .withColumn("city", coalesce("city", lit("Unknown"))) \
                           .withColumn("postal_code", coalesce("postal_code", lit("Unknown"))) \
                           .withColumn("address", coalesce("address", lit("Unknown")))
    dim_geo = df_changed.select("country","region","city","postal_code","address").dropDuplicates() \
                        .withColumn("GeographyKey", expr("hash(country,region,city,postal_code,address)"))
    write_ch_table(dim_geo.select("GeographyKey","country","region","city","postal_code","address"), "DimGeography")

    dim_sup = df_changed.join(dim_geo, on=["country","region","city","postal_code","address"], how="left") \
        .withColumn("SupplierKey", expr("hash(supplier_id)")) \
        .withColumnRenamed("supplier_id","SupplierAlternateKey") \
        .select(
            col("SupplierKey").cast("long"),
            col("SupplierAlternateKey"),
            col("GeographyKey").cast("long"),
            col("company_name"),
            col("contact_name"),
            col("contact_title"),
            col("phone"),
            col("fax"),
            col("homepage"),
            col("updatedate").alias("updatedate")
        )
    write_ch_table(dim_sup, "DimSuppliers")
    print(f"DimSuppliers: wrote {dim_sup.count()} rows")

def process_dim_products(last_run):
    where = f"p.updatedate > toDateTime('{last_run}')"
    # need join with categories and suppliers - we'll read changed products and join full categories/suppliers
    df_changed = read_ch_table("northwind.northwind_products p", where)
    if df_changed.rdd.isEmpty():
        print("DimProducts: no changes")
        return
    # read categories (small) and resolve SupplierKey from dimension table
    categories = read_ch_table("northwind.northwind_categories").select("category_id","category_name")
    dim_suppliers = read_ch_table("DimSuppliers").select(
        col("SupplierKey").cast("long"),
        col("SupplierAlternateKey")
    )

    dim_products = df_changed.alias("prod") \
        .join(categories.alias("cat"), "category_id", "left") \
        .join(
            dim_suppliers.alias("sup"),
            col("prod.supplier_id") == col("sup.SupplierAlternateKey"),
            "left"
        ) \
        .withColumn("ProductKey", expr("hash(prod.product_id)")) \
        .withColumnRenamed("product_id","ProductAlternateKey") \
        .withColumnRenamed("product_name","ProductName") \
        .select(
            col("ProductKey").cast("long"),
            col("ProductAlternateKey"),
            col("sup.SupplierKey").alias("SupplierKey"),
            col("ProductName"),
            col("cat.category_name").alias("CategoryName"),
            "quantity_per_unit",
            "unit_price",
            "units_in_stock",
            "units_on_order",
            "reorder_level",
            "discontinued",
            col("updatedate").alias("updatedate")
        )
    write_ch_table(dim_products, "DimProducts")
    print(f"DimProducts: wrote {dim_products.count()} rows")

def process_dim_shippers(last_run):
    where = f"updatedate > toDateTime('{last_run}')"
    df_changed = read_ch_table("northwind.northwind_shippers", where)
    if df_changed.rdd.isEmpty():
        print("DimShippers: no changes")
        return
    dim_shippers = df_changed.withColumn("ShipperKey", expr("hash(shipper_id)")) \
        .withColumnRenamed("shipper_id","ShipperAlternateKey") \
        .select(
            col("ShipperKey").cast("long"),
            col("ShipperAlternateKey"),
            col("company_name"),
            col("phone"),
            col("updatedate").alias("updatedate")
        )
    write_ch_table(dim_shippers, "DimShippers")
    print(f"DimShippers: wrote {dim_shippers.count()} rows")

def process_dim_territories(last_run):
    where = f"updatedate > toDateTime('{last_run}')"
    df_changed = read_ch_table("northwind.northwind_territories", where)
    if df_changed.rdd.isEmpty():
        print("DimTerritories: no changes")
        return
    regions = read_ch_table("northwind.northwind_region").select(
        col("region_id"),
        trim(col("region_description")).alias("RegionDescription")
    )
    dim_terr = df_changed.alias("terr") \
        .join(regions.alias("reg"), "region_id", "left") \
        .withColumn("TerritoryKey", expr("hash(terr.territory_id)")) \
        .withColumn("TerritoryAlternateKey", col("terr.territory_id").cast("string")) \
        .withColumn("TerritoryDescription", trim(col("terr.territory_description"))) \
        .withColumn("RegionDescription", col("reg.RegionDescription")) \
        .withColumn("StartDate", col("terr.updatedate").cast(TimestampType())) \
        .withColumn("EndDate", lit(None).cast(TimestampType())) \
        .withColumn("updatedate", col("terr.updatedate")) \
        .select(
            col("TerritoryKey").cast("long"),
            "TerritoryAlternateKey",
            "RegionDescription",
            "TerritoryDescription",
            "StartDate",
            "EndDate",
            "updatedate"
        )
    write_ch_table(dim_terr, "DimTerritories")
    print(f"DimTerritories: wrote {dim_terr.count()} rows")

# ---------------- Fact processing ----------------
def process_fact_employee_territories(last_run):
    where_clause = f"updatedate > toDateTime('{last_run}')"
    df_changed = read_ch_table("northwind.northwind_employee_territories", where_clause)
    if df_changed.rdd.isEmpty():
        print("FactEmployeeTerritories: no changes")
        return

    try:
        dim_emp = read_ch_table("DimEmployees").select(
            col("EmployeeAlternateKey").alias("EmployeeAlternateKeyDim"),
            col("EmployeeKey")
        )
    except Exception as exc:
        print(f"FactEmployeeTerritories: cannot read DimEmployees ({exc})")
        return

    try:
        dim_terr = read_ch_table("DimTerritories").select(
            col("TerritoryAlternateKey").alias("TerritoryAlternateKeyDim"),
            col("TerritoryKey")
        )
    except Exception as exc:
        print(f"FactEmployeeTerritories: cannot read DimTerritories ({exc})")
        return

    pairs = df_changed.select(
        col("employee_id").cast("string").alias("EmployeeAlternateKey"),
        col("territory_id").cast("string").alias("TerritoryAlternateKey"),
        col("updatedate")
    ).groupBy("EmployeeAlternateKey", "TerritoryAlternateKey") \
     .agg(max("updatedate").alias("updatedate"))
    total_pairs = pairs.count()

    fact = pairs \
        .join(dim_emp, pairs.EmployeeAlternateKey == dim_emp.EmployeeAlternateKeyDim, "left") \
        .join(dim_terr, pairs.TerritoryAlternateKey == dim_terr.TerritoryAlternateKeyDim, "left") \
        .drop("EmployeeAlternateKeyDim", "TerritoryAlternateKeyDim") \
        .drop("EmployeeAlternateKey", "TerritoryAlternateKey")

    fact = fact.withColumn("EmployeeKey", col("EmployeeKey").cast("long")) \
               .withColumn("TerritoryKey", col("TerritoryKey").cast("long")) \
               .withColumn(
                    "FactEmployeeTerritoryKey",
                    expr("hash(EmployeeKey, TerritoryKey)").cast("long")
               ) \
               .select(
                    "FactEmployeeTerritoryKey",
                    "EmployeeKey",
                    "TerritoryKey",
                    "updatedate"
               ).dropna(subset=["EmployeeKey", "TerritoryKey"])

    row_count = fact.count()
    if row_count == 0:
        print("FactEmployeeTerritories: nothing to upsert after key resolution")
        return

    write_ch_table(fact, "FactEmployeeTerritories")
    dropped = total_pairs - row_count
    if dropped:
        print(f"FactEmployeeTerritories: skipped {dropped} pairs missing dimension keys")
    print(f"FactEmployeeTerritories: wrote {row_count} rows")


def process_fact_orders(last_run):
    # 1) find all order_ids changed in orders or order_details since last_run
    where_o = f"updatedate > toDateTime('{last_run}')"
    where_od = f"updatedate > toDateTime('{last_run}')"
    orders_changed = read_ch_table("northwind.northwind_orders", where_o).select("order_id").distinct()
    ods_changed = read_ch_table("northwind.northwind_order_details", where_od).select("order_id").distinct()
    changed_order_ids = orders_changed.union(ods_changed).distinct()
    if changed_order_ids.rdd.isEmpty():
        print("FactOrders: no changed orders")
        return

    changed_ids_list = [r["order_id"] for r in changed_order_ids.collect()]
    print(f"FactOrders: rebuilding for {len(changed_ids_list)} orders (sample: {changed_ids_list[:5]})")

    # 2) read the full orders and order_details for those ids
    ids_csv = ",".join(str(i) for i in changed_ids_list)
    where_full_orders = f"order_id IN ({ids_csv})"
    orders_df = read_ch_table("northwind.northwind_orders", where_full_orders).alias("o")
    od_df = read_ch_table("northwind.northwind_order_details", where_full_orders).alias("od")

    # 3) read dimension keys from built dimensions (we assume DimCustomer/DimEmployees/DimProducts/DimShippers are already populated)
    dim_customer = read_ch_table("DimCustomer").select(col("CustomerAlternateKey"), col("CustomerKey"))
    dim_emp = read_ch_table("DimEmployees").select(col("EmployeeAlternateKey"), col("EmployeeKey"))
    dim_prod = read_ch_table("DimProducts").select(col("ProductAlternateKey"), col("ProductKey"))
    dim_ship = read_ch_table("DimShippers").select(col("ShipperAlternateKey"), col("ShipperKey"))

    od = od_df.alias("od")
    o = orders_df.alias("o")
    c = dim_customer.alias("c")
    e = dim_emp.alias("e")
    p = dim_prod.alias("p")
    s = dim_ship.alias("s")     

    fact = od.join(o, "order_id") \
        .join(c, o.customer_id == c.CustomerAlternateKey, "left") \
        .join(e, o.employee_id == e.EmployeeAlternateKey, "left") \
        .join(p, od.product_id == p.ProductAlternateKey, "left") \
        .join(s, o.ship_via == s.ShipperAlternateKey, "left") \
        .withColumn("FactOrderKey", expr("hash(order_id, product_id)").cast("long")) \
        .select(
            col("FactOrderKey"),
            col("o.order_id").alias("OrderAlternateKey"),
            col("c.CustomerKey"),
            col("e.EmployeeKey"),
            col("p.ProductKey"),
            col("s.ShipperKey"),
            col("o.order_date").alias("OrderDate"),
            col("o.required_date").alias("DueDate"),
            col("o.shipped_date").alias("ShipDate"),
            col("od.quantity").alias("Quantity"),
            col("od.unit_price").alias("UnitPrice"),
            (col("od.quantity") * col("od.unit_price")).alias("TotalAmount"),
            col("o.freight").alias("Freight"),
            col("o.updatedate").alias("updatedate")
        )


    # 5) write fact rows (append). ClickHouse table FactOrders should be ReplacingMergeTree(updatedate) with ORDER BY (OrderAlternateKey, ProductKey) or similar
    write_ch_table(fact, "FactOrders")
    print(f"FactOrders: wrote {fact.count()} rows")

# ---------------- main run ----------------
def main_once():
    last_run = get_last_run()
    print("last_run =", last_run)
    now_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # ensure static dimensions are seeded
    initialize_dim_date()

    # process dimensions (only those with updatedate)
    process_dim_customers(last_run)
    process_dim_employees(last_run)
    process_dim_suppliers(last_run)
    process_dim_products(last_run)
    process_dim_shippers(last_run)
    process_dim_territories(last_run)
    # other dims (categories, territories) if you want can be added similarly

    # process facts
    process_fact_employee_territories(last_run)
    process_fact_orders(last_run)

    # update last_run only after successful processing
    set_last_run(now_ts)
    print("Updated last_run ->", now_ts)

if __name__ == "__main__":
    # main_once()
    # Optionally: run in a loop (or schedule via cron/systemd)
    while True:
        main_once()
        time.sleep(POLL_INTERVAL_SEC)
