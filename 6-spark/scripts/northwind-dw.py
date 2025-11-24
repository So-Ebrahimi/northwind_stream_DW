from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, monotonically_increasing_id, year, month, dayofmonth, dayofweek
from pyspark.sql.types import DateType

# ---------- Configuration ----------
clickhouse_url = "jdbc:ch://clickhouse1:8123/default"
clickhouse_user = "default"
clickhouse_password = "123456"
clickhouse_driver = "com.clickhouse.jdbc.ClickHouseDriver"

# ---------- Spark Setup ----------
spark = SparkSession.builder \
    .appName("northwind_star_schema_surrogate_keys") \
    .master("local[*]") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ---------- Read ClickHouse Table ----------
def read_ch_table(table_name):
    return spark.read \
        .format("jdbc") \
        .option("driver", clickhouse_driver) \
        .option("url", clickhouse_url) \
        .option("user", clickhouse_user) \
        .option("password", clickhouse_password) \
        .option("dbtable", table_name) \
        .load()

# ---------- Source Tables ----------
df_customers = read_ch_table("northwind.northwind_customers")
df_orders = read_ch_table("northwind.northwind_orders")
df_order_details = read_ch_table("northwind.northwind_order_details")
df_products = read_ch_table("northwind.northwind_products")
df_suppliers = read_ch_table("northwind.northwind_suppliers")
df_employees = read_ch_table("northwind.northwind_employees")
df_shippers = read_ch_table("northwind.northwind_shippers")
df_categories = read_ch_table("northwind.northwind_categories")

# ---------- DimGeography ----------
geo_customers = df_customers.select("country", "region", "city", "postal_code", "address")
geo_employees = df_employees.select("country", "region", "city", "postal_code", "address")

dim_geography = geo_customers.union(geo_employees) \
    .dropDuplicates(["country", "region", "city", "postal_code", "address"]) \
    .withColumn("GeographyKey", monotonically_increasing_id()) \
    .withColumn("StartDate", current_timestamp()) \
    .withColumn("EndDate", current_timestamp())

# ---------- DimCustomer ----------
dim_customers = df_customers.join(
    dim_geography,
    on=["country", "region", "city", "postal_code", "address"],
    how="left"
).select(
    monotonically_increasing_id().alias("CustomerKey"),
    col("customer_id").alias("CustomerAlternateKey"),
    col("GeographyKey"),
    col("company_name").alias("CompanyName"),
    col("contact_name").alias("ContactName"),
    current_timestamp().alias("StartDate"),
    current_timestamp().alias("EndDate")
)

# ---------- DimEmployees ----------
dim_employees = df_employees.join(
    dim_geography,
    on=["country", "region", "city", "postal_code", "address"],
    how="left"
).select(
    monotonically_increasing_id().alias("EmployeeKey"),
    col("reports_to").alias("ParentEmployeeKey"),
    col("GeographyKey"),
    col("first_name").alias("FirstName"),
    col("last_name").alias("LastName"),
    col("title").alias("Title"),
    col("hire_date").alias("HireDate"),
    current_timestamp().alias("StartDate"),
    current_timestamp().alias("EndDate")
)

# ---------- DimSuppliers ----------
dim_suppliers = df_suppliers.join(
    dim_geography,
    on=["country", "region", "city", "postal_code", "address"],
    how="left"
).select(
    monotonically_increasing_id().alias("SupplierKey"),
    col("supplier_id").alias("SupplierAlternateKey"),
    col("GeographyKey"),
    col("company_name").alias("CompanyName"),
    col("contact_name").alias("ContactName"),
    current_timestamp().alias("StartDate"),
    current_timestamp().alias("EndDate")
)

# ---------- DimProducts ----------
dim_products = df_products.join(
    df_categories,
    on="category_id",
    how="left"
).select(
    monotonically_increasing_id().alias("ProductKey"),
    col("product_id").alias("ProductAlternateKey"),
    col("supplier_id").alias("SupplierKey"),
    col("category_name").alias("CategoryName"),
    col("unit_price").alias("UnitPrice"),
    col("units_in_stock").alias("UnitsInStock"),
    current_timestamp().alias("StartDate"),
    current_timestamp().alias("EndDate")
)

# ---------- DimShippers ----------
dim_shippers = df_shippers.select(
    monotonically_increasing_id().alias("ShipperKey"),
    col("shipper_id").alias("ShipperAlternateKey"),
    col("company_name").alias("CompanyName"),
    col("phone").alias("Phone")
)

# ---------- DimDate ----------
dim_date = df_orders.select(
    col("order_date").cast(DateType()).alias("FullDate")
).dropDuplicates(["FullDate"]) \
    .withColumn("DateKey", monotonically_increasing_id()) \
    .withColumn("FullDateAlternateKey", col("FullDate")) \
    .withColumn("CalendarYear", year(col("FullDate"))) \
    .withColumn("MonthNumber", month(col("FullDate"))) \
    .withColumn("DayNumber", dayofmonth(col("FullDate"))) \
    .withColumn("DayOfWeek", dayofweek(col("FullDate")))

# ---------- DimTerritories (example, using regions from geography) ----------
dim_territories = dim_geography.select(
    monotonically_increasing_id().alias("TerritoryKey"),
    col("region").alias("RegionDescription"),
    current_timestamp().alias("StartDate"),
    current_timestamp().alias("EndDate")
)

# ---------- FactOrders ----------
# Join fact_orders to surrogate keys
fact_orders = df_order_details.join(df_orders, on="order_id") \
    .join(dim_customers, df_orders.customer_id == dim_customers.CustomerAlternateKey) \
    .join(dim_employees, df_orders.employee_id == dim_employees.EmployeeKey, how="left") \
    .join(dim_products, df_order_details.product_id == dim_products.ProductAlternateKey) \
    .join(dim_shippers, df_orders.ship_via == dim_shippers.ShipperAlternateKey, how="left") \
    .select(
        col("order_id").alias("OrderKey"),
        col("CustomerKey"),
        col("EmployeeKey"),
        col("ProductKey"),
        col("ShipperKey"),
        col("order_date").alias("OrderDate"),
        col("quantity"),
        col("unit_price"),
        (col("quantity") * col("unit_price")).alias("TotalAmount")
    )

# ---------- Show Tables ----------
dim_customers.show(5, truncate=False)
dim_employees.show(5, truncate=False)
dim_suppliers.show(5, truncate=False)
dim_products.show(5, truncate=False)
dim_shippers.show(5, truncate=False)
dim_geography.show(5, truncate=False)
dim_date.show(5, truncate=False)
dim_territories.show(5, truncate=False)
fact_orders.show(5, truncate=False)
