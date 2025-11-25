from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import DateType
from datetime import date, timedelta

# ---------- Setup ----------
clickhouse_url = "jdbc:ch://clickhouse1:8123/default"
clickhouse_user = "default"
clickhouse_password = "123456"
clickhouse_driver = "com.clickhouse.jdbc.ClickHouseDriver"

spark = SparkSession.builder \
    .appName("northwind_star_schema_exact_model") \
    .master("local[*]") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ---------- Read CH Tables ----------
def read_ch(table):
    return spark.read.format("jdbc") \
        .option("driver", clickhouse_driver) \
        .option("url", clickhouse_url) \
        .option("user", clickhouse_user) \
        .option("password", clickhouse_password) \
        .option("dbtable", table) \
        .load()

# ---------- Load Source ----------
df_customers = read_ch("northwind.northwind_customers")
df_orders = read_ch("northwind.northwind_orders")
df_order_details = read_ch("northwind.northwind_order_details")
df_products = read_ch("northwind.northwind_products")
df_suppliers = read_ch("northwind.northwind_suppliers")
df_employees = read_ch("northwind.northwind_employees")
df_shippers = read_ch("northwind.northwind_shippers")
df_categories = read_ch("northwind.northwind_categories")
df_employee_territories = read_ch("northwind.northwind_employee_territories")  # mapping
df_territories_master = read_ch("northwind.northwind_territories")              # details
df_regions = read_ch("northwind.northwind_region")

# ============================================================
# ======================= DimGeography ========================
# ============================================================

geo_all = (
    df_customers.select("country", "region", "city", "postal_code", "address")
    .union(df_employees.select("country", "region", "city", "postal_code", "address"))
)

DimGeography = geo_all.dropDuplicates() \
    .withColumn("GeographyKey", monotonically_increasing_id()) \
    .select("GeographyKey", "country", "region", "city", "postal_code", "address")

# ============================================================
# ======================= DimCustomer =========================
# ============================================================

DimCustomer = df_customers.join(
    DimGeography,
    on=["country", "region", "city", "postal_code", "address"],
    how="left"
).withColumn("CustomerKey", monotonically_increasing_id()) \
 .select(
    "CustomerKey",
    col("customer_id").alias("CustomerAlternateKey"),
    "GeographyKey",
    col("company_name").alias("CompanyName"),
    col("contact_name").alias("ContactName"),
    col("contact_title").alias("ContactTitle"),
    "phone",
    "fax",
)

# ============================================================
# ======================= DimEmployees ========================
# ============================================================

DimEmployees = df_employees.join(
    DimGeography,
    on=["country", "region", "city", "postal_code", "address"],
    how="left"
).withColumn("EmployeeKey", monotonically_increasing_id()) \
 .select(
    "EmployeeKey",
    col("reports_to").alias("ParentEmployeeKey"),
    "GeographyKey",
    col("first_name").alias("FirstName"),
    col("last_name").alias("LastName"),
    "title",
    "title_of_courtesy",
    "birth_date",
    "hire_date",
    "home_phone",
    "extension",
    "photo",
    "notes",
    "photo_path",
)

# ============================================================
# ======================= DimSuppliers ========================
# ============================================================

DimSuppliers = df_suppliers.join(
    DimGeography,
    on=["country", "region", "city", "postal_code", "address"],
    how="left"
).withColumn("SupplierKey", monotonically_increasing_id()) \
 .select(
    "SupplierKey",
    col("supplier_id").alias("SupplierAlternateKey"),
    "GeographyKey",
    "company_name",
    "contact_name",
    "contact_title",
    "phone",
    "fax",
    "homepage",
)

# ============================================================
# ======================== DimProducts ========================
# ============================================================

DimProducts = df_products.join(df_categories, "category_id", "left") \
    .withColumn("ProductKey", monotonically_increasing_id()) \
    .select(
        "ProductKey",
        col("product_id").alias("ProductAlternateKey"),
        col("supplier_id").alias("SupplierKey"),
        col("product_name").alias("ProductName"),
        col("category_name").alias("CategoryName"),
        "quantity_per_unit",
        "unit_price",
        "units_in_stock",
        "units_on_order",
        "reorder_level",
        "discontinued",
    )

# ============================================================
# ======================== DimShippers ========================
# ============================================================

DimShippers = df_shippers.withColumn("ShipperKey", monotonically_increasing_id()) \
    .select(
        "ShipperKey",
        col("shipper_id").alias("ShipperAlternateKey"),
        "company_name",
        "phone"
    )

# ============================================================
# ========================== DimDate ==========================
# ============================================================

start_date = date(2020, 1, 1)
end_date = date(2025, 12, 31)
dates = []
current = start_date
while current <= end_date:
    dates.append((current,))
    current += timedelta(days=1)

DimDate = spark.createDataFrame(dates, ["FullDate"]) \
    .withColumn("DateKey", date_format("FullDate", "yyyyMMdd").cast("int")) \
    .withColumn("FullDateAlternateKey", col("FullDate")) \
    .withColumn("CalendarYear", year("FullDate")) \
    .withColumn("MonthNumberOfYear", month("FullDate")) \
    .withColumn("DayNumberOfMonth", dayofmonth("FullDate")) \
    .withColumn("DayOfWeekName", date_format("FullDate", "EEEE"))

# ============================================================
# ======================= DimTerritories ======================
# ============================================================

DimTerritories = df_territories_master.withColumn("TerritoryKey", monotonically_increasing_id()) \
    .select(
        current_timestamp().alias("StartDate"),
        current_timestamp().alias("EndDate"),
        col("territory_description").alias("TerritoryDescription"),
        col("territory_id").alias("TerritoryAlternateKey"),
        col("TerritoryKey")
    )

# ============================================================
# ================== FactEmployeeTerritories ==================
# ============================================================

FactEmployeeTerritories = df_employee_territories.alias("fet") \
    .join(DimEmployees.alias("e"), col("fet.employee_id") == col("e.EmployeeKey"), "left") \
    .join(DimTerritories.alias("t"), col("fet.territory_id") == col("t.TerritoryAlternateKey"), "left") \
    .select(
        col("e.EmployeeKey"),
        col("t.TerritoryKey")
    )

# ============================================================
# ========================== FactOrders =======================
# ============================================================

FactOrders = df_order_details.alias("od").join(df_orders.alias("o"), "order_id") \
    .join(DimCustomer.alias("c"), col("o.customer_id") == col("c.CustomerAlternateKey")) \
    .join(DimEmployees.alias("e"), col("o.employee_id") == col("e.EmployeeKey"), "left") \
    .join(DimProducts.alias("p"), col("od.product_id") == col("p.ProductAlternateKey")) \
    .join(DimShippers.alias("s"), col("o.ship_via") == col("s.ShipperAlternateKey"), "left") \
    .join(DimDate.alias("d"), col("o.order_date") == col("d.DateKey"), "left") \
    .select(
        monotonically_increasing_id().alias("OrderID"),
        col("o.order_date").alias("order_date"),
        col("o.required_date").alias("required_date"),
        col("o.shipped_date").alias("shipped_date"),
        col("c.CustomerKey"),
        col("e.EmployeeKey"),
        col("p.ProductKey"),
        col("s.ShipperKey"),
        col("o.order_id").alias("OrderAlternateKey"),
        col("od.quantity"),
        col("od.unit_price"),  
        (col("od.quantity") * col("od.unit_price")).alias("TotalAmount"),
        col("o.freight")
    )

# ============================================================
# ================== Show Tables (Preview) ====================
# ============================================================

DimGeography.show(5, truncate=False)
DimCustomer.show(5, truncate=False)
DimEmployees.show(5, truncate=False)
DimSuppliers.show(5, truncate=False)
DimShippers.show(5, truncate=False)
DimProducts.show(5, truncate=False)
DimDate.show(5, truncate=False)
DimTerritories.show(5, truncate=False)
FactEmployeeTerritories.show(5, truncate=False)
FactOrders.show(5, truncate=False)
