# incremental_etl.py
import os
import sys
import logging
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import TimestampType
import time 
from pyspark.sql.window import Window

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ------------- config -------------
CLICKHOUSE_URL = "jdbc:ch://clickhouse1:8123/default"
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASS = "123456"
CLICKHOUSE_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"

LAST_RUN_FILE = "last_run.txt"   
POLL_INTERVAL_SEC = 20         
# ----------------------------------

# Initialize Spark session with error handling
try:
    spark = SparkSession.builder \
        .appName("northwind_incremental_etl") \
        .getOrCreate()
    logger.info("Spark session created successfully")
except Exception as e:
    logger.error(f"Failed to create Spark session: {e}", exc_info=True)
    sys.exit(1)

# Set log level for Spark to reduce noise, but keep our logging
spark.sparkContext.setLogLevel("WARN")

def read_ch_table(table, where_clause=None):
    """
    Read data from ClickHouse table.
    
    Args:
        table: Table name to read from
        where_clause: Optional WHERE clause for filtering
        
    Returns:
        DataFrame with table data
        
    Raises:
        Exception: If read operation fails
    """
    try:
        logger.debug(f"Reading from ClickHouse table: {table}" + (f" with WHERE: {where_clause}" if where_clause else ""))
        reader = spark.read.format("jdbc") \
            .option("driver", CLICKHOUSE_DRIVER) \
            .option("url", CLICKHOUSE_URL) \
            .option("user", CLICKHOUSE_USER) \
            .option("password", CLICKHOUSE_PASS) \
            .option("dbtable", table)
        if where_clause:
            # use subquery to apply WHERE when driver needs
            sql = f"(SELECT * FROM {table} WHERE {where_clause} ) as t"
            reader = reader.option("dbtable", sql)
        df = reader.load()
        logger.debug(f"Successfully read {df.count()} rows from {table}")
        return df
    except Exception as e:
        logger.error(f"Failed to read from ClickHouse table {table}: {e}", exc_info=True)
        raise

def write_ch_table(df, table_name, mode="append"):
    """
    Write DataFrame to ClickHouse table.
    
    Args:
        df: DataFrame to write
        table_name: Target table name
        mode: Write mode (append, overwrite, etc.)
        
    Raises:
        Exception: If write operation fails
    """
    try:
        row_count = df.count()
        logger.debug(f"Writing {row_count} rows to ClickHouse table: {table_name} (mode: {mode})")
        
        df.write.format("jdbc") \
            .option("driver", CLICKHOUSE_DRIVER) \
            .option("url", CLICKHOUSE_URL) \
            .option("user", CLICKHOUSE_USER) \
            .option("password", CLICKHOUSE_PASS) \
            .option("dbtable", table_name) \
            .mode(mode) \
            .save()
        
        logger.debug(f"Successfully wrote {row_count} rows to {table_name}")
    except Exception as e:
        logger.error(f"Failed to write to ClickHouse table {table_name}: {e}", exc_info=True)
        raise

def table_has_rows(table_name):
    """
    Returns True if the ClickHouse table already contains data.
    
    Args:
        table_name: Table name to check
        
    Returns:
        True if table has rows, False otherwise
    """
    try:
        logger.debug(f"Checking if table {table_name} has rows")
        df = spark.read.format("jdbc") \
            .option("driver", CLICKHOUSE_DRIVER) \
            .option("url", CLICKHOUSE_URL) \
            .option("user", CLICKHOUSE_USER) \
            .option("password", CLICKHOUSE_PASS) \
            .option("dbtable", f"(SELECT 1 FROM {table_name} LIMIT 1) t") \
            .load()
        has_rows = not df.rdd.isEmpty()
        logger.debug(f"Table {table_name} has rows: {has_rows}")
        return has_rows
    except Exception as exc:
        logger.warning(f"Could not query {table_name} to check for rows: {exc}")
        return False


def get_max_key(table_name, key_column):
    """
    Retrieve the current maximum surrogate key from a ClickHouse table.
    Returns 0 if the table is empty or cannot be queried.
    """
    try:
        df = spark.read.format("jdbc") \
            .option("driver", CLICKHOUSE_DRIVER) \
            .option("url", CLICKHOUSE_URL) \
            .option("user", CLICKHOUSE_USER) \
            .option("password", CLICKHOUSE_PASS) \
            .option("dbtable", f"(SELECT max({key_column}) AS max_key FROM {table_name}) t") \
            .load()
        row = df.first()
        if row and row["max_key"] is not None:
            return int(row["max_key"])
    except Exception as exc:
        logger.warning(f"Could not read max({key_column}) from {table_name}: {exc}")
    return 0


def assign_incremental_keys(df, key_column, natural_key_cols, target_table, existing_df=None):
    """
    Assign incremental surrogate keys based on existing keys in the target table.
    - Reuses existing keys when natural keys already exist.
    - Generates new keys starting after the current max key.
    """
    try:
        # Pull existing mappings if none provided
        if existing_df is None:
            try:
                existing_df = read_ch_table(target_table).select(
                    col(key_column), *[col(c) for c in natural_key_cols]
                )
                if existing_df.rdd.isEmpty():
                    existing_df = None
            except Exception as exc:
                logger.debug(f"{target_table}: could not load existing keys: {exc}")
                existing_df = None

        if existing_df is not None:
            df = df.join(existing_df, on=natural_key_cols, how="left")
        else:
            df = df.withColumn(key_column, lit(None).cast("long"))

        # New rows without a surrogate key
        new_rows = df.filter(col(key_column).isNull())
        if not new_rows.rdd.isEmpty():
            offset = get_max_key(target_table, key_column)
            window = Window.orderBy(*[col(c) for c in natural_key_cols])
            new_rows = new_rows.withColumn(
                key_column,
                (row_number().over(window) + lit(offset)).cast("long")
            )
            existing_rows = df.filter(col(key_column).isNotNull())
            df = existing_rows.unionByName(new_rows, allowMissingColumns=True)

        df = df.withColumn(key_column, col(key_column).cast("long"))
        return df
    except Exception as exc:
        logger.error(f"Failed to assign incremental keys for {target_table}: {exc}", exc_info=True)
        raise

def initialize_dim_date():
    """
    Populates DimDate with dates 1970-01-01 .. 2050-12-31 on the first run.
    
    Raises:
        Exception: If initialization fails
    """
    try:
        if table_has_rows("DimDate"):
            logger.info("DimDate already populated - skipping initialization")
            return

        logger.info("Initializing DimDate table")
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
        row_count = dim_date.count()
        logger.info(f"DimDate initialized with {row_count} rows")
        
    except Exception as e:
        logger.error(f"Failed to initialize DimDate: {e}", exc_info=True)
        raise

def get_last_run():
    """
    Get the last run timestamp from file.
    
    Returns:
        Last run timestamp string, or default (10 years ago) if file doesn't exist
        
    Raises:
        Exception: If file read fails
    """
    try:
        if not os.path.exists(LAST_RUN_FILE):
            start = (datetime.now(timezone.utc) - timedelta(days=3650)).strftime("%Y-%m-%d %H:%M:%S")
            logger.info(f"Last run file not found, using default timestamp: {start}")
            return start
        with open(LAST_RUN_FILE, "r") as f:
            last_run = f.read().strip()
            logger.debug(f"Read last run timestamp: {last_run}")
            return last_run
    except Exception as e:
        logger.error(f"Failed to read last run file {LAST_RUN_FILE}: {e}", exc_info=True)
        # Return default on error
        start = (datetime.now(timezone.utc) - timedelta(days=3650)).strftime("%Y-%m-%d %H:%M:%S")
        logger.warning(f"Using default timestamp due to error: {start}")
        return start

def set_last_run(ts):
    """
    Save the last run timestamp to file.
    
    Args:
        ts: Timestamp string to save
        
    Raises:
        Exception: If file write fails
    """
    try:
        logger.debug(f"Writing last run timestamp: {ts}")
        with open(LAST_RUN_FILE, "w") as f:
            f.write(ts)
        logger.debug(f"Successfully saved last run timestamp to {LAST_RUN_FILE}")
    except Exception as e:
        logger.error(f"Failed to write last run file {LAST_RUN_FILE}: {e}", exc_info=True)
        raise

# ---------------- transformations for dimensions ----------------
def process_dim_customers(last_run):
    """
    Process DimCustomer dimension table.
    
    Args:
        last_run: Last run timestamp for incremental processing
        
    Raises:
        Exception: If processing fails
    """
    try:
        logger.info("Processing DimCustomer")
        where = f"updatedate > toDateTime('{last_run}') and operation != 'd'" 
        df_changed = read_ch_table("northwind.northwind_customers", where)
        if df_changed.rdd.isEmpty():
            logger.debug("DimCustomer: no changes")
            return
        
        from pyspark.sql.functions import coalesce, lit
        df_changed = df_changed.withColumn("country", coalesce("country", lit("Unknown"))) \
                               .withColumn("region", coalesce("region", lit("Unknown"))) \
                               .withColumn("city", coalesce("city", lit("Unknown"))) \
                               .withColumn("postal_code", coalesce("postal_code", lit("Unknown"))) \
                               .withColumn("address", coalesce("address", lit("Unknown")))
        geo = df_changed.select("country","region","city","postal_code","address").dropDuplicates()
        geo_out = assign_incremental_keys(
            geo,
            "GeographyKey",
            ["country","region","city","postal_code","address"],
            "DimGeography"
        )
        write_ch_table(geo_out.select("GeographyKey","country","region","city","postal_code","address"), "DimGeography")

        dim_geo = geo_out.select("GeographyKey","country","region","city","postal_code","address")
        dim_customer_base = df_changed.join(dim_geo, on=["country","region","city","postal_code","address"], how="left") \
            .withColumnRenamed("customer_id","CustomerAlternateKey") \
            .select(
                col("CustomerAlternateKey"),
                col("GeographyKey").cast("long"),
                col("company_name").alias("CompanyName"),
                col("contact_name").alias("ContactName"),
                col("contact_title").alias("ContactTitle"),
                col("phone"),
                col("fax"),
                col("updatedate").alias("updatedate")
            )
        dim_customer = assign_incremental_keys(
            dim_customer_base,
            "CustomerKey",
            ["CustomerAlternateKey"],
            "DimCustomer"
        ).select(
            "CustomerKey",
            "CustomerAlternateKey",
            "GeographyKey",
            "CompanyName",
            "ContactName",
            "ContactTitle",
            "phone",
            "fax",
            "updatedate"
        )
        write_ch_table(dim_customer, "DimCustomer")
        row_count = dim_customer.count()
        logger.info(f"DimCustomer: wrote {row_count} rows")
        
    except Exception as e:
        logger.error(f"Failed to process DimCustomer: {e}", exc_info=True)
        raise

def process_dim_employees(last_run):
    """
    Process DimEmployees dimension table.
    
    Args:
        last_run: Last run timestamp for incremental processing
        
    Raises:
        Exception: If processing fails
    """
    try:
        logger.info("Processing DimEmployees")
        where = f"updatedate > toDateTime('{last_run}') and operation != 'd'"
        df_changed = read_ch_table("northwind.northwind_employees", where)
        if df_changed.rdd.isEmpty():
            logger.debug("DimEmployees: no changes")
            return
        
        from pyspark.sql.functions import coalesce, lit
        df_changed = df_changed.withColumn("country", coalesce("country", lit("Unknown"))) \
                               .withColumn("region", coalesce("region", lit("Unknown"))) \
                               .withColumn("city", coalesce("city", lit("Unknown"))) \
                               .withColumn("postal_code", coalesce("postal_code", lit("Unknown"))) \
                               .withColumn("address", coalesce("address", lit("Unknown")))
        dim_geo = df_changed.select("country","region","city","postal_code","address").dropDuplicates()
        dim_geo = assign_incremental_keys(
            dim_geo,
            "GeographyKey",
            ["country","region","city","postal_code","address"],
            "DimGeography"
        )
        write_ch_table(dim_geo.select("GeographyKey","country","region","city","postal_code","address"), "DimGeography")

        dim_emp_base = df_changed.join(dim_geo, on=["country","region","city","postal_code","address"], how="left") \
            .withColumn("EmployeeAlternateKey", col("employee_id").cast("string")) \
            .withColumnRenamed("reports_to","ParentEmployeeKey") \
            .withColumn("birth_date", when(col("birth_date").isNotNull() & (col("birth_date") != ""), col("birth_date").cast("long")).otherwise(lit(None).cast("long"))) \
            .withColumn("hire_date", when(col("hire_date").isNotNull() & (col("hire_date") != ""), col("hire_date").cast("long")).otherwise(lit(None).cast("long"))) \
            .select(
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
        dim_emp = assign_incremental_keys(
            dim_emp_base,
            "EmployeeKey",
            ["EmployeeAlternateKey"],
            "DimEmployees"
        ).select(
            "EmployeeKey",
            "EmployeeAlternateKey",
            "ParentEmployeeKey",
            "GeographyKey",
            "FirstName",
            "LastName",
            "title",
            "title_of_courtesy",
            "birth_date",
            "hire_date",
            "home_phone",
            "extension",
            "photo",
            "notes",
            "photo_path",
            "updatedate"
        )
        write_ch_table(dim_emp, "DimEmployees")
        row_count = dim_emp.count()
        logger.info(f"DimEmployees: wrote {row_count} rows")
        
    except Exception as e:
        logger.error(f"Failed to process DimEmployees: {e}", exc_info=True)
        raise

def process_dim_suppliers(last_run):
    """
    Process DimSuppliers dimension table.
    
    Args:
        last_run: Last run timestamp for incremental processing
        
    Raises:
        Exception: If processing fails
    """
    try:
        logger.info("Processing DimSuppliers")
        where = f"updatedate > toDateTime('{last_run}') and operation != 'd'"
        df_changed = read_ch_table("northwind.northwind_suppliers", where)
        if df_changed.rdd.isEmpty():
            logger.debug("DimSuppliers: no changes")
            return
        
        from pyspark.sql.functions import coalesce, lit
        df_changed = df_changed.withColumn("country", coalesce("country", lit("Unknown"))) \
                               .withColumn("region", coalesce("region", lit("Unknown"))) \
                               .withColumn("city", coalesce("city", lit("Unknown"))) \
                               .withColumn("postal_code", coalesce("postal_code", lit("Unknown"))) \
                               .withColumn("address", coalesce("address", lit("Unknown")))
        dim_geo = df_changed.select("country","region","city","postal_code","address").dropDuplicates()
        dim_geo = assign_incremental_keys(
            dim_geo,
            "GeographyKey",
            ["country","region","city","postal_code","address"],
            "DimGeography"
        )
        write_ch_table(dim_geo.select("GeographyKey","country","region","city","postal_code","address"), "DimGeography")

        dim_sup_base = df_changed.join(dim_geo, on=["country","region","city","postal_code","address"], how="left") \
            .withColumn("SupplierAlternateKey", col("supplier_id").cast("string")) \
            .select(
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
        dim_sup = assign_incremental_keys(
            dim_sup_base,
            "SupplierKey",
            ["SupplierAlternateKey"],
            "DimSuppliers"
        ).select(
            "SupplierKey",
            "SupplierAlternateKey",
            "GeographyKey",
            "company_name",
            "contact_name",
            "contact_title",
            "phone",
            "fax",
            "homepage",
            "updatedate"
        )
        write_ch_table(dim_sup, "DimSuppliers")
        row_count = dim_sup.count()
        logger.info(f"DimSuppliers: wrote {row_count} rows")
        
    except Exception as e:
        logger.error(f"Failed to process DimSuppliers: {e}", exc_info=True)
        raise

def process_dim_products(last_run):
    """
    Process DimProducts dimension table.
    
    Args:
        last_run: Last run timestamp for incremental processing
        
    Raises:
        Exception: If processing fails
    """
    try:
        logger.info("Processing DimProducts")
        where = f"updatedate > toDateTime('{last_run}') and operation != 'd'"
        df_changed = read_ch_table("northwind.northwind_products", where)
        if df_changed.rdd.isEmpty():
            logger.debug("DimProducts: no changes")
            return
        
        categories = read_ch_table("northwind.northwind_categories").select("category_id","category_name")
        dim_suppliers = read_ch_table("DimSuppliers").select(
            col("SupplierKey").cast("long"),
            col("SupplierAlternateKey")
        )

        dim_products_base = df_changed.alias("prod") \
            .join(categories.alias("cat"), "category_id", "left") \
            .join(
                dim_suppliers.alias("sup"),
                col("prod.supplier_id").cast("string") == col("sup.SupplierAlternateKey"),
                "left"
            ) \
            .withColumn("ProductAlternateKey", col("prod.product_id").cast("string")) \
            .withColumnRenamed("product_name","ProductName") \
            .select(
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
        dim_products = assign_incremental_keys(
            dim_products_base,
            "ProductKey",
            ["ProductAlternateKey"],
            "DimProducts"
        ).select(
            "ProductKey",
            "ProductAlternateKey",
            "SupplierKey",
            "ProductName",
            "CategoryName",
            "quantity_per_unit",
            "unit_price",
            "units_in_stock",
            "units_on_order",
            "reorder_level",
            "discontinued",
            "updatedate"
        )
        write_ch_table(dim_products, "DimProducts")
        row_count = dim_products.count()
        logger.info(f"DimProducts: wrote {row_count} rows")
        
    except Exception as e:
        logger.error(f"Failed to process DimProducts: {e}", exc_info=True)
        raise

def process_dim_shippers(last_run):
    """
    Process DimShippers dimension table.
    
    Args:
        last_run: Last run timestamp for incremental processing
        
    Raises:
        Exception: If processing fails
    """
    try:
        logger.info("Processing DimShippers")
        where = f"updatedate > toDateTime('{last_run}') and operation != 'd'"
        df_changed = read_ch_table("northwind.northwind_shippers", where)
        if df_changed.rdd.isEmpty():
            logger.debug("DimShippers: no changes")
            return
        
        dim_shippers_base = df_changed.withColumn("ShipperAlternateKey", col("shipper_id").cast("string")) \
            .select(
                col("ShipperAlternateKey"),
                col("company_name"),
                col("phone"),
                col("updatedate").alias("updatedate")
            )
        dim_shippers = assign_incremental_keys(
            dim_shippers_base,
            "ShipperKey",
            ["ShipperAlternateKey"],
            "DimShippers"
        ).select(
            "ShipperKey",
            "ShipperAlternateKey",
            "company_name",
            "phone",
            "updatedate"
        )
        write_ch_table(dim_shippers, "DimShippers")
        row_count = dim_shippers.count()
        logger.info(f"DimShippers: wrote {row_count} rows")
        
    except Exception as e:
        logger.error(f"Failed to process DimShippers: {e}", exc_info=True)
        raise

def process_dim_territories(last_run):
    """
    Process DimTerritories dimension table.
    
    Args:
        last_run: Last run timestamp for incremental processing
        
    Raises:
        Exception: If processing fails
    """
    try:
        logger.info("Processing DimTerritories")
        where = f"updatedate > toDateTime('{last_run}') and operation != 'd'"
        df_changed = read_ch_table("northwind.northwind_territories", where)
        if df_changed.rdd.isEmpty():
            logger.debug("DimTerritories: no changes")
            return
        
        regions = read_ch_table("northwind.northwind_region").select(
            col("region_id"),
            trim(col("region_description")).alias("RegionDescription")
        )
        dim_terr_base = df_changed.alias("terr") \
            .join(regions.alias("reg"), "region_id", "left") \
            .withColumn("TerritoryAlternateKey", col("terr.territory_id").cast("string")) \
            .withColumn("TerritoryDescription", trim(col("terr.territory_description"))) \
            .withColumn("RegionDescription", col("reg.RegionDescription")) \
            .withColumn("StartDate", col("terr.updatedate").cast(TimestampType())) \
            .withColumn("EndDate", lit(None).cast(TimestampType())) \
            .withColumn("updatedate", col("terr.updatedate")) \
            .select(
                "TerritoryAlternateKey",
                "RegionDescription",
                "TerritoryDescription",
                "StartDate",
                "EndDate",
                "updatedate"
            )
        dim_terr = assign_incremental_keys(
            dim_terr_base,
            "TerritoryKey",
            ["TerritoryAlternateKey"],
            "DimTerritories"
        ).select(
            "TerritoryKey",
            "TerritoryAlternateKey",
            "RegionDescription",
            "TerritoryDescription",
            "StartDate",
            "EndDate",
            "updatedate"
        )
        write_ch_table(dim_terr, "DimTerritories")
        row_count = dim_terr.count()
        logger.info(f"DimTerritories: wrote {row_count} rows")
        
    except Exception as e:
        logger.error(f"Failed to process DimTerritories: {e}", exc_info=True)
        raise

# ---------------- Fact processing ----------------
def process_fact_employee_territories(last_run):
    """
    Process FactEmployeeTerritories fact table.
    
    Args:
        last_run: Last run timestamp for incremental processing
        
    Raises:
        Exception: If processing fails
    """
    try:
        logger.info("Processing FactEmployeeTerritories")
        where_clause = f"updatedate > toDateTime('{last_run}') and operation != 'd'"
        df_changed = read_ch_table("northwind.northwind_employee_territories", where_clause)
        if df_changed.rdd.isEmpty():
            logger.debug("FactEmployeeTerritories: no changes")
            return

        try:
            dim_emp = read_ch_table("DimEmployees").select(
                col("EmployeeAlternateKey").alias("EmployeeAlternateKeyDim"),
                col("EmployeeKey")
            )
        except Exception as exc:
            logger.error(f"FactEmployeeTerritories: cannot read DimEmployees: {exc}", exc_info=True)
            return

        try:
            dim_terr = read_ch_table("DimTerritories").select(
                col("TerritoryAlternateKey").alias("TerritoryAlternateKeyDim"),
                col("TerritoryKey")
            )
        except Exception as exc:
            logger.error(f"FactEmployeeTerritories: cannot read DimTerritories: {exc}", exc_info=True)
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
                   .select(
                        "EmployeeKey",
                        "TerritoryKey",
                        "updatedate"
                   ).dropna(subset=["EmployeeKey", "TerritoryKey"])

        existing_pairs = None
        try:
            emp_keys = [str(r["EmployeeKey"]) for r in fact.select("EmployeeKey").distinct().collect()]
            terr_keys = [str(r["TerritoryKey"]) for r in fact.select("TerritoryKey").distinct().collect()]
            if emp_keys and terr_keys:
                where_pairs = f"EmployeeKey IN ({','.join(emp_keys)}) AND TerritoryKey IN ({','.join(terr_keys)})"
                existing_pairs = read_ch_table("FactEmployeeTerritories", where_pairs) \
                    .select("FactEmployeeTerritoryKey", "EmployeeKey", "TerritoryKey")
        except Exception as exc:
            logger.debug(f"FactEmployeeTerritories: could not load existing pairs: {exc}")

        fact = assign_incremental_keys(
            fact,
            "FactEmployeeTerritoryKey",
            ["EmployeeKey", "TerritoryKey"],
            "FactEmployeeTerritories",
            existing_pairs
        ).select(
            "FactEmployeeTerritoryKey",
            "EmployeeKey",
            "TerritoryKey",
            "updatedate"
        )

        row_count = fact.count()
        if row_count == 0:
            logger.warning("FactEmployeeTerritories: nothing to upsert after key resolution")
            return

        write_ch_table(fact, "FactEmployeeTerritories")
        dropped = total_pairs - row_count
        if dropped > 0:
            logger.warning(f"FactEmployeeTerritories: skipped {dropped} pairs missing dimension keys")
        elif dropped < 0:
            logger.debug(f"FactEmployeeTerritories: wrote {row_count} rows (expected {total_pairs} pairs, difference may be due to join behavior)")
        logger.info(f"FactEmployeeTerritories: wrote {row_count} rows")
        
    except Exception as e:
        logger.error(f"Failed to process FactEmployeeTerritories: {e}", exc_info=True)
        raise


def process_fact_orders(last_run):
    """
    Process FactOrders fact table.
    
    Args:
        last_run: Last run timestamp for incremental processing
        
    Raises:
        Exception: If processing fails
    """
    try:
        logger.info("Processing FactOrders")
        where_o = f"updatedate > toDateTime('{last_run}')"
        where_od = f"updatedate > toDateTime('{last_run}')"
        orders_changed = read_ch_table("northwind.northwind_orders", where_o).select("order_id").distinct()
        ods_changed = read_ch_table("northwind.northwind_order_details", where_od).select("order_id").distinct()
        changed_order_ids = orders_changed.union(ods_changed).distinct()
        if changed_order_ids.rdd.isEmpty():
            logger.debug("FactOrders: no changed orders")
            return

        changed_ids_list = [r["order_id"] for r in changed_order_ids.collect()]
        logger.info(f"FactOrders: rebuilding for {len(changed_ids_list)} orders (sample: {changed_ids_list[:5]})")

        ids_csv = ",".join(str(i) for i in changed_ids_list)
        where_full_orders = f"order_id IN ({ids_csv})"
        orders_df = read_ch_table("northwind.northwind_orders", where_full_orders).alias("o")
        od_df = read_ch_table("northwind.northwind_order_details", where_full_orders).alias("od")

        try:
            dim_customer = read_ch_table("DimCustomer").select(col("CustomerAlternateKey"), col("CustomerKey"))
            dim_emp = read_ch_table("DimEmployees").select(col("EmployeeAlternateKey"), col("EmployeeKey"))
            dim_prod = read_ch_table("DimProducts").select(col("ProductAlternateKey"), col("ProductKey"))
            dim_ship = read_ch_table("DimShippers").select(col("ShipperAlternateKey"), col("ShipperKey"))
        except Exception as e:
            logger.error(f"FactOrders: failed to read dimension tables: {e}", exc_info=True)
            raise

        od = od_df.alias("od")
        o = orders_df.alias("o")
        c = dim_customer.alias("c")
        e = dim_emp.alias("e")
        p = dim_prod.alias("p")
        s = dim_ship.alias("s")     

        fact = od.join(o, "order_id") \
            .join(c, col("o.customer_id").cast("string") == c.CustomerAlternateKey, "left") \
            .join(e, col("o.employee_id").cast("string") == e.EmployeeAlternateKey, "left") \
            .join(p, col("od.product_id").cast("string") == p.ProductAlternateKey, "left") \
            .join(s, col("o.ship_via").cast("string") == s.ShipperAlternateKey, "left") \
            .withColumn("OrderDate", when(col("o.order_date").isNotNull(), col("o.order_date").cast("long")).otherwise(lit(None).cast("long"))) \
            .withColumn("DueDate", when(col("o.required_date").isNotNull(), col("o.required_date").cast("long")).otherwise(lit(None).cast("long"))) \
            .withColumn("ShipDate", when(col("o.shipped_date").isNotNull(), col("o.shipped_date").cast("long")).otherwise(lit(None).cast("long"))) \
            .select(
                col("o.order_id").alias("OrderAlternateKey"),
                col("c.CustomerKey"),
                col("e.EmployeeKey"),
                col("p.ProductKey"),
                col("s.ShipperKey"),
                col("OrderDate"),
                col("DueDate"),
                col("ShipDate"),
                col("od.quantity").alias("Quantity"),
                col("od.unit_price").alias("UnitPrice"),
                (col("od.quantity") * col("od.unit_price")).alias("TotalAmount"),
                col("o.freight").alias("Freight"),
                col("o.updatedate").alias("updatedate")
            )

        # Log counts before filtering to help diagnose join issues
        total_before_filter = fact.count()
        null_customer = fact.filter(col("CustomerKey").isNull()).count()
        null_employee = fact.filter(col("EmployeeKey").isNull()).count()
        null_product = fact.filter(col("ProductKey").isNull()).count()
        null_shipper = fact.filter(col("ShipperKey").isNull()).count()
        logger.info(f"FactOrders: before filtering - total: {total_before_filter}, NULL CustomerKey: {null_customer}, NULL EmployeeKey: {null_employee}, NULL ProductKey: {null_product}, NULL ShipperKey: {null_shipper}")
        
        # Filter out rows with NULL dimension keys (required by ClickHouse schema)
        fact = fact.dropna(subset=["CustomerKey", "EmployeeKey", "ProductKey", "ShipperKey"])
        
        if fact.rdd.isEmpty():
            logger.warning("FactOrders: nothing to write after filtering NULL dimension keys")
            return

        existing_fact = None
        try:
            order_keys = [str(r["OrderAlternateKey"]) for r in fact.select("OrderAlternateKey").distinct().collect()]
            product_keys = [str(r["ProductKey"]) for r in fact.select("ProductKey").distinct().collect()]
            if order_keys and product_keys:
                where_pairs = f"OrderAlternateKey IN ({','.join(order_keys)}) AND ProductKey IN ({','.join(product_keys)})"
                existing_fact = read_ch_table("FactOrders", where_pairs) \
                    .select("FactOrderKey", "OrderAlternateKey", "ProductKey")
        except Exception as exc:
            logger.debug(f"FactOrders: could not load existing keys: {exc}")

        fact = assign_incremental_keys(
            fact,
            "FactOrderKey",
            ["OrderAlternateKey", "ProductKey"],
            "FactOrders",
            existing_fact
        ).select(
            "FactOrderKey",
            "OrderAlternateKey",
            "CustomerKey",
            "EmployeeKey",
            "ProductKey",
            "ShipperKey",
            "OrderDate",
            "DueDate",
            "ShipDate",
            "Quantity",
            "UnitPrice",
            "TotalAmount",
            "Freight",
            "updatedate"
        )

        row_count = fact.count()

        write_ch_table(fact, "FactOrders")
        logger.info(f"FactOrders: wrote {row_count} rows")
        
    except Exception as e:
        logger.error(f"Failed to process FactOrders: {e}", exc_info=True)
        raise

# ---------------- main run ----------------
def main_once():
    """
    Execute one ETL run.
    
    Raises:
        Exception: If ETL run fails
    """
    try:
        last_run = get_last_run()
        logger.info(f"Starting ETL run with last_run = {last_run}")
        now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        # ensure static dimensions are seeded
        try:
            initialize_dim_date()
        except Exception as e:
            logger.error(f"Failed to initialize DimDate: {e}", exc_info=True)
            raise

        # process dimensions (only those with updatedate)
        dimension_processors = [
            ("DimCustomer", process_dim_customers),
            ("DimEmployees", process_dim_employees),
            ("DimSuppliers", process_dim_suppliers),
            ("DimProducts", process_dim_products),
            ("DimShippers", process_dim_shippers),
            ("DimTerritories", process_dim_territories),
        ]
        
        for dim_name, processor in dimension_processors:
            try:
                processor(last_run)
            except Exception as e:
                logger.error(f"Failed to process {dim_name}: {e}", exc_info=True)
                # Continue with other dimensions instead of failing completely
                continue

        # process facts
        fact_processors = [
            ("FactEmployeeTerritories", process_fact_employee_territories),
            ("FactOrders", process_fact_orders),
        ]
        
        for fact_name, processor in fact_processors:
            try:
                processor(last_run)
            except Exception as e:
                logger.error(f"Failed to process {fact_name}: {e}", exc_info=True)
                # Continue with other facts instead of failing completely
                continue

        # update last_run only after successful processing
        try:
            set_last_run(now_ts)
            logger.info(f"ETL run completed successfully. Updated last_run -> {now_ts}")
        except Exception as e:
            logger.error(f"Failed to update last_run timestamp: {e}", exc_info=True)
            # Don't raise - the processing was successful
            
    except Exception as e:
        logger.error(f"Fatal error in ETL run: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    try:
        logger.info("Starting incremental ETL job")
        # Optionally: run in a loop (or schedule via cron/systemd)
        while True:
            try:
                main_once()
            except KeyboardInterrupt:
                logger.info("Received interrupt signal. Stopping ETL job...")
                break
            except Exception as e:
                logger.error(f"Error in ETL run, will retry after {POLL_INTERVAL_SEC} seconds: {e}", exc_info=True)
                # Continue loop to retry
            time.sleep(POLL_INTERVAL_SEC)
    except Exception as e:
        logger.error(f"Fatal error in ETL job: {e}", exc_info=True)
        sys.exit(1)
