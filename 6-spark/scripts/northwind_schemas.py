from pyspark.sql.types import *



# ==========================
# categories
# ==========================
categories_schema = StructType([
    StructField("category_id", IntegerType(), True),
    StructField("category_name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("picture", StringType(), True),
    StructField("__deleted", StringType(), True),
    StructField("__op", StringType(), True),
    StructField("__ts_ms", LongType(), True)
])


# ==========================
# customer_customer_demo
# ==========================
customer_customer_demo_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_type_id", StringType(), True),
    StructField("__deleted", StringType(), True),
    StructField("__op", StringType(), True),
    StructField("__ts_ms", LongType(), True)
])


# ==========================
# customer_demographics
# ==========================
customer_demographics_schema = StructType([
    StructField("customer_type_id", StringType(), True),
    StructField("customer_desc", StringType(), True),
    StructField("__deleted", StringType(), True),
    StructField("__op", StringType(), True),
    StructField("__ts_ms", LongType(), True)
])


# ==========================
# customers
# ==========================
customers_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("company_name", StringType(), True),
    StructField("contact_name", StringType(), True),
    StructField("contact_title", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("region", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("fax", StringType(), True),
    StructField("__deleted", StringType(), True),
    StructField("__op", StringType(), True),
    StructField("__ts_ms", LongType(), True)
])


# ==========================
# employees
# ==========================
employees_schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("last_name", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("title", StringType(), True),
    StructField("title_of_courtesy", StringType(), True),
    StructField("birth_date", StringType(), True),
    StructField("hire_date", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("region", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("home_phone", StringType(), True),
    StructField("extension", StringType(), True),
    StructField("photo", StringType(), True),
    StructField("notes", StringType(), True),
    StructField("reports_to", IntegerType(), True),
    StructField("photo_path", StringType(), True),
    StructField("__deleted", StringType(), True),
    StructField("__op", StringType(), True),
    StructField("__ts_ms", LongType(), True)
])


# ==========================
# employee_territories
# ==========================
employee_territories_schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("territory_id", StringType(), True),
    StructField("__deleted", StringType(), True),
    StructField("__op", StringType(), True),
    StructField("__ts_ms", LongType(), True)
])


# ==========================
# order_details
# ==========================
order_details_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("unit_price", FloatType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("discount", FloatType(), True),
    StructField("__deleted", StringType(), True),
    StructField("__op", StringType(), True),
    StructField("__ts_ms", LongType(), True)
])


# ==========================
# orders
# ==========================
orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", StringType(), True),
    StructField("employee_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("required_date", StringType(), True),
    StructField("shipped_date", StringType(), True),
    StructField("ship_via", IntegerType(), True),
    StructField("freight", FloatType(), True),
    StructField("ship_name", StringType(), True),
    StructField("ship_address", StringType(), True),
    StructField("ship_city", StringType(), True),
    StructField("ship_region", StringType(), True),
    StructField("ship_postal_code", StringType(), True),
    StructField("ship_country", StringType(), True),
    StructField("__deleted", StringType(), True),
    StructField("__op", StringType(), True),
    StructField("__ts_ms", LongType(), True)
])


# ==========================
# products
# ==========================
products_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("supplier_id", IntegerType(), True),
    StructField("category_id", IntegerType(), True),
    StructField("quantity_per_unit", StringType(), True),
    StructField("unit_price", FloatType(), True),
    StructField("units_in_stock", IntegerType(), True),
    StructField("units_on_order", IntegerType(), True),
    StructField("reorder_level", IntegerType(), True),
    StructField("discontinued", IntegerType(), True),
    StructField("__deleted", StringType(), True),
    StructField("__op", StringType(), True),
    StructField("__ts_ms", LongType(), True)
])


# ==========================
# region
# ==========================
region_schema = StructType([
    StructField("region_id", IntegerType(), True),
    StructField("region_description", StringType(), True),
    StructField("__deleted", StringType(), True),
    StructField("__op", StringType(), True),
    StructField("__ts_ms", LongType(), True)
])


# ==========================
# shippers
# ==========================
shippers_schema = StructType([
    StructField("shipper_id", IntegerType(), True),
    StructField("company_name", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("__deleted", StringType(), True),
    StructField("__op", StringType(), True),
    StructField("__ts_ms", LongType(), True)
])


# ==========================
# suppliers
# ==========================
suppliers_schema = StructType([
    StructField("supplier_id", IntegerType(), True),
    StructField("company_name", StringType(), True),
    StructField("contact_name", StringType(), True),
    StructField("contact_title", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("region", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("fax", StringType(), True),
    StructField("homepage", StringType(), True),
    StructField("__deleted", StringType(), True),
    StructField("__op", StringType(), True),
    StructField("__ts_ms", LongType(), True)
])


# ==========================
# territories
# ==========================
territories_schema = StructType([
    StructField("territory_id", StringType(), True),
    StructField("territory_description", StringType(), True),
    StructField("region_id", IntegerType(), True),
    StructField("__deleted", StringType(), True),
    StructField("__op", StringType(), True),
    StructField("__ts_ms", LongType(), True)
])


# ==========================
# us_states
# ==========================
us_states_schema = StructType([
    StructField("state_id", IntegerType(), True),
    StructField("state_name", StringType(), True),
    StructField("state_abbr", StringType(), True),
    StructField("state_region", StringType(), True),
    StructField("__deleted", StringType(), True),
    StructField("__op", StringType(), True),
    StructField("__ts_ms", LongType(), True)
])

table_mapping = {
    "categories": ["northwind.northwind_categories", "northwind.public.categories", categories_schema],
    "customers": ["northwind.northwind_customers", "northwind.public.customers", customers_schema],
    "employee_territories": ["northwind.northwind_employee_territories", "northwind.public.employee_territories", employee_territories_schema],
    "employees": ["northwind.northwind_employees", "northwind.public.employees", employees_schema],
    "order_details": ["northwind.northwind_order_details", "northwind.public.order_details", order_details_schema],
    "orders": ["northwind.northwind_orders", "northwind.public.orders", orders_schema],
    "products": ["northwind.northwind_products", "northwind.public.products", products_schema],
    "region": ["northwind.northwind_region", "northwind.public.region", region_schema],
    "shippers": ["northwind.northwind_shippers", "northwind.public.shippers", shippers_schema],
    "suppliers": ["northwind.northwind_suppliers", "northwind.public.suppliers", suppliers_schema],
    "territories": ["northwind.northwind_territories", "northwind.public.territories", territories_schema],
    "us_states": ["northwind.northwind_us_states", "northwind.public.us_states", us_states_schema]
    ##    "customer_customer_demo": ["northwind.northwind_customer_customer_demo", "northwind.public.customer_customer_demo", customer_customer_demo_schema],
##     "customer_demographics": ["northwind.northwind_customer_demographics", "northwind.public.customer_demographics", customer_demographics_schema],
}
