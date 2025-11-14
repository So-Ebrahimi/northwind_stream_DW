from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, get_json_object, when, lit, current_timestamp, struct, to_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
spark = (
    SparkSession.builder
    .appName("KafkaDebeziumClickHouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

kafka_bootstrap_servers = "kafka:29092"
kafka_topic = "northwind.public.region"

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "earliest")
    .load()
)

json_df = df.selectExpr("CAST(value AS STRING) as json_value")

region_schema = StructType([
    StructField("region_id", IntegerType(), True),
    StructField("region_description", StringType(), True),
    StructField("__deleted", StringType(), True),
    StructField("__op", StringType(), True),
    StructField("__ts_ms", LongType(), True)
])

parsed_df = json_df.select(from_json(get_json_object(col("json_value"), "$.payload"), region_schema).alias("payload"))

final_df = parsed_df.select(
    col("payload.region_id").alias("region_id"),
    col("payload.region_description").alias("region_description"),
    col("payload.__op").alias("operation"),
    current_timestamp().alias("updatedate"),
)

# # چاپ داده‌ها روی کنسول
# console_query = final_df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", False) \
#     .start()
# # همزمان هر دو جریان را اجرا می‌کنیم
# console_query.awaitTermination()
# kafka_query.awaitTermination()


# from pyspark.sql import SparkSession
# from pyspark.sql import Row

# jar_files = [
#     "jars/clickhouse-jdbc-0.7.2-all.jar"
# ]

# # Initialize Spark session with JARs
# spark = SparkSession.builder \
#     .appName("example") \
#     .master("local") \
#     .config("spark.jars", ",".join(jar_files)) \
#     .getOrCreate()

# # Create DataFrame
# data = [Row(id=11, name="John"), Row(id=12, name="Doe")]
# df = spark.createDataFrame(data)

url = "jdbc:ch://clickhouse1:8123/default"
user = "default" 
password = "123456"  
driver = "com.clickhouse.jdbc.ClickHouseDriver"

# Write DataFrame to ClickHouse
final_df.writeStream \
    .foreachBatch(lambda batch_df, batch_id: batch_df.write
                  .format("jdbc")
                  .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
                  .option("url", "jdbc:clickhouse://clickhouse1:8123/default")
                  .option("user", "default")
                  .option("password", "123456")
                  .option("dbtable", "northwind.northwind_region")
                  .mode("append")
                  .save()
                 ) \
    .outputMode("append") \
    .start() \
    .awaitTermination()
