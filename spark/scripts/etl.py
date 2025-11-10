from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, BooleanType

spark = (
    SparkSession.builder
    .appName("KafkaDebeziumReader")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

kafka_bootstrap_servers = "kafka:29092"
kafka_topic = "northwind.public.orders"

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "earliest")
    .load()
)

json_df = df.selectExpr("CAST(value AS STRING) as json_value")

from pyspark.sql.functions import get_json_object

after_df = json_df.select(
    get_json_object(col("json_value"), "$.payload.after").alias("after_json")
)

query = (
    after_df.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", False)
    .start()
)

query.awaitTermination()
