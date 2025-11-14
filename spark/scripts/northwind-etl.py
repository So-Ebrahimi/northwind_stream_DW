from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, get_json_object, when, lit, current_timestamp, struct, to_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from norhwind_schemas import * 

spark = (
    SparkSession.builder
    .appName("KafkaDebeziumClickHouse")
    .config("spark.ui.enabled", "false")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "com.clickhouse:clickhouse-jdbc:0.7.2"
    )    
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


kafka_bootstrap_servers = "localhost:9092"

url = "jdbc:clickhouse://localhost:9123/default"
user = "default" 
password = "123456"  
driver = "com.clickhouse.jdbc.ClickHouseDriver"


def readDataFromTopics(kafka_topic ,schema):
    df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "earliest")
    .load()
            )

    json_df = df.selectExpr("CAST(value AS STRING) as json_value")
    parsed_df = json_df.select(from_json(get_json_object(col("json_value"), "$.payload"), schema).alias("payload"))

    return parsed_df
from pyspark.sql.functions import col, current_timestamp

def transformDebeziumPayload(parsed_df):
    # Remove tombstone messages
    parsed_df = parsed_df.filter(col("payload").isNotNull())
    
    # Flatten all fields dynamically from payload
    payload_schema = parsed_df.schema["payload"].dataType
    select_exprs = []
    for f in payload_schema.fields:
        if f.name not in ["__deleted", "__op", "__ts_ms"]:
            select_exprs.append(col(f"payload." + f.name).alias(f.name))
    # select_exprs = [col(f"payload." + f.name).alias(f.name) for f in payload_schema.fields]
    
    # Always include operation and update timestamp
    select_exprs += [
        col("payload.__op").alias("operation"),
        col("payload.__op").alias("updatedate")
    ]

    final_df = parsed_df.select(*select_exprs)
    return final_df
streams = []
for short_name, (table, topic, schema) in table_mapping.items():
    print(table)
    df = readDataFromTopics(topic, schema)
    transformed_df = transformDebeziumPayload(df)
    transformed_df.printSchema()

    console_query = transformed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()
# همزمان هر دو جریان را اجرا می‌کنیم
    stream = (
        transformed_df.writeStream
        .foreachBatch(lambda batch_df, batch_id:
    batch_df.write \
        .format("jdbc") \
        .option("driver", driver) \
        .option("url", url) \
        .option("user", user) \
        .option("password", password) \
        .option("dbtable", "default.categories")        
        .mode("append").save())
        .outputMode("append")
        .start()
    )
    streams.append(console_query)

    streams.append(stream)

# Wait for all streams AFTER the loop
for stream in streams:
    stream.awaitTermination()
    
