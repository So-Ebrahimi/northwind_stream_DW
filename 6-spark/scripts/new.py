from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, get_json_object, current_timestamp
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


def readDataFromTopics(kafka_topic, schema):
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


def transformDebeziumPayload(parsed_df):
    parsed_df = parsed_df.filter(col("payload").isNotNull())
    
    payload_schema = parsed_df.schema["payload"].dataType
    select_exprs = []
    for f in payload_schema.fields:
        if f.name not in ["__deleted", "__op", "__ts_ms"]:
            select_exprs.append(col(f"payload.{f.name}").alias(f.name))
    
    select_exprs += [
        col("payload.__op").alias("operation"),
        current_timestamp().alias("updatedate")
    ]
    return parsed_df.select(*select_exprs)


def writeToClickHouse(batch_df, batch_id, table_name):
    print(f"Processing batch {batch_id} for table {table_name}")
    batch_df.show(5, truncate=False)
    batch_df.write \
        .format("jdbc") \
        .option("driver", driver) \
        .option("url", url) \
        .option("user", user) \
        .option("password", password) \
        .option("dbtable", table_name) \
        .mode("append") \
        .save()


streams = []
for short_name, (table, topic, schema) in table_mapping.items():
    print(f"Starting stream for table: {table}")
    df = readDataFromTopics(topic, schema)
    transformed_df = transformDebeziumPayload(df)
    transformed_df.printSchema()

    # نمایش در کنسول برای دیباگ
    console_query = transformed_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()

    # نوشتن در ClickHouse
    stream = transformed_df.writeStream \
        .foreachBatch(lambda batch_df, batch_id, t=table: writeToClickHouse(batch_df, batch_id, t)) \
        .outputMode("append") \
        .start()

    streams.append(console_query)
    streams.append(stream)

for stream in streams:
    stream.awaitTermination()