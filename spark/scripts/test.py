from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, get_json_object
from pyspark.sql.types import StructType
from norhwind_schemas import *  # Make sure this defines your table_mapping
import clickhouse_connect

# -------------------------
# Spark Session
# -------------------------
spark = (
    SparkSession.builder
    .appName("KafkaDebeziumClickHouse")
    .config("spark.ui.enabled", "false")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    )
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# -------------------------
# Kafka & ClickHouse Config
# -------------------------
kafka_bootstrap_servers = "localhost:9092"

# ClickHouse client
client = clickhouse_connect.get_client(
    host='localhost',
    port=9123,
    username='default',
    password='123456'
)
print(client.command('SELECT 1'))  # test connection

# -------------------------
# Functions
# -------------------------
def readDataFromTopics(kafka_topic, schema: StructType):
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "earliest")
        .load()
    )
    json_df = df.selectExpr("CAST(value AS STRING) as json_value")
    parsed_df = json_df.select(
        from_json(get_json_object(col("json_value"), "$.payload"), schema).alias("payload")
    )
    return parsed_df


def transformDebeziumPayload(parsed_df):
    # Remove tombstone messages
    parsed_df = parsed_df.filter(col("payload").isNotNull())
    
    # Flatten payload fields dynamically
    payload_schema = parsed_df.schema["payload"].dataType
    select_exprs = []
    for f in payload_schema.fields:
        if f.name not in ["__deleted", "__op", "__ts_ms"]:
            select_exprs.append(col(f"payload.{f.name}").alias(f.name))
    
    # Include operation and update timestamp
    select_exprs += [
        col("payload.__op").alias("operation"),
        col("payload.__op").alias("updatedate")
    ]

    final_df = parsed_df.select(*select_exprs)
    return final_df


def write_to_clickhouse(batch_df, batch_id, table_name):
    """Write each micro-batch to ClickHouse using clickhouse-connect."""
    pdf = batch_df.toPandas()
    if not pdf.empty:
        client.insert(table=table_name, data=pdf.to_dict('records'))


# -------------------------
# Streaming Execution
# -------------------------
streams = []

for short_name, (table, topic, schema) in table_mapping.items():
    print(f"Processing table: {table}")

    # Read from Kafka
    df = readDataFromTopics(topic, schema)

    # Transform Debezium payload
    transformed_df = transformDebeziumPayload(df)
    transformed_df.printSchema()

    # Optional: console output for debugging
    console_query = transformed_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()
    
    # Write to ClickHouse using clickhouse-connect
    stream = transformed_df.writeStream \
        .foreachBatch(lambda batch_df, batch_id, tbl=table: write_to_clickhouse(batch_df, batch_id, tbl)) \
        .outputMode("append") \
        .start()
    
    streams.append(console_query)
    streams.append(stream)

# Wait for all streams
for stream in streams:
    stream.awaitTermination()
