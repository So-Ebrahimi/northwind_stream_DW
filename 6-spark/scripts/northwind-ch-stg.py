from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, get_json_object, when, lit, current_timestamp, struct, to_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from norhwind_schemas import * 
from pyspark.sql.functions import expr
spark = SparkSession.builder \
    .appName("example") \
    .master("local") \
    .getOrCreate()


spark.sparkContext.setLogLevel("ERROR")


kafka_bootstrap_servers = "kafka:9092"


url = "jdbc:ch://clickhouse1:8123/default"
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
    
    payload_schema = parsed_df.schema["payload"].dataType
    select_exprs = []
    for f in payload_schema.fields:
        if f.name not in ["__deleted", "__op", "__ts_ms"]:
            select_exprs.append(col(f"payload." + f.name).alias(f.name))

    select_exprs += [
        col("payload.__op").alias("operation"),
        current_timestamp().alias("updatedate")
    ]

    final_df = parsed_df.select(*select_exprs)
    return final_df

def transformDate(final_df , dateColumns) :
    for dateColumn in dateColumns :
        final_df = final_df.withColumn(dateColumn, expr(f"to_timestamp({dateColumn} * 86400)"))
    return  final_df


streams = []

for short_name, (table, topic, schema) in table_mapping.items():
    df = readDataFromTopics(topic, schema)
    transformed_df = transformDebeziumPayload(df)
    if table=="northwind.northwind_employees" : 
        transformed_df = transformDate(transformed_df , ["birth_date","hire_date"])
    elif table=="northwind.northwind_orders" : 
        transformed_df = transformDate(transformed_df , ["order_date","required_date","shipped_date"])    

    def foreach_batch(batch_df, batch_id, table_name=table):
        row_count = batch_df.count()  # تعداد ردیف‌های این batch
        print(f"Batch {batch_id} for table {table_name}: {row_count} rows updated")
        
        batch_df.write \
            .format("jdbc") \
            .option("driver", driver) \
            .option("url", url) \
            .option("user", user) \
            .option("password", password) \
            .option("dbtable", table_name) \
            .mode("append") \
            .save()

    stream = (
        transformed_df.writeStream
        .foreachBatch(foreach_batch)
        .outputMode("append")
        .option("checkpointLocation", f"/tmp/spark_checkpoints/{table}")  
        .start()
    )

    streams.append(stream)

for stream in streams:
    stream.awaitTermination()

    
