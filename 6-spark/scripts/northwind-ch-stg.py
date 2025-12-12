import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, get_json_object, when, lit, current_timestamp, struct, to_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from northwind_schemas import * 
from pyspark.sql.functions import expr, date_format
from pyspark.sql.utils import AnalysisException
from pyspark import SparkConf

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

try:
    spark = SparkSession.builder \
        .appName("northwind_ch_stg") \
        .getOrCreate()
    logger.info("Spark session created successfully")
except Exception as e:
    logger.error(f"Failed to create Spark session: {e}", exc_info=True)
    sys.exit(1)

spark.sparkContext.setLogLevel("WARN")


kafka_bootstrap_servers = "kafka:9092"


url = "jdbc:ch://clickhouse1:8123/default"
user = "default" 
password = "123456"  
driver = "com.clickhouse.jdbc.ClickHouseDriver"


def readDataFromTopics(kafka_topic, schema):
    try:
        logger.info(f"Reading from Kafka topic: {kafka_topic}")
        df = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
            .option("subscribe", kafka_topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false") 
            .load()
        )
        logger.info(f"Successfully connected to Kafka topic: {kafka_topic}")

        json_df = df.selectExpr("CAST(value AS STRING) as json_value")
        parsed_df = json_df.select(
            from_json(get_json_object(col("json_value"), "$.payload"), schema).alias("payload")
        )
        
        logger.info(f"Successfully parsed JSON from topic: {kafka_topic}")
        return parsed_df
        
    except Exception as e:
        logger.error(f"Failed to read from Kafka topic {kafka_topic}: {e}", exc_info=True)
        raise

def transformDebeziumPayload(parsed_df):
    try:
        logger.debug("Transforming Debezium payload")
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
        logger.debug("Successfully transformed Debezium payload")
        return final_df
        
    except Exception as e:
        logger.error(f"Failed to transform Debezium payload: {e}", exc_info=True)
        raise

def transformDate(final_df, dateColumns):
    try:
        logger.debug(f"Transforming date columns: {dateColumns}")
        for dateColumn in dateColumns:
            if dateColumn not in final_df.columns:
                logger.warning(f"Date column {dateColumn} not found in DataFrame, skipping")
                continue
            final_df = (
                final_df
                .withColumn(dateColumn, 
                    when(col(dateColumn).isNotNull(), 
                        date_format(expr(f"to_timestamp({dateColumn} * 86400)"), "yyyyMMdd")
                    ).otherwise(lit(None).cast("string"))
                )
            )
        logger.debug("Successfully transformed date columns")
        return final_df
        
    except Exception as e:
        logger.error(f"Failed to transform date columns {dateColumns}: {e}", exc_info=True)
        raise

streams = []

def foreach_batch_factory(table_name):
    def foreach_batch(batch_df, batch_id):
        try:
            row_count = batch_df.count()
            logger.info(f"Processing batch {batch_id} for table {table_name}: {row_count} rows")
            
            if row_count == 0:
                logger.debug(f"Batch {batch_id} for table {table_name} is empty, skipping write")
                return

            max_retries = 3
            retry_count = 0
            while retry_count < max_retries:
                try:
                    (batch_df.write
                        .format("jdbc")
                        .option("driver", driver)
                        .option("url", url)
                        .option("user", user)
                        .option("password", password)
                        .option("dbtable", table_name)
                        .mode("append")
                        .save()
                    )
                    logger.info(f"Successfully wrote batch {batch_id} for table {table_name}: {row_count} rows")
                    break
                    
                except Exception as write_error:
                    retry_count += 1
                    if retry_count >= max_retries:
                        logger.error(
                            f"Failed to write batch {batch_id} for table {table_name} after {max_retries} retries: {write_error}",
                            exc_info=True
                        )
                        raise
                    else:
                        logger.warning(
                            f"Retry {retry_count}/{max_retries} for batch {batch_id} table {table_name}: {write_error}"
                        )
                        import time
                        time.sleep(2 ** retry_count) 
                        
        except Exception as e:
            logger.error(
                f"Error processing batch {batch_id} for table {table_name}: {e}",
                exc_info=True
            )
            raise
            
    return foreach_batch


try:
    logger.info("Starting streaming job for all tables")
    logger.info(f"Total tables to process: {len(table_mapping)}")
    
    for short_name, (table, topic, schema) in table_mapping.items():
        try:
            logger.info(f"Setting up stream for table: {table}, topic: {topic}")
            
            df = readDataFromTopics(topic, schema)
            transformed_df = transformDebeziumPayload(df)

            if table == "northwind.northwind_employees":
                logger.debug(f"Applying date transformation for {table}")
                transformed_df = transformDate(transformed_df, ["birth_date", "hire_date"])

            elif table == "northwind.northwind_orders":
                logger.debug(f"Applying date transformation for {table}")
                transformed_df = transformDate(transformed_df, ["order_date", "required_date", "shipped_date"])

            stream = (
                transformed_df.writeStream
                .foreachBatch(foreach_batch_factory(table))
                .option("checkpointLocation", f"/tmp/spark_checkpoints/{table}")
                .option("maxFilesPerTrigger", 100)  
                .start()
            )

            streams.append(stream)
            logger.info(f"Successfully started stream for table: {table}")
            
        except Exception as e:
            logger.error(
                f"Failed to set up stream for table {table} (topic {topic}): {e}",
                exc_info=True
            )
            continue

    if not streams:
        logger.error("No streams were successfully started. Exiting.")
        sys.exit(1)
    
    logger.info(f"Successfully started {len(streams)} streams. Waiting for termination...")
    
    for i, stream in enumerate(streams):
        try:
            logger.info(f"Waiting for stream {i+1}/{len(streams)} to terminate")
            stream.awaitTermination()
        except Exception as e:
            logger.error(f"Stream {i+1} terminated with error: {e}", exc_info=True)
            
except KeyboardInterrupt:
    logger.info("Received interrupt signal. Stopping all streams...")
    for stream in streams:
        try:
            stream.stop()
        except Exception as e:
            logger.error(f"Error stopping stream: {e}", exc_info=True)
    logger.info("All streams stopped")
    
except Exception as e:
    logger.error(f"Fatal error in streaming job: {e}", exc_info=True)
    for stream in streams:
        try:
            stream.stop()
        except Exception as stop_error:
            logger.error(f"Error stopping stream during cleanup: {stop_error}", exc_info=True)
    sys.exit(1)


    
