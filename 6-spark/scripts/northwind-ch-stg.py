import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, get_json_object, when, lit, current_timestamp, struct, to_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from northwind_schemas import * 
from pyspark.sql.functions import expr, date_format
from pyspark.sql.utils import AnalysisException
from pyspark import SparkConf

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Initialize Spark session with error handling
try:
    spark = SparkSession.builder \
        .appName("northwind_ch_stg") \
        .getOrCreate()
    logger.info("Spark session created successfully")
except Exception as e:
    logger.error(f"Failed to create Spark session: {e}", exc_info=True)
    sys.exit(1)

# Set log level for Spark to reduce noise, but keep our logging
spark.sparkContext.setLogLevel("WARN")


kafka_bootstrap_servers = "kafka:9092"


url = "jdbc:ch://clickhouse1:8123/default"
user = "default" 
password = "123456"  
driver = "com.clickhouse.jdbc.ClickHouseDriver"


def readDataFromTopics(kafka_topic, schema):
    """
    Read data from Kafka topic and parse JSON payload.
    
    Args:
        kafka_topic: Kafka topic name
        schema: Schema for parsing JSON payload
        
    Returns:
        DataFrame with parsed payload
        
    Raises:
        Exception: If Kafka read or parsing fails
    """
    try:
        logger.info(f"Reading from Kafka topic: {kafka_topic}")
        df = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
            .option("subscribe", kafka_topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")  # Don't fail on data loss
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
from pyspark.sql.functions import col, current_timestamp

def transformDebeziumPayload(parsed_df):
    """
    Transform Debezium CDC payload to extract actual data fields.
    
    Args:
        parsed_df: DataFrame with parsed Debezium payload
        
    Returns:
        Transformed DataFrame with business fields
        
    Raises:
        Exception: If transformation fails
    """
    try:
        logger.debug("Transforming Debezium payload")
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
        logger.debug("Successfully transformed Debezium payload")
        return final_df
        
    except Exception as e:
        logger.error(f"Failed to transform Debezium payload: {e}", exc_info=True)
        raise

def transformDate(final_df, dateColumns):
    """
    Transform date columns from numeric format to yyyyMMdd format.
    
    Args:
        final_df: DataFrame to transform
        dateColumns: List of date column names to transform
        
    Returns:
        DataFrame with transformed date columns
        
    Raises:
        Exception: If date transformation fails
    """
    try:
        logger.debug(f"Transforming date columns: {dateColumns}")
        for dateColumn in dateColumns:
            if dateColumn not in final_df.columns:
                logger.warning(f"Date column {dateColumn} not found in DataFrame, skipping")
                continue
            final_df = (
                final_df
                .withColumn(dateColumn, expr(f"to_timestamp({dateColumn} * 86400)"))
                .withColumn(dateColumn, date_format(dateColumn, "yyyyMMdd"))
            )
        logger.debug("Successfully transformed date columns")
        return final_df
        
    except Exception as e:
        logger.error(f"Failed to transform date columns {dateColumns}: {e}", exc_info=True)
        raise

streams = []

def foreach_batch_factory(table_name):
    """
    Factory function to create foreachBatch handler for a specific table.
    
    Args:
        table_name: Target ClickHouse table name
        
    Returns:
        foreachBatch handler function
    """
    def foreach_batch(batch_df, batch_id):
        """
        Process each micro-batch and write to ClickHouse.
        
        Args:
            batch_df: DataFrame for current batch
            batch_id: Unique batch identifier
        """
        try:
            row_count = batch_df.count()
            logger.info(f"Processing batch {batch_id} for table {table_name}: {row_count} rows")
            
            if row_count == 0:
                logger.debug(f"Batch {batch_id} for table {table_name} is empty, skipping write")
                return

            # Write to ClickHouse with retry logic
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
                        time.sleep(2 ** retry_count)  # Exponential backoff
                        
        except Exception as e:
            logger.error(
                f"Error processing batch {batch_id} for table {table_name}: {e}",
                exc_info=True
            )
            # Re-raise to let Spark handle the failure
            raise
            
    return foreach_batch


# Main execution with error handling
try:
    logger.info("Starting streaming job for all tables")
    logger.info(f"Total tables to process: {len(table_mapping)}")
    
    for short_name, (table, topic, schema) in table_mapping.items():
        try:
            logger.info(f"Setting up stream for table: {table}, topic: {topic}")
            
            # Read from Kafka
            df = readDataFromTopics(topic, schema)
            
            # Transform Debezium payload
            transformed_df = transformDebeziumPayload(df)

            # Apply date transformations for specific tables
            if table == "northwind.northwind_employees":
                logger.debug(f"Applying date transformation for {table}")
                transformed_df = transformDate(transformed_df, ["birth_date", "hire_date"])

            elif table == "northwind.northwind_orders":
                logger.debug(f"Applying date transformation for {table}")
                transformed_df = transformDate(transformed_df, ["order_date", "required_date", "shipped_date"])

            # Create and start stream
            stream = (
                transformed_df.writeStream
                .foreachBatch(foreach_batch_factory(table))
                .option("checkpointLocation", f"/tmp/spark_checkpoints/{table}")
                .option("maxFilesPerTrigger", 100)  # Limit files per trigger
                .start()
            )

            streams.append(stream)
            logger.info(f"Successfully started stream for table: {table}")
            
        except Exception as e:
            logger.error(
                f"Failed to set up stream for table {table} (topic {topic}): {e}",
                exc_info=True
            )
            # Continue with other tables instead of failing completely
            continue

    if not streams:
        logger.error("No streams were successfully started. Exiting.")
        sys.exit(1)
    
    logger.info(f"Successfully started {len(streams)} streams. Waiting for termination...")
    
    # Wait for all streams to terminate
    for i, stream in enumerate(streams):
        try:
            logger.info(f"Waiting for stream {i+1}/{len(streams)} to terminate")
            stream.awaitTermination()
        except Exception as e:
            logger.error(f"Stream {i+1} terminated with error: {e}", exc_info=True)
            # Continue waiting for other streams
            
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
    # Stop all streams on fatal error
    for stream in streams:
        try:
            stream.stop()
        except Exception as stop_error:
            logger.error(f"Error stopping stream during cleanup: {stop_error}", exc_info=True)
    sys.exit(1)


    
