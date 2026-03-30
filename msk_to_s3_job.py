"""
AWS Glue Job - Read from MSK Kafka, Write to S3
Processes transaction data from MSK into Parquet format
"""

import sys
import json
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import (
    col, from_json, schema_of_json, current_timestamp,
    year, month, day, hour
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'MSK_BROKERS',
    'KAFKA_TOPIC',
    'S3_OUTPUT_PATH',
    'AWS_REGION'
])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define transaction schema
transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("status", StringType(), True),
    StructField("card_type", StringType(), True),
    StructField("country", StringType(), True),
])

try:
    # Read from MSK Kafka
    print(f"Reading from Kafka topic: {args['KAFKA_TOPIC']}")
    print(f"Bootstrap servers: {args['MSK_BROKERS']}")
    
    df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", args['MSK_BROKERS']) \
        .option("subscribe", args['KAFKA_TOPIC']) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .option("kafka.security.protocol", "PLAINTEXT") \
        .load()
    
    # Parse JSON value column
    df = df.select(
        from_json(col("value").cast("string"), transaction_schema).alias("data")
    ).select("data.*")
    
    # Add processing metadata
    df = df.withColumn("processed_at", current_timestamp())
    df = df.withColumn("year", year(col("timestamp")))
    df = df.withColumn("month", month(col("timestamp")))
    df = df.withColumn("day", day(col("timestamp")))
    df = df.withColumn("hour", hour(col("timestamp")))
    
    # Show sample data
    print("Sample data:")
    df.show(5)
    print(f"Total records: {df.count()}")
    
    # Write to S3 in Parquet format (partitioned)
    output_path = f"{args['S3_OUTPUT_PATH']}/year={datetime.now().year}/month={datetime.now().month}/day={datetime.now().day}"
    
    print(f"Writing to S3: {output_path}")
    df.write \
        .mode("overwrite") \
        .partitionBy("year", "month", "day") \
        .parquet(output_path)
    
    print("✅ Job completed successfully!")
    
except Exception as e:
    print(f"❌ Job failed with error: {str(e)}")
    raise

finally:
    job.commit()