"""
Glue Jobs Stack - ETL jobs for data processing
- Streaming job: MSK -> Raw S3
- Batch job: Raw S3 -> Curated Iceberg S3
"""

from aws_cdk import (
    aws_glue as glue,
    aws_s3 as s3,
    aws_iam as iam,
    aws_ec2 as ec2,
    Tags,
    CfnOutput,
    Stack,
)
from constructs import Construct


class GlueJobsStack(Stack):
    """Stack for Glue ETL jobs"""

    def __init__(
        self,
        scope: Construct,
        id: str,
        vpc,
        glue_role: iam.Role,
        msk_security_group: ec2.SecurityGroup,
        glue_security_group: ec2.SecurityGroup,
        raw_bucket: s3.Bucket,
        curated_bucket: s3.Bucket,
        project_name: str = "transactional-datalake",
        environment: str = "dev",
        **kwargs,
    ):
        super().__init__(scope, id, **kwargs)

        self.project_name_val = project_name
        self.environment_val = environment

        # Create Glue Streaming job for MSK to Raw S3
        self.msk_to_raw_streaming_job = self._create_msk_to_raw_streaming_job(
            glue_role,
            msk_security_group,
            glue_security_group,
            raw_bucket,
            vpc,
        )

        # Create Glue Batch job for Raw to Curated Iceberg
        self.raw_to_curated_batch_job = self._create_raw_to_curated_batch_job(
            glue_role,
            raw_bucket,
            curated_bucket,
        )

        self._create_outputs()

        Tags.of(self).add("Project", project_name)
        Tags.of(self).add("Environment", environment)
        Tags.of(self).add("Component", "Glue")

    def _create_msk_to_raw_streaming_job(
        self,
        glue_role: iam.Role,
        msk_sg: ec2.SecurityGroup,
        glue_sg: ec2.SecurityGroup,
        raw_bucket: s3.Bucket,
        vpc: ec2.Vpc,
    ) -> glue.CfnJob:
        """Create Glue Streaming job to read from MSK and write to S3 Raw (continuous ingestion)"""

        script_content = """import sys
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.streaming.dynamic_frame import DynamicFrame
from pyspark.sql.functions import col, from_json, current_timestamp, from_utc_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'MSK_BROKERS', 'KAFKA_TOPIC', 'S3_OUTPUT_PATH', 'AWS_REGION'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

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
    print(f"🔄 Starting Glue Streaming Job - MSK to Raw")
    print(f"Kafka Topic: {args['KAFKA_TOPIC']}")
    print(f"Bootstrap Servers: {args['MSK_BROKERS']}")
    
    # Read streaming data from Kafka
    df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", args['MSK_BROKERS']).option("subscribe", args['KAFKA_TOPIC']).option("startingOffsets", "latest").option("maxOffsetsPerTrigger", "10000").option("failOnDataLoss", "false").load()
    
    # Parse JSON value column
    df = df.select(from_json(col("value").cast("string"), transaction_schema).alias("data")).select("data.*")
    
    # Add ingestion metadata
    df = df.withColumn("ingested_at", current_timestamp())
    df = df.withColumn("ingestion_batch_id", from_utc_timestamp(current_timestamp(), "UTC"))
    
    # Convert to DynamicFrame
    dyf = DynamicFrame.fromDF(df, glueContext, "kafka_stream")
    
    # Write to S3 Raw bucket
    output_path = f"{args['S3_OUTPUT_PATH']}/raw/transactions/"
    print(f"📝 Writing streaming data to: {output_path}")
    
    glueContext.write_dynamic_frame.from_options(
        frame=dyf,
        connection_type="s3",
        format="parquet",
        connection_options={"path": output_path},
        transformation_ctx="s3_sink"
    )
    
    print("✅ Streaming job running continuously!")
    
except Exception as e:
    print(f"❌ Streaming job failed: {str(e)}")
    raise
finally:
    job.commit()
"""

        job = glue.CfnJob(
            self,
            "MSKToRawStreamingJob",
            name=f"{self.project_name_val}-{self.environment_val}-msk-to-raw-streaming",
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="gluestreaming",
                script_location=f"s3://{raw_bucket.bucket_name}/glue-scripts/msk_to_raw_streaming_job.py",
                python_version="3.9",
            ),
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1,
            ),
            default_arguments={
                "--job-language": "python",
                "--enable-spark-ui": "true",
                "--spark-event-logs-path": f"s3://{raw_bucket.bucket_name}/spark-logs/",
                "--enable-glue-datacatalog": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--MSK_BROKERS": "10.0.0.0:9092",
                "--KAFKA_TOPIC": "transactions",
                "--S3_OUTPUT_PATH": f"s3://{raw_bucket.bucket_name}/",
                "--AWS_REGION": self.region,
            },
            glue_version="4.0",
            max_retries=0,
            timeout=2880,
            worker_type="G.2X",
            number_of_workers=5,
            tags={
                "Project": self.project_name_val,
                "Environment": self.environment_val,
                "JobType": "Streaming",
            },
        )

        return job

    def _create_raw_to_curated_batch_job(
        self,
        glue_role: iam.Role,
        raw_bucket: s3.Bucket,
        curated_bucket: s3.Bucket,
    ) -> glue.CfnJob:
        """Create Glue Batch job to transform raw data to curated Iceberg tables (partitioned)"""

        script_content = """import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, from_unixtime, current_timestamp, year, month, day, hour, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'RAW_INPUT_PATH', 'CURATED_OUTPUT_PATH', 'AWS_REGION'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

try:
    print(f"📊 Starting Glue Batch Job - Raw to Curated Iceberg")
    print(f"Input: {args['RAW_INPUT_PATH']}")
    print(f"Output: {args['CURATED_OUTPUT_PATH']}")
    
    # Read raw data from S3
    df = spark.read.parquet(args['RAW_INPUT_PATH'])
    
    print(f"Read {df.count()} records from raw S3")
    
    # Parse timestamp and add time-based columns
    df = df.withColumn("timestamp_dt", to_timestamp(col("timestamp")))
    df = df.withColumn("processed_at", current_timestamp())
    df = df.withColumn("year", year(col("timestamp_dt")))
    df = df.withColumn("month", month(col("timestamp_dt")))
    df = df.withColumn("day", day(col("timestamp_dt")))
    df = df.withColumn("hour", hour(col("timestamp_dt")))
    
    # Data quality checks
    df = df.filter(col("amount") > 0)
    df = df.filter(col("customer_id").isNotNull())
    df = df.filter(col("transaction_id").isNotNull())
    df = df.filter(col("status").isNotNull())
    
    print(f"After data quality checks: {df.count()} records")
    
    # Write to Curated bucket as Iceberg table, partitioned by date and status
    output_path = args['CURATED_OUTPUT_PATH']
    print(f"💾 Writing to Iceberg table (partitioned by year/month/day/status): {output_path}")
    
    df.write \\
        .format("iceberg") \\
        .mode("overwrite") \\
        .partitionBy("year", "month", "day", "status") \\
        .option("path", output_path) \\
        .save()
    
    print(f"✅ Successfully transformed and wrote {df.count()} records to Iceberg!")
    
except Exception as e:
    print(f"❌ Batch job failed: {str(e)}")
    raise
finally:
    job.commit()
"""

        job = glue.CfnJob(
            self,
            "RawToCuratedBatchJob",
            name=f"{self.project_name_val}-{self.environment_val}-raw-to-curated-batch",
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=f"s3://{raw_bucket.bucket_name}/glue-scripts/raw_to_curated_batch_job.py",
                python_version="3.9",
            ),
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1,
            ),
            default_arguments={
                "--job-language": "python",
                "--job-bookmark-option": "job-bookmark-enable",
                "--TempDir": f"s3://{raw_bucket.bucket_name}/glue-temp/",
                "--enable-spark-ui": "true",
                "--spark-event-logs-path": f"s3://{raw_bucket.bucket_name}/spark-logs/",
                "--enable-glue-datacatalog": "true",
                "--RAW_INPUT_PATH": f"s3://{raw_bucket.bucket_name}/raw/transactions/",
                "--CURATED_OUTPUT_PATH": f"s3://{curated_bucket.bucket_name}/transactions/",
                "--AWS_REGION": self.region,
            },
            glue_version="4.0",
            max_retries=1,
            timeout=2880,
            worker_type="G.2X",
            number_of_workers=10,
            tags={
                "Project": self.project_name_val,
                "Environment": self.environment_val,
                "JobType": "Batch",
            },
        )

        return job

    def _create_outputs(self):
        """Create CloudFormation outputs"""
        CfnOutput(
            self,
            "MSKToRawStreamingJobName",
            value=self.msk_to_raw_streaming_job.name,
            description="Glue Streaming Job name for MSK to Raw S3",
            export_name=f"{self.project_name_val}-{self.environment_val}-msk-to-raw-streaming-job",
        )
        
        CfnOutput(
            self,
            "RawToCuratedBatchJobName",
            value=self.raw_to_curated_batch_job.name,
            description="Glue Batch Job name for Raw to Curated Iceberg",
            export_name=f"{self.project_name_val}-{self.environment_val}-raw-to-curated-batch-job",
        )