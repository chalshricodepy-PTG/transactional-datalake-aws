"""
S3 Stack - Data Lake Storage
Creates S3 buckets for raw and curated data with lifecycle policies
"""

from aws_cdk import (
    aws_s3 as s3,
    Tags,
    CfnOutput,
    Stack,
    Duration,
    RemovalPolicy,
)
from constructs import Construct


class S3DataLakeStack(Stack):
    """Stack for S3 data lake buckets"""

    def __init__(
        self,
        scope: Construct,
        id: str,
        project_name: str = "transactional-datalake",
        environment: str = "dev",
        **kwargs,
    ):
        super().__init__(scope, id, **kwargs)

        self.project_name_val = project_name
        self.environment_val = environment

        # Create S3 buckets
        self.raw_bucket = self._create_raw_bucket()
        self.curated_bucket = self._create_curated_bucket()
        self.logs_bucket = self._create_logs_bucket()
        self.metadata_bucket = self._create_metadata_bucket()

        # Output bucket information
        self._create_outputs()

        # Add tags
        Tags.of(self).add("Project", project_name)
        Tags.of(self).add("Environment", environment)
        Tags.of(self).add("Component", "Storage")

    def _create_raw_bucket(self) -> s3.Bucket:
        """Create raw data lake bucket"""
        bucket = s3.Bucket(
            self,
            "RawDataBucket",
            bucket_name=f"raw-{self.project_name_val}-{self.environment_val}-{self.account}",
            versioned=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.KMS_MANAGED,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        # Enable intelligent tiering to reduce costs
        bucket.add_lifecycle_rule(
            transitions=[
                s3.Transition(
                    storage_class=s3.StorageClass.INTELLIGENT_TIERING,
                    transition_after=Duration.days(0),
                )
            ],
            noncurrent_version_transitions=[
                s3.NoncurrentVersionTransition(
                    storage_class=s3.StorageClass.GLACIER,
                    transition_after=Duration.days(90),
                )
            ],
        )

        return bucket

    def _create_curated_bucket(self) -> s3.Bucket:
        """Create curated data (Iceberg) bucket"""
        bucket = s3.Bucket(
            self,
            "CuratedDataBucket",
            bucket_name=f"curated-{self.project_name_val}-{self.environment_val}-{self.account}",
            versioned=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.KMS_MANAGED,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        # Lifecycle policy for Iceberg tables
        bucket.add_lifecycle_rule(
            transitions=[
                s3.Transition(
                    storage_class=s3.StorageClass.INTELLIGENT_TIERING,
                    transition_after=Duration.days(30),
                )
            ],
            noncurrent_version_transitions=[
                s3.NoncurrentVersionTransition(
                    storage_class=s3.StorageClass.GLACIER,
                    transition_after=Duration.days(180),
                )
            ],
        )

        return bucket

    def _create_logs_bucket(self) -> s3.Bucket:
        """Create logs bucket for S3 access logs"""
        bucket = s3.Bucket(
            self,
            "LogsBucket",
            bucket_name=f"logs-{self.project_name_val}-{self.environment_val}-{self.account}",
            versioned=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        # Lifecycle policy for logs (delete old logs)
        bucket.add_lifecycle_rule(
            expiration=Duration.days(30),
            noncurrent_version_expiration=Duration.days(7),
        )

        return bucket

    def _create_metadata_bucket(self) -> s3.Bucket:
        """Create metadata bucket for Glue catalog and Iceberg metadata"""
        bucket = s3.Bucket(
            self,
            "MetadataBucket",
            bucket_name=f"metadata-{self.project_name_val}-{self.environment_val}-{self.account}",
            versioned=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.KMS_MANAGED,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        return bucket

    def _create_outputs(self):
        """Create CloudFormation outputs for bucket names"""
        CfnOutput(
            self,
            "RawBucketName",
            value=self.raw_bucket.bucket_name,
            description="Raw Data Bucket Name",
            export_name=f"{self.project_name_val}-{self.environment_val}-raw-bucket",
        )

        CfnOutput(
            self,
            "CuratedBucketName",
            value=self.curated_bucket.bucket_name,
            description="Curated Data Bucket Name",
            export_name=f"{self.project_name_val}-{self.environment_val}-curated-bucket",
        )

        CfnOutput(
            self,
            "LogsBucketName",
            value=self.logs_bucket.bucket_name,
            description="Logs Bucket Name",
            export_name=f"{self.project_name_val}-{self.environment_val}-logs-bucket",
        )

        CfnOutput(
            self,
            "MetadataBucketName",
            value=self.metadata_bucket.bucket_name,
            description="Metadata Bucket Name",
            export_name=f"{self.project_name_val}-{self.environment_val}-metadata-bucket",
        )