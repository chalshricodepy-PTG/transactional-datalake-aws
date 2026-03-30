"""
MWAA (Managed Workflows for Apache Airflow) Stack
Orchestrates Glue jobs and data pipeline
"""

from aws_cdk import (
    aws_mwaa as mwaa,
    aws_s3 as s3,
    aws_iam as iam,
    aws_ec2 as ec2,
    Tags,
    CfnOutput,
    Stack,
    RemovalPolicy,
)
from constructs import Construct


class MWAAStack(Stack):
    """Stack for MWAA environment and DAGs"""

    def __init__(
        self,
        scope: Construct,
        id: str,
        vpc: ec2.Vpc,
        mwaa_role: iam.Role,
        glue_role: iam.Role,
        mwaa_security_group: ec2.SecurityGroup,
        metadata_bucket: s3.Bucket,
        project_name: str = "transactional-datalake",
        environment: str = "dev",
        **kwargs,
    ):
        super().__init__(scope, id, **kwargs)

        self.project_name_val = project_name
        self.environment_val = environment

        # Create DAGs bucket for Airflow
        self.dags_bucket = self._create_dags_bucket()

        # Create MWAA environment
        self.mwaa_environment = self._create_mwaa_environment(
            mwaa_role,
            vpc,
            mwaa_security_group,
            self.dags_bucket,
        )

        self._create_outputs()

        Tags.of(self).add("Project", project_name)
        Tags.of(self).add("Environment", environment)
        Tags.of(self).add("Component", "MWAA")

    def _create_dags_bucket(self) -> s3.Bucket:
        """Create S3 bucket for Airflow DAGs"""

        bucket = s3.Bucket(
            self,
            "MWAADAGsBucket",
            bucket_name=f"mwaa-dags-{self.project_name_val}-{self.environment_val}-{self.account}",
            versioned=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            removal_policy=RemovalPolicy.RETAIN,
        )

        return bucket

    def _create_mwaa_environment(
        self,
        mwaa_role: iam.Role,
        vpc: ec2.Vpc,
        mwaa_security_group: ec2.SecurityGroup,
        dags_bucket: s3.Bucket,
    ) -> mwaa.CfnEnvironment:
        """Create MWAA environment"""

        # Get private subnets
        private_subnets = [subnet.subnet_id for subnet in vpc.private_subnets]

        environment = mwaa.CfnEnvironment(
            self,
            "MWAAEnvironment",
            name=f"{self.project_name_val}-{self.environment_val}-airflow",
            airflow_configuration_options={
                "core.load_examples": "False",
                "core.max_active_tasks_per_dag": "16",
                "core.max_active_runs_per_dag": "1",
                "core.default_task_retries": "1",
                "core.parallelism": "32",
                "core.dag_concurrency": "16",
            },
            airflow_version="2.8.1",
            dag_s3_path=f"s3://{dags_bucket.bucket_name}/dags/",
            environment_class="mw1.small",
            execution_role_arn=mwaa_role.role_arn,
            logging_configuration=mwaa.CfnEnvironment.LoggingConfigurationProperty(
                dag_processing_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(
                    enabled=True,
                    log_level="INFO",
                ),
                scheduler_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(
                    enabled=True,
                    log_level="INFO",
                ),
                task_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(
                    enabled=True,
                    log_level="INFO",
                ),
                webserver_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(
                    enabled=True,
                    log_level="INFO",
                ),
            ),
            max_workers=10,
            min_workers=1,
            network_configuration=mwaa.CfnEnvironment.NetworkConfigurationProperty(
                security_group_ids=[mwaa_security_group.security_group_id],
                subnet_ids=private_subnets[:2],
            ),
            source_bucket_arn=dags_bucket.bucket_arn,
            tags={
                "Project": self.project_name_val,
                "Environment": self.environment_val,
            },
        )

        return environment

    def _create_outputs(self):
        """Create CloudFormation outputs"""
        CfnOutput(
            self,
            "MWAAEnvironmentName",
            value=self.mwaa_environment.name or "N/A",
            description="MWAA environment name",
            export_name=f"{self.project_name_val}-{self.environment_val}-mwaa-env",
        )

        CfnOutput(
            self,
            "MWAADAGsBucketName",
            value=self.dags_bucket.bucket_name,
            description="S3 bucket for Airflow DAGs",
            export_name=f"{self.project_name_val}-{self.environment_val}-mwaa-dags-bucket",
        )