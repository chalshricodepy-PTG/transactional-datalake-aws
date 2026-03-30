"""
IAM Stack - Service Roles and Policies
Creates IAM roles for Glue, Lambda, MWAA, and other services
"""

from aws_cdk import (
    aws_iam as iam,
    Tags,
    CfnOutput,
    Stack,
)
from constructs import Construct


class IAMStack(Stack):
    """Stack for IAM roles and policies"""

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

        self.glue_role = self._create_glue_role()
        self.lambda_role = self._create_lambda_role()
        self.mwaa_role = self._create_mwaa_role()
        self.s3_access_role = self._create_s3_access_role()

        self._create_outputs()

        Tags.of(self).add("Project", project_name)
        Tags.of(self).add("Environment", environment)
        Tags.of(self).add("Component", "IAM")

    def _create_glue_role(self) -> iam.Role:
        """Create IAM role for Glue jobs"""
        role = iam.Role(
            self,
            "GlueJobRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            role_name=f"{self.project_name_val}-{self.environment_val}-glue-role",
            description="Role for AWS Glue jobs",
        )

        role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AWSGlueServiceRole"
            )
        )

        role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListBucket",
                ],
                resources=[
                    f"arn:aws:s3:::*-{self.project_name_val}-*",
                    f"arn:aws:s3:::*-{self.project_name_val}-*/*",
                ],
            )
        )

        role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "kafka:DescribeCluster",
                    "kafka:GetBootstrapBrokers",
                    "kafka-cluster:Connect",
                    "kafka-cluster:AlterCluster",
                    "kafka-cluster:DescribeCluster",
                ],
                resources=["*"],
            )
        )

        role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "glue:GetDatabase",
                    "glue:GetTable",
                    "glue:GetPartitions",
                    "glue:CreateTable",
                    "glue:UpdateTable",
                    "glue:DeleteTable",
                    "glue:PutDataCatalogEncryptionSettings",
                    "glue:GetDataCatalogEncryptionSettings",
                ],
                resources=["*"],
            )
        )

        role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                resources=[f"arn:aws:logs:{self.region}:{self.account}:*"],
            )
        )

        role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "ec2:CreateNetworkInterface",
                    "ec2:DescribeNetworkInterfaces",
                    "ec2:DeleteNetworkInterface",
                    "ec2:DescribeSecurityGroups",
                    "ec2:DescribeSubnets",
                    "ec2:DescribeVpcs",
                ],
                resources=["*"],
            )
        )

        role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "glue:GetSchemaVersion",
                    "glue:GetSchema",
                    "glue:CreateSchema",
                    "glue:UpdateSchema",
                ],
                resources=["*"],
            )
        )

        return role

    def _create_lambda_role(self) -> iam.Role:
        """Create IAM role for Lambda functions"""
        role = iam.Role(
            self,
            "LambdaExecutionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            role_name=f"{self.project_name_val}-{self.environment_val}-lambda-role",
            description="Role for Lambda functions",
        )

        role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AWSLambdaVPCAccessExecutionRole"
            )
        )

        role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "kafka:DescribeCluster",
                    "kafka:GetBootstrapBrokers",
                    "kafka-cluster:Connect",
                    "kafka-cluster:DescribeCluster",
                    "kafka-cluster:*Topic*",
                    "kafka-cluster:WriteData",
                    "kafka-cluster:ReadData",
                ],
                resources=["*"],
            )
        )

        role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:ListBucket",
                ],
                resources=[
                    f"arn:aws:s3:::*-{self.project_name_val}-*",
                    f"arn:aws:s3:::*-{self.project_name_val}-*/*",
                ],
            )
        )

        role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                resources=[f"arn:aws:logs:{self.region}:{self.account}:*"],
            )
        )

        return role

    def _create_mwaa_role(self) -> iam.Role:
        """Create IAM role for MWAA"""
        role = iam.Role(
            self,
            "MWAAExecutionRole",
            assumed_by=iam.ServicePrincipal("airflow.amazonaws.com"),
            role_name=f"{self.project_name_val}-{self.environment_val}-mwaa-role",
            description="Role for Amazon MWAA",
        )

        role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "airflow:PublishMetrics",
                    "s3:GetObject*",
                    "s3:GetBucket*",
                    "s3:List*",
                ],
                resources=["*"],
            )
        )

        role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "glue:StartJobRun",
                    "glue:GetJobRun",
                    "glue:GetJobRuns",
                    "glue:BatchStopJobRun",
                    "glue:GetJob",
                ],
                resources=[f"arn:aws:glue:{self.region}:{self.account}:job/*"],
            )
        )

        role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "s3:GetObject",
                    "s3:ListBucket",
                ],
                resources=[
                    f"arn:aws:s3:::*-{self.project_name_val}-*",
                    f"arn:aws:s3:::*-{self.project_name_val}-*/*",
                ],
            )
        )

        role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "logs:CreateLogStream",
                    "logs:CreateLogGroup",
                    "logs:PutLogEvents",
                    "logs:GetLogEvents",
                    "logs:GetLogRecord",
                    "logs:GetQueryResults",
                    "logs:DescribeLogStreams",
                ],
                resources=[f"arn:aws:logs:{self.region}:{self.account}:*"],
            )
        )

        return role

    def _create_s3_access_role(self) -> iam.Role:
        """Create IAM role for S3 cross-service access"""
        role = iam.Role(
            self,
            "S3AccessRole",
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("glue.amazonaws.com"),
                iam.ServicePrincipal("lambda.amazonaws.com"),
                iam.ServicePrincipal("athena.amazonaws.com"),
            ),
            role_name=f"{self.project_name_val}-{self.environment_val}-s3-access-role",
            description="Role for S3 access across services",
        )

        role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["s3:*"],
                resources=[
                    f"arn:aws:s3:::*-{self.project_name_val}-*",
                    f"arn:aws:s3:::*-{self.project_name_val}-*/*",
                ],
            )
        )

        return role

    def _create_outputs(self):
        """Create CloudFormation outputs for role ARNs"""
        CfnOutput(
            self,
            "GlueRoleArn",
            value=self.glue_role.role_arn,
            description="Glue Service Role ARN",
            export_name=f"{self.project_name_val}-{self.environment_val}-glue-role-arn",
        )

        CfnOutput(
            self,
            "LambdaRoleArn",
            value=self.lambda_role.role_arn,
            description="Lambda Execution Role ARN",
            export_name=f"{self.project_name_val}-{self.environment_val}-lambda-role-arn",
        )

        CfnOutput(
            self,
            "MWAARoleArn",
            value=self.mwaa_role.role_arn,
            description="MWAA Execution Role ARN",
            export_name=f"{self.project_name_val}-{self.environment_val}-mwaa-role-arn",
        )

        CfnOutput(
            self,
            "S3AccessRoleArn",
            value=self.s3_access_role.role_arn,
            description="S3 Access Role ARN",
            export_name=f"{self.project_name_val}-{self.environment_val}-s3-access-role-arn",
        )