"""
MSK Stack - Amazon Managed Streaming for Apache Kafka
Creates and configures an MSK cluster with 3 brokers
"""

from aws_cdk import (
    aws_msk as msk,
    aws_ec2 as ec2,
    aws_logs as logs,
    Tags,
    CfnOutput,
    Stack,
    RemovalPolicy,
)
from constructs import Construct
import os
from dotenv import load_dotenv

load_dotenv()


class MSKStack(Stack):
    """Stack for MSK cluster"""

    def __init__(
        self,
        scope: Construct,
        id: str,
        vpc: ec2.IVpc,
        msk_security_group: ec2.ISecurityGroup,
        **kwargs,
    ):
        super().__init__(scope, id, **kwargs)

        project_name = os.getenv("PROJECT_NAME", "transactional-datalake")
        environment = os.getenv("ENVIRONMENT", "dev")
        broker_count = int(os.getenv("MSK_BROKER_COUNT", "3"))
        broker_type = os.getenv("MSK_BROKER_TYPE", "kafka.m5.large")

        # CloudWatch Logs for MSK
        msk_log_group = logs.LogGroup(
            self,
            "MSKLogGroup",
            log_group_name=f"/aws/msk/{project_name}-{environment}",
            retention=logs.RetentionDays.TWO_WEEKS,
            removal_policy=RemovalPolicy.DESTROY,
        )

        # Get private subnet IDs
        private_subnet_ids = [subnet.subnet_id for subnet in vpc.private_subnets]

        # Create MSK cluster using CfnCluster
        self.cluster = msk.CfnCluster(
            self,
            "DataLakeMSKCluster",
            broker_node_group_info=msk.CfnCluster.BrokerNodeGroupInfoProperty(
                instance_type=broker_type,
                client_subnets=private_subnet_ids,
                security_groups=[msk_security_group.security_group_id],
                storage_info=msk.CfnCluster.StorageInfoProperty(
                    ebs_storage_info=msk.CfnCluster.EBSStorageInfoProperty(
                        volume_size=1000,
                    )
                ),
            ),
            cluster_name=f"{project_name}-{environment}-cluster",
            kafka_version="3.6.0",
            number_of_broker_nodes=broker_count,
            encryption_info=msk.CfnCluster.EncryptionInfoProperty(
                encryption_in_transit=msk.CfnCluster.EncryptionInTransitProperty(
                    client_broker="TLS",
                ),
            ),
            client_authentication=msk.CfnCluster.ClientAuthenticationProperty(
                sasl=msk.CfnCluster.SaslProperty(
                    scram=msk.CfnCluster.ScramProperty(
                        enabled=True,
                    ),
                ),
            ),
            logging_info=msk.CfnCluster.LoggingInfoProperty(
                broker_logs=msk.CfnCluster.BrokerLogsProperty(
                    cloud_watch_logs=msk.CfnCluster.CloudWatchLogsProperty(
                        enabled=True,
                        log_group=msk_log_group.log_group_name,
                    ),
                    firehose=msk.CfnCluster.FirehoseProperty(
                        enabled=False,
                    ),
                    s3=msk.CfnCluster.S3Property(
                        enabled=False,
                    ),
                ),
            ),
        )

        # Output cluster information
        CfnOutput(
            self,
            "MSKClusterName",
            value=self.cluster.cluster_name or "transactional-datalake-dev-cluster",
            description="MSK Cluster Name",
            export_name=f"{project_name}-{environment}-msk-cluster-name",
        )

        CfnOutput(
            self,
            "MSKClusterArn",
            value=self.cluster.attr_arn,
            description="MSK Cluster ARN",
            export_name=f"{project_name}-{environment}-msk-cluster-arn",
        )

        CfnOutput(
            self,
            "MSKBrokerNodes",
            value=str(broker_count),
            description="Number of MSK Broker Nodes",
        )

        CfnOutput(
            self,
            "MSKInstanceType",
            value=broker_type,
            description="MSK Instance Type",
        )

        # Add tags
        Tags.of(self).add("Project", project_name)
        Tags.of(self).add("Environment", environment)
        Tags.of(self).add("Component", "Messaging")