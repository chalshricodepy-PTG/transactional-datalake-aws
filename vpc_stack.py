"""
VPC Stack - Network Infrastructure
Creates a VPC with public and private subnets across multiple AZs
"""

from aws_cdk import (
    aws_ec2 as ec2,
    Tags,
    CfnOutput,
    Stack,
)
from constructs import Construct


class VPCStack(Stack):
    """Stack for VPC and networking resources"""

    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        project_name = "transactional-datalake"
        environment = "dev"

        # Create VPC with public and private subnets
        self.vpc = ec2.Vpc(
            self,
            "DataLakeVPC",
            max_azs=3,
            ip_addresses=ec2.IpAddresses.cidr("10.0.0.0/16"),
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    subnet_type=ec2.SubnetType.PUBLIC,
                    name="Public",
                    cidr_mask=24,
                ),
                ec2.SubnetConfiguration(
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                    name="Private",
                    cidr_mask=24,
                ),
            ],
            nat_gateways=1,
            enable_dns_hostnames=True,
            enable_dns_support=True,
        )

        # MSK Security Group
        self.msk_security_group = ec2.SecurityGroup(
            self,
            "MSKSecurityGroup",
            vpc=self.vpc,
            description="Security group for MSK cluster",
            allow_all_outbound=True,
        )

        # Allow traffic on Kafka plaintext port from VPC
        self.msk_security_group.add_ingress_rule(
            peer=ec2.Peer.ipv4("10.0.0.0/16"),
            connection=ec2.Port.tcp(9092),
            description="Kafka plaintext port from VPC",
        )

        # Allow traffic on Kafka TLS port from VPC
        self.msk_security_group.add_ingress_rule(
            peer=ec2.Peer.ipv4("10.0.0.0/16"),
            connection=ec2.Port.tcp(9094),
            description="Kafka TLS port from VPC",
        )

        # Allow traffic on Zookeeper port from VPC
        self.msk_security_group.add_ingress_rule(
            peer=ec2.Peer.ipv4("10.0.0.0/16"),
            connection=ec2.Port.tcp(2181),
            description="Zookeeper port from VPC",
        )

        # Glue Security Group
        self.glue_security_group = ec2.SecurityGroup(
            self,
            "GlueSecurityGroup",
            vpc=self.vpc,
            description="Security group for Glue jobs",
            allow_all_outbound=True,
        )

        # Lambda Security Group
        self.lambda_security_group = ec2.SecurityGroup(
            self,
            "LambdaSecurityGroup",
            vpc=self.vpc,
            description="Security group for Lambda functions",
            allow_all_outbound=True,
        )

        # MWAA Security Group
        self.mwaa_security_group = ec2.SecurityGroup(
            self,
            "MWAASecurityGroup",
            vpc=self.vpc,
            description="Security group for MWAA",
            allow_all_outbound=True,
        )

        # Allow Glue to communicate with MWAA
        self.glue_security_group.add_ingress_rule(
            peer=ec2.Peer.ipv4(self.vpc.vpc_cidr_block),
            connection=ec2.Port.tcp(443),
            description="Allow Glue to MWAA",
        )

        # Allow self communication
        self.mwaa_security_group.add_ingress_rule(
            peer=ec2.Peer.ipv4(self.vpc.vpc_cidr_block),
            connection=ec2.Port.all_tcp(),
            description="Allow MWAA internal communication",
        )

        # Output VPC information
        CfnOutput(
            self,
            "VPCId",
            value=self.vpc.vpc_id,
            description="VPC ID",
            export_name=f"{project_name}-{environment}-vpc-id",
        )

        CfnOutput(
            self,
            "PrivateSubnets",
            value=",".join([subnet.subnet_id for subnet in self.vpc.private_subnets]),
            description="Private Subnet IDs",
            export_name=f"{project_name}-{environment}-private-subnets",
        )

        CfnOutput(
            self,
            "MSKSecurityGroupId",
            value=self.msk_security_group.security_group_id,
            description="MSK Security Group ID",
            export_name=f"{project_name}-{environment}-msk-sg",
        )

        CfnOutput(
            self,
            "GlueSecurityGroupId",
            value=self.glue_security_group.security_group_id,
            description="Glue Security Group ID",
            export_name=f"{project_name}-{environment}-glue-sg",
        )

        CfnOutput(
            self,
            "LambdaSecurityGroupId",
            value=self.lambda_security_group.security_group_id,
            description="Lambda Security Group ID",
            export_name=f"{project_name}-{environment}-lambda-sg",
        )

        CfnOutput(
            self,
            "MWAASecurityGroupId",
            value=self.mwaa_security_group.security_group_id,
            description="MWAA Security Group ID",
            export_name=f"{project_name}-{environment}-mwaa-sg",
        )

        # Add tags to all resources
        Tags.of(self).add("Project", project_name)
        Tags.of(self).add("Environment", environment)
        Tags.of(self).add("Component", "Network")