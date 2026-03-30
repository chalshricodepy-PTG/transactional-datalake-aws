"""
Transactional Data Lake AWS CDK Application
Deploys MSK, Glue, S3, and supporting infrastructure
"""

import os
from aws_cdk import App, Environment

# Import all stacks
from infrastructure.stacks.vpc_stack import VPCStack
from infrastructure.stacks.iam_stack import IAMStack
from infrastructure.stacks.msk_stack import MSKStack
from infrastructure.stacks.s3_stack import S3DataLakeStack
from infrastructure.stacks.glue_catalog_stack import GlueCatalogStack
from infrastructure.stacks.glue_jobs_stack import GlueJobsStack

def create_stacks():
    """Create and configure all CDK stacks"""

    app = App()

    # Get AWS account and region
    account = os.environ.get("AWS_ACCOUNT_ID") or "043505372147"
    region = os.environ.get("AWS_REGION") or "us-east-2"

    env = Environment(account=account, region=region)

    # Configuration
    project_name = "transactional-datalake"
    environment = "dev"

    print("=" * 70)
    print("DEPLOYING TRANSACTIONAL DATA LAKE PIPELINE")
    print("=" * 70)
    print(f"Project: {project_name}")
    print(f"Environment: {environment}")
    print(f"AWS Account: {account}")
    print(f"AWS Region: {region}")
    print("=" * 70)

    print("\nCreating VPC Stack...")
    vpc_stack = VPCStack(
        app,
        "transactional-datalake-dev-vpc-stack",
        env=env,
    )

    print("Creating IAM Stack...")
    iam_stack = IAMStack(
        app,
        "transactional-datalake-dev-iam-stack",
        project_name=project_name,
        environment=environment,
        env=env,
    )
    iam_stack.add_dependency(vpc_stack)

    print("Creating MSK Stack...")
    msk_stack = MSKStack(
        app,
        "transactional-datalake-dev-msk-stack",
        vpc=vpc_stack.vpc,
        msk_security_group=vpc_stack.msk_security_group,
        env=env,
    )
    msk_stack.add_dependency(vpc_stack)

    print("Creating S3 Data Lake Stack...")
    s3_data_lake_stack = S3DataLakeStack(
        app,
        "transactional-datalake-dev-s3-stack",
        project_name=project_name,
        environment=environment,
        env=env,
    )
    s3_data_lake_stack.add_dependency(iam_stack)

    print("Creating Glue Catalog Stack...")
    glue_catalog_stack = GlueCatalogStack(
        app,
        "transactional-datalake-dev-glue-catalog-stack",
        project_name=project_name,
        environment=environment,
        env=env,
    )
    glue_catalog_stack.add_dependency(s3_data_lake_stack)

    print("Creating Glue Jobs Stack...")
    glue_jobs_stack = GlueJobsStack(
        app,
        "transactional-datalake-dev-glue-jobs-stack",
        vpc=vpc_stack.vpc,
        glue_role=iam_stack.glue_role,
        msk_security_group=vpc_stack.msk_security_group,
        glue_security_group=vpc_stack.glue_security_group,
        raw_bucket=s3_data_lake_stack.raw_bucket,
        curated_bucket=s3_data_lake_stack.curated_bucket,
        project_name=project_name,
        environment=environment,
        env=env,
    )
    glue_jobs_stack.add_dependency(s3_data_lake_stack)
    glue_jobs_stack.add_dependency(iam_stack)
    glue_jobs_stack.add_dependency(vpc_stack)
    glue_jobs_stack.add_dependency(glue_catalog_stack)

    print("\n" + "=" * 70)
    print("✓ CDK stacks defined successfully!")
    print("✓ Next step: Run 'npx cdk deploy' to deploy all stacks")
    print("=" * 70)

    return app


if __name__ == "__main__":
    app = create_stacks()
    app.synth()