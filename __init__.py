"""Infrastructure stacks module"""

from infrastructure.stacks.vpc_stack import VPCStack
from infrastructure.stacks.iam_stack import IAMStack
from infrastructure.stacks.msk_stack import MSKStack
from infrastructure.stacks.s3_stack import S3DataLakeStack
from infrastructure.stacks.glue_catalog_stack import GlueCatalogStack

__all__ = [
    "VPCStack",
    "IAMStack",
    "MSKStack",
    "S3DataLakeStack",
    "GlueCatalogStack",
]