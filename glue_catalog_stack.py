"""
Glue Catalog Stack - Metadata Management
Creates Glue database and initial table structure
"""

from aws_cdk import (
    aws_glue as glue,
    Tags,
    CfnOutput,
    Stack,
)
from constructs import Construct


class GlueCatalogStack(Stack):
    """Stack for Glue Catalog resources"""

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

        # Create Glue databases
        self.raw_database = self._create_raw_database()
        self.curated_database = self._create_curated_database()

        # Output database information
        self._create_outputs()

        # Add tags
        Tags.of(self).add("Project", project_name)
        Tags.of(self).add("Environment", environment)
        Tags.of(self).add("Component", "Metadata")

    def _create_raw_database(self) -> glue.CfnDatabase:
        """Create Glue database for raw data"""
        database = glue.CfnDatabase(
            self,
            "RawDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=f"{self.project_name_val}_{self.environment_val}_raw",
                description="Raw data database",
            ),
        )

        return database

    def _create_curated_database(self) -> glue.CfnDatabase:
        """Create Glue database for curated data (Iceberg tables)"""
        database = glue.CfnDatabase(
            self,
            "CuratedDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=f"{self.project_name_val}_{self.environment_val}_curated",
                description="Curated data database (Iceberg tables)",
            ),
        )

        return database

    def _create_outputs(self):
        """Create CloudFormation outputs for database names"""
        CfnOutput(
            self,
            "RawDatabaseName",
            value=f"{self.project_name_val}_{self.environment_val}_raw",
            description="Raw Data Glue Database Name",
            export_name=f"{self.project_name_val}-{self.environment_val}-raw-db",
        )

        CfnOutput(
            self,
            "CuratedDatabaseName",
            value=f"{self.project_name_val}_{self.environment_val}_curated",
            description="Curated Data Glue Database Name",
            export_name=f"{self.project_name_val}-{self.environment_val}-curated-db",
        )