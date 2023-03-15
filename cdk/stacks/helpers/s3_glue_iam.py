"""Iam helpers for data resource permissioning"""
import os
from urllib.parse import urlparse
import boto3
from dataclasses import dataclass, field
from typing import List
from botocore.errorfactory import ClientError
import logging

from aws_cdk import (
    aws_iam as iam,
    Environment
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

LOGGER = logging.getLogger()

@dataclass
class GlueDataCatalogPermissions:
    """Helper class to get Glue and S3 IAM Access policies given a database/table"""

    env : Environment
    database: str # db name
    tables: List[str] = field(default_factory=lambda: ["*"])  # default to all tables if unspecified
    #in case permissions are being requested for write access for a new table, this is the S3 bucket it will be stored in
    write_destination_bucket: str = field(default_factory=lambda: None)
    region: str = field(init=False)
    account_id: str = field(init=False)

    def __post_init__(self):
        self.region = self.env.region
        self.account_id = self.env.account


    def get_s3_policy(self, access_level: str) -> iam.PolicyStatement:
        """Generates S3 policy statement"""
        
        client = boto3.client('glue')
        resources=[]
        for t in self.tables:
            try:
                s3_path = client.get_table(DatabaseName=self.database,Name=t)['Table']['StorageDescriptor']['Location']
                print(f'Found S3 Path:: {s3_path}')
            except ClientError as exc: # table doesn't exist
                if self.write_destination_bucket is not None and exc.response['Error']['Code']=='EntityNotFoundException':
                    # check if permissions are being requested to create new table
                    s3_path=f's3://{self.write_destination_bucket}/{self.database}/{t}'
                    print(f'DIDNT FIND S3 Path:: {s3_path}')
                else:
                    LOGGER.error(f"TABLE:: [{t}] was not found in DB [{self.database}] and no write_destination_bucket was \
                                 passed. Ensure the table already exists or pass in a write_destination_bucket where the table\
                                 will store data.")
                    LOGGER.exception(exc)
            
            loc = urlparse(s3_path)
            bucket=loc.netloc
            path=loc.path[1:]

            db_arn=f'arn:aws:s3:::{bucket}'
            folder=os.path.join(db_arn,f"{path}_$folder$") # https://aws.amazon.com/premiumsupport/knowledge-center/emr-s3-empty-files/
            star=os.path.join(db_arn, path, "*")

            resources.append(db_arn)
            resources.append(folder)
            resources.append(star)

        read_actions = ["s3:GetObject*", "s3:GetBucket*", "s3:List*", "s3:Head*"]
        write_actions = ["s3:PutObject*", "s3:Abort*", "s3:DeleteObject*"] + read_actions

        actions = write_actions if access_level == "write" else read_actions
        return iam.PolicyStatement(actions=actions, resources=resources)

    def get_glue_policy(self, access_level: str) -> iam.PolicyStatement:
        """Generates Glue database and table policy statement"""
        read_actions = ["glue:Get*", "glue:BatchGet*"]
        write_actions = [
            "glue:CreateTable",
            "glue:CreatePartition",
            "glue:UpdatePartition",
            "glue:UpdateTable",
            "glue:DeleteTable",
            "glue:DeletePartition",
            "glue:BatchCreatePartition",
            "glue:BatchDeletePartition",
        ] + read_actions
        base_arn = f"arn:aws:glue:{self.region}:{self.account_id}"
        table_resources = (
            [f"{base_arn}:table/{self.database}/*"]
            if "*" in self.tables
            else [f"{base_arn}:table/{self.database}/{table}*" for table in self.tables]
        )
        resources = [
            f"{base_arn}:catalog",
            f"{base_arn}:database/{self.database}",
        ] + table_resources
        
        actions = write_actions if access_level == "write" else read_actions
        
        return iam.PolicyStatement(actions=actions, resources=resources)