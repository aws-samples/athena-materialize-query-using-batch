from aws_cdk import (
    Stack, Environment, Aws
)
from constructs import Construct
from .helpers import batch_job_utils, s3_glue_iam

class SampleBatchJob(Stack):
    
    def __init__(self, scope: Construct, construct_id: str, base_env:Stack, cdk_env:Environment,**kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        source_tables = s3_glue_iam.GlueDataCatalogPermissions(env=cdk_env,
            database="covid-19",
            tables=['nytimes_counties']
        )
        dst_tables = s3_glue_iam.GlueDataCatalogPermissions(env=cdk_env,
            database="covid-19",
            tables=['covid_state_data']
        )
        
        policies = [source_tables.get_glue_policy('read'),
            source_tables.get_s3_policy('read'),
            dst_tables.get_glue_policy('write'),
            dst_tables.get_s3_policy('write')
        ]
        
        sample_job = batch_job_utils.get_batch_job_definition(
            self,
            base_env=base_env,
            job_def_name="sample-covid-sql-athena-mat",
            cmd=["python3", "materialize_athena_query.py", "some_project/sample-nyc-covid.sql", "<<<TARGET_BUCKET>>>", "covid-19","covid_state_data","aggregated covid data","<<<TMP_ATHENA_BUCKET>>>"],
            job_role_policies=policies,
            schedule = "" # some cron syntax such as (0 10 * * *) for 10am everyday
        )