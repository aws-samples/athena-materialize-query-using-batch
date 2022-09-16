from aws_cdk import (
    Stack,
    Environment
)
from constructs import Construct
from aws_cdk import (
    aws_s3 as _s3,
    aws_s3_notifications as s3n,
    aws_glue as _glue,
    aws_lambda  as _lambda,
    aws_iam as _iam,
)
from .helpers import s3_glue_iam


class FileTypeConversionStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, cdk_env:Environment, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # creating a s3 bucket for input file.

        s3_input = _s3.Bucket(self, "s3_input_lambda_wrangler",
            encryption=_s3.BucketEncryption.S3_MANAGED,
            bucket_key_enabled=False,
            versioned=True,
            block_public_access=_s3.BlockPublicAccess.BLOCK_ALL
        )

        # creating a s3 bcuket for output files.

        s3_output = _s3.Bucket(self, "s3_output_lambda_wrangler", 
            encryption=_s3.BucketEncryption.S3_MANAGED,
            bucket_key_enabled=False,
            versioned=True,
            block_public_access=_s3.BlockPublicAccess.BLOCK_ALL
        )

        # creating a lambda to take s3 and convert the file to parquet format.

        # using managed layers, see docs here for latest version
        # https://aws-data-wrangler.readthedocs.io/en/stable/layers.html
        lambda_wrangler_layer = _lambda.LayerVersion.from_layer_version_arn(
            self,  # awswrangler
            id="awswrangler-layer",
            layer_version_arn="arn:aws:lambda:{cdk_env.region}:336392948345:layer:AWSDataWrangler-Python38:8",
        )


        lambda_role = _iam.Role(
            self,
            id=f"FileIngestionLambdaIAMRole",
            assumed_by=_iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                _iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ],
        )

        function = _lambda.Function(self, "lambda_function", 
            runtime=_lambda.Runtime.PYTHON_3_8,
            handler="lambda_func.lambda_handler",
            code=_lambda.Code.from_asset('../src/file_conversion_lambda'),
            layers=[lambda_wrangler_layer],
            environment={'OUTPUT_BUCKET_NAME': s3_output.bucket_name},
            role=lambda_role
        )

        # function s3 permissions
        s3_input.grant_read_write(function)
        s3_output.grant_read_write(function)

        s3_input.add_event_notification(
            _s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(function),
            _s3.NotificationKeyFilter(
                prefix="s3_inputs/",
                suffix=".csv"
            )
        )

        # creating a glue data catalog for s3 as source.
        target_db_name = "TARGET_DB_FOR_S3_INGEST"
        glue_role = _iam.Role(self, 
            'glueRole', 
            assumed_by=_iam.ServicePrincipal('glue.amazonaws.com'),
            managed_policies=[
                _iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSGlueServiceRole'),
                # S3 Read
                _iam.ManagedPolicy(
                    self,
                    id=f"GlueCrawlerS3Read",
                    managed_policy_name=f"GlueCrawlerS3Read",
                    statements=[
                        _iam.PolicyStatement(
                            effect=_iam.Effect.ALLOW,
                            resources=[
                                f"arn:aws:s3:::{s3_input.bucket_name}/*",
                                f"arn:aws:s3:::{s3_input.bucket_name}*",
                            ],
                            actions=["s3:List*","s3:Get*"],
                        ),
                        s3_glue_iam.GlueDataCatalogPermissions(env=cdk_env,
                            database=target_db_name
                        ).get_glue_policy("write")
                    ]
                )
            ]
        )

        path_name = s3_output.bucket_arn

        crawler_s3 = _glue.CfnCrawler(self, "Crawler_Glue", role=glue_role.role_arn,
            database_name=target_db_name,
            targets={
                's3Targets' :[{"path": path_name}]
            }
        )
