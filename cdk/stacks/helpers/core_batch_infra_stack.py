from aws_cdk import (
    Stack, 
    aws_batch as batch,
    aws_ec2 as ec2,
    aws_ecr as ecr,
    aws_iam as iam,
    aws_s3 as s3,
    Environment
)
from constructs import Construct

class BatchMaterializeInfraStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, cdk_env:Environment, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        account_id=cdk_env.account

        ecr_repo = ecr.Repository(self,"CfnECRRepo", repository_name="batch-materialize-env")
        # for example only, make a base ECR URI available for Batch jobs based on this stack
        self.base_ecr_repo_uri = ecr_repo.repository_uri

        vpc = ec2.Vpc(self, "CfnVPCBatchMaterializeQuery")

        batch_compute_env = batch.CfnComputeEnvironment(self,'CfnComputeEnvironmentBatchMaterializeQuery',
            type="MANAGED",
            compute_environment_name="BatchMaterializeQueryCE",
            state="Enabled",
            compute_resources=batch.CfnComputeEnvironment.ComputeResourcesProperty(
                maxv_cpus=8,
                type="FARGATE",
                subnets=vpc.select_subnets(
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_NAT
                ).subnet_ids,
                security_group_ids=[
                    ec2.SecurityGroup(self, "DefaultSG",
                        vpc=vpc
                    ).security_group_id
                ]
            )
        )
        
        batch_job_queue = batch.CfnJobQueue(self,"CfnJobQueueBatchMaterializeQuery",
            compute_environment_order=[batch.CfnJobQueue.ComputeEnvironmentOrderProperty(
                compute_environment=batch_compute_env.attr_compute_environment_arn,
                order=1
            )],
            priority=1,
            state="ENABLED"
        )
        
        athena_bucket = s3.Bucket.from_bucket_name(
            self,
            "athena-tmp-bucket",
            bucket_name=f'athena-tmp-bucket-{account_id}'
        )
        #make athena bucket available
        self.athena_tmp_bucket=athena_bucket

