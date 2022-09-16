from aws_cdk import (
    aws_batch as batch,
    aws_iam as iam,
    aws_s3 as s3
)
from typing import List
from constructs import Construct


def _get_batch_job_exec_role(scope):
    exec_role = iam.Role(scope,"ExecRole",
        assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
        description="Execution Role used by Batch"
    )
    exec_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonECSTaskExecutionRolePolicy"))
    exec_role.add_to_policy(iam.PolicyStatement(
        actions=["logs:CreateLogGroup","logs:CreateLogStream","logs:PutLogEvents","logs:DescribeLogStreams"],
        resources=["arn:aws:logs:*:*:*"]
    ))
    return exec_role

def _get_batch_job_role_arn(scope, policies:List) -> iam.Role:
    job_role = iam.Role(scope,"BatchJobRole",
        assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
        description="Job Role used by Batch with access to resources for running queries"
    )
    
    for p in policies:
        job_role.add_to_policy(p)

    return job_role

def get_batch_job_definition(scope, ecr_repo_uri:str, job_def_name:str, cmd:List[str],job_role_policies:List[iam.PolicyStatement],tmp_athena_bucket:s3.Bucket) -> batch.CfnJobDefinition:

    job_role=_get_batch_job_role_arn(scope,job_role_policies)
    
    tmp_athena_bucket.grant_read_write(job_role)

    sample_job_def = batch.CfnJobDefinition(scope,"CfnSampleJobDef",
        type="container",
        job_definition_name=job_def_name,
        platform_capabilities=["FARGATE"],
        container_properties=batch.CfnJobDefinition.ContainerPropertiesProperty(
            image=ecr_repo_uri,
            command=cmd,
            fargate_platform_configuration=batch.CfnJobDefinition.FargatePlatformConfigurationProperty(
                platform_version="1.4.0"
            ),
            network_configuration=batch.CfnJobDefinition.NetworkConfigurationProperty(
                assign_public_ip="ENABLED"
            ),
            resource_requirements=[
                batch.CfnJobDefinition.ResourceRequirementProperty(
                type="VCPU",value="1"
                ),
                batch.CfnJobDefinition.ResourceRequirementProperty(
                type="MEMORY",value="2048"
                )
            ],
            execution_role_arn=_get_batch_job_exec_role(scope).role_arn,
            job_role_arn=job_role.role_arn
        )
    )
    return sample_job_def


