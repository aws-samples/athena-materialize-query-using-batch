import os
import aws_cdk as cdk

from stacks import BatchMaterializeInfraStack, SampleBatchJob,FileTypeConversionStack

app = cdk.App()

my_env = cdk.Environment(
    account=os.environ["CDK_DEFAULT_ACCOUNT"],
    region=os.environ["CDK_DEFAULT_REGION"]
)

base_infra_stack = BatchMaterializeInfraStack(app, 
    construct_id="BatchMaterializeQueryStack",
    cdk_env=my_env
    )

sample_job_stack = SampleBatchJob(app, 
    construct_id="SampleJobStack",
    base_env=base_infra_stack,
    cdk_env=my_env
    )
sample_job_stack.add_dependency(base_infra_stack)

file_conversion_stack = FileTypeConversionStack(app, 
    construct_id="FileConversionLambdaStack",
)

app.synth()
