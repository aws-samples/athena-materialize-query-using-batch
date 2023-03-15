import awswrangler as wr
import urllib
import os

    
def lambda_handler(event, context):

    try:
        bucket = event['Records'][0]['s3']['bucket']['name']
        output_bucket = os.environ['OUTPUT_BUCKET_NAME']
        key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
        file_name = key.split("/")[-1].split(".")[0]
        df = wr.s3.read_csv(f"s3://{bucket}/{key}", dataset=True)
        
        # will create a table in the Default Glue DB or whatever is set in global config: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb
        # table name will be the filename
        wr.s3.to_parquet(
            df, 
            path=f"s3://{output_bucket}/ingested_csv/{file_name}/",
            dataset=True,
            table=file_name,
            mode="append", #default is append
            index=False, 
            schema_evolution=True,
        )
        
        print(" Successfully converted the file")
        
        return{
            'StatusCode':'200',
            'Message':'Successfully Completed'
        }
    except Exception as e:
        print (e)
        print ('Error Occurred.')
        raise e
