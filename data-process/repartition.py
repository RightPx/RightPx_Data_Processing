import os
import boto3
import requests
import urllib.parse
import json

#from aws_lambda_powertools.logging import Logger
#logger = Logger()

s3 = boto3.client('s3')
s3c = boto3.client('athena', region_name='us-east-2', aws_access_key_id='AKIA5GWC4RWCLLVC5TCO',aws_secret_access_key='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')


table_name = "t4"
format_type = "parquet"
external_location = "s3://zigna-nsa-payer-data-parquet-staging/jan_load_month/datazigna_final_data_model_ui_partitioned_v1/"
external_location_2 = "s3://zigna-nsa-payer-data-parquet-staging/test2/"
compression_type = "SNAPPY"
partition_column = "partition_column"
database_name = "excellus"
table_source_name = "datazigna_final_data_model_ui_partitioned_v1"
reporting_entity_name = "Excellus"

def lambda_handler(event, context):
    """
    Args:
        url: Url of file to download
        chunk_size_in_MB: Size of chunks to download in (minimum 5MB, or completion will fail)
    return:
        location: Location of uploaded file
    """

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    print(key)
    
    data = s3.get_object(Bucket=bucket, Key=key)
    f = data.get('Body').read().decode('utf-8')
    json_data = json.loads(f)
    
    partition_values = json_data['partition_value']
    size = json_data['size']
    # query = json_data['query']
   
    repartition(partition_values)
    # bucket = event['Records'][0]['s3']['bucket']['name']
    # key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    # copy_source = {
    #                     'Bucket': bucket,
    #                     'Key': key,
    #                   }
    # s3.copy(copy_source,os.environ['PROCESSED_BUCKET'],key)
    # delete_object(os.environ['INPUT_BUCKET'], key)

    # return location
    
def repartition(partition_values):
    partition_value = partition_values
    
    query1 = """
    DROP TABLE IF EXISTS default.t4;
    """
    
    query = """
    CREATE TABLE "default"."{}" WITH (
      format = '{}',
      external_location = '{}',
      write_compression = '{}',
      partitioned_by = ARRAY [ '{}' ]
    ) AS
    SELECT * FROM "{}"."{}"
    where {} = '{}' and reporting_entity_name != '{}';
    """.format(table_name, format_type, external_location, compression_type, partition_column, database_name, table_source_name, partition_column, partition_value, reporting_entity_name)


    response = s3c.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': 'default'
        },
        ResultConfiguration={
            'OutputLocation': external_location
        }
    )

    query_execution_id = response['QueryExecutionId']   
    


    while True:
        query_status = s3c.get_query_execution(QueryExecutionId=query_execution_id)
        query_state = query_status['QueryExecution']['Status']['State']
        if query_state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
    if query_state == 'SUCCEEDED':
        print("Table created successfully.")
    else:
        print("Table creation failed. Status: {}".format(query_state))
        
    response2 = s3c.start_query_execution(
        QueryString=query1,
        QueryExecutionContext={
            'Database': 'default'
        },
        ResultConfiguration={
            'OutputLocation': external_location_2
        }
    )

    query_execution_id2 = response2['QueryExecutionId']
    
    while True:
        query_status2 = s3c.get_query_execution(QueryExecutionId=query_execution_id2)
        query_state2 = query_status2['QueryExecution']['Status']['State']
        if query_state2 in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
    if query_state2 == 'SUCCEEDED':
        print("Table dropped successfully.")
    else:
        print("Table dropping failed. Status: {}".format(query_state2))
    