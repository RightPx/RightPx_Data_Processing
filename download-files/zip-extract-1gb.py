import json
import zipfile
import os

s3r = boto3.resource("s3", region_name="us-east-2", aws_access_key_id="AKIA5GWC4RWCLLVC5TCO",aws_secret_access_key="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")
s3c = boto3.client("s3", region_name="us-east-2", aws_access_key_id="AKIA5GWC4RWCLLVC5TCO",aws_secret_access_key="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")

def delete_object(bucket, filename):
    
    paginator = s3.get_paginator('list_object_versions')
    response_iterator = paginator.paginate(Bucket=bucket)
    
    for response in response_iterator:
        versions = response.get('Versions', [])
        versions.extend(response.get('DeleteMarkers', []))
        
        for version_id in [x['VersionId'] for x in versions if x['Key'] == filename and x['VersionId'] == 'null']:
            #print('Deleting {} version {}'.format(filename, version_id))
            s3.delete_object(Bucket=bucket, Key=filename, VersionId=version_id)

def gzip_extract(bucket, gzipped_key, uncompressed_key):
    """
     ****LAMBDA & EC2 SPECIFIC****
     Extracts gzip files upto 1.5 GB via lambda and stores the unzipped file in the specified s3 location.
     Extracts gzip files greater than 1.5 GB via AWS workspace. EC2 (we will have to experiment)
    """
    now  = datetime.now()
    print(now)
    s3c.upload_fileobj(
        Fileobj=gzip.GzipFile(
            None,
            'rb',
            fileobj=BytesIO(s3c.get_object(Bucket=bucket, Key=gzipped_key)['Body'].read())),
        Bucket=bucket,
        Key=uncompressed_key)   

    now  = datetime.now()
    print(now) 
    
def zip_extract(bucket_name, key):
    """
    ****LAMBDA SPECIFIC****
    Extracts zipped files less than 1.5 gb
    Stores the unzipped files in specified s3 bucket.
    """
    zip_obj = s3r.Object(bucket_name=bucket_name, key=key)
    buffer = BytesIO(zip_obj.get()["Body"].read())
    z = zipfile.ZipFile(buffer)
    c = 0
    for filename in z.namelist():
        print(filename)
        file_info = z.getinfo(filename)
        self.s3r.meta.client.upload_fileobj(
            z.open(filename),
            Bucket=bucket_name,
            Key=f'{filename}'
        )
        c+=1
    print(f'completed extracting {c} files from the zipfile {key}')

def lambda_handler(event, context):

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    print(key)
    if key.endswith('zip'):
        #key = os.environ['OUTPUT_BUCKET']
        #zip_extract(bucket, key)
        print('zip')
        
    elif key.endswith('gz'):
        gzip_extract(bucket, key,key.replace('.gz',''))
        
    else:
        print(f'The triggered file {key} is not a valid zip file')

    return {
        'statusCode': 200,
        'body': json.dumps('Done!')
    }



