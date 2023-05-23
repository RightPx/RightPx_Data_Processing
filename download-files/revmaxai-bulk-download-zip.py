import json
import zipfile
import os
import urllib
import boto3
from io import BytesIO


s3r = boto3.resource("s3")
s3 = boto3.client("s3")
def delete_object(bucket, filename):
    
    paginator = s3.get_paginator('list_object_versions')
    response_iterator = paginator.paginate(Bucket=bucket)
    
    for response in response_iterator:
        versions = response.get('Versions', [])
        versions.extend(response.get('DeleteMarkers', []))
        
        for version_id in [x['VersionId'] for x in versions if x['Key'] == filename and x['VersionId'] == 'null']:
            #print('Deleting {} version {}'.format(filename, version_id))
            s3.delete_object(Bucket=bucket, Key=filename, VersionId=version_id)
            
    
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
        fname = os.environ['KEY'] + '/' + filename
        print(fname)
        file_info = z.getinfo(filename)
        bucket_name = os.environ['OUTPUT_BUCKET']
        s3r.meta.client.upload_fileobj(
            z.open(filename),
            Bucket=bucket_name,
            Key= fname
        )
        c+=1
    print(f'completed extracting {c} files from the zipfile {fname}')

def lambda_handler(event, context):

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    print(key)
    if key.endswith('zip') and key.size < 102486 and '2023' in key:
        zip_extract(bucket, key)
        # delete the zipped object after unzipping the required file
        delete_object(bucket, key)
    else:
        print(f'The triggered file {key} is not a valid zip file')

    return 'Done'

