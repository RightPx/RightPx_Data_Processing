import os
import boto3
import requests
import urllib.parse
import json

#from aws_lambda_powertools.logging import Logger
#logger = Logger()

s3 = boto3.client('s3')

def delete_object(bucket, filename):
    
    paginator = s3.get_paginator('list_object_versions')
    response_iterator = paginator.paginate(Bucket=bucket)
    
    for response in response_iterator:
        versions = response.get('Versions', [])
        versions.extend(response.get('DeleteMarkers', []))
        
        for version_id in [x['VersionId'] for x in versions if x['Key'] == filename and x['VersionId'] == 'null']:
            #print('Deleting {} version {}'.format(filename, version_id))
            s3.delete_object(Bucket=bucket, Key=filename, VersionId=version_id)
            
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
    
    url = json_data['url']
    pname = json_data['pname']
    #print(url)
    fname = pname + json_data['fname'] +'.'+ json_data['ftype']
    #print(fname)
    
    chunk_size_in_MB = int(os.environ['CHUNK_SIZE_IN_MB'])
    #logger.info({"url": url, "chunk_size": chunk_size_in_MB, "bucket":bucket})
    location = main(
        url=url,
        chunk_size_in_MB=chunk_size_in_MB,
        key=fname,
        bucket=os.environ['OUTPUT_BUCKET'],
    )
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    copy_source = {
                        'Bucket': bucket,
                        'Key': key,
                      }
    s3.copy(copy_source,os.environ['PROCESSED_BUCKET'],key)
    delete_object(os.environ['INPUT_BUCKET'], key)

    return location


def main(url, chunk_size_in_MB, key, bucket):
    """
    Downloads the file at the url provided in chunks, and uploads to <key> in <bucket>
    """

    upload_id = create_multipart_upload(bucket, key)
    parts = download_and_upload(url, upload_id, key, bucket, chunk_size_in_MB)
    location = complete_multipart_upload(key, bucket, upload_id, parts)
    
    return location

def create_multipart_upload(bucket, key):
    # Create Multipart upload
    response = s3.create_multipart_upload(
        Bucket=bucket,
        Key=key)
    upload_id = response['UploadId']
    #logger.info({"message": "Multipart upload created", "upload_id": upload_id})
    return upload_id

def download_and_upload(url, upload_id, key, bucket, chunk_size_in_MB):
    parts = []

    # stream
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        #logger.info({"headers": r.headers})

        # download & upload chunks
        for part_number, chunk in enumerate(r.iter_content(chunk_size=chunk_size_in_MB * 1024 * 1024)):
            response = s3.upload_part(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=part_number + 1,
                Body=chunk,
            )
            #logger.debug({"UploadID": upload_id, "part_number": part_number + 1, "status": "uploaded"})
            parts.append({
                "ETag": response['ETag'],
                "PartNumber": part_number + 1,
            })
    #logger.debug(parts)
    return parts

def complete_multipart_upload(key, bucket, upload_id, parts):
    # complete multipart upload
    print("Completed uploaded, closing multipart")
    response = s3.complete_multipart_upload(
        Bucket=bucket,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )

    location = response['Location']
    eTag = response['ETag']
    
    #logger.debug(response)
    #logger.info({"location": location, "eTag": eTag})

    return location
