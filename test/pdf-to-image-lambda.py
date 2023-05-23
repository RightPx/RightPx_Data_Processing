import os
import json
import boto3
from pathlib import Path
from urllib.parse import unquote_plus
from io import BytesIO

from pdf2image import convert_from_bytes


OUTPUT_BUCKET_NAME = os.environ["OUTPUT_BUCKET_NAME"]
OUTPUT_S3_PREFIX = os.environ["OUTPUT_S3_PREFIX"]
OUTPUT_S3_IMAGE_PREFIX = os.environ["OUTPUT_S3_IMAGE_PREFIX"]
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
SNS_ROLE_ARN = os.environ["SNS_ROLE_ARN"]


_SUPPORTED_FILE_EXTENSION = '.pdf'
DPI = 300
FMT = "png"

s3c = boto3.client("s3")
s3r = boto3.resource("s3")

def delete_object(bucket, filename):
    
    paginator = s3c.get_paginator('list_object_versions')
    response_iterator = paginator.paginate(Bucket=bucket)
    
    for response in response_iterator:
        versions = response.get('Versions', [])
        versions.extend(response.get('DeleteMarkers', []))
        
        for version_id in [x['VersionId'] for x in versions if x['Key'] == filename and x['VersionId'] == 'null']:
            print('Deleting {} version {}'.format(filename, version_id))
            s3c.delete_object(Bucket=bucket, Key=filename, VersionId=version_id)
            
            

def lambda_handler(event, context):

    # textract = boto3.client("textract")
    if event:
        file_obj = event["Records"][0]
        bucketname = str(file_obj["s3"]["bucket"]["name"])
        filename = unquote_plus(str(file_obj["s3"]["object"]["key"]))

        print(f"Bucket: {bucketname} ::: Key: {filename}")

        pdf_to_image(bucketname, filename)

        # response = textract.start_document_text_detection(
        #     DocumentLocation={"S3Object": {"Bucket": bucketname, "Name": filename}},
        #     OutputConfig={"S3Bucket": OUTPUT_BUCKET_NAME, "S3Prefix": OUTPUT_S3_PREFIX},
        #     NotificationChannel={"SNSTopicArn": SNS_TOPIC_ARN, "RoleArn": SNS_ROLE_ARN},
        # )
        # if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        #     return {"statusCode": 200, "body": json.dumps("Job created successfully!")}
        # else:
        #     return {"statusCode": 500, "body": json.dumps("Job creation failed!")}


def pdf_to_image(bucketname, filename):
  
    """Take a pdf fom an S3 bucket and convert it to a list of pillow images (one for each page of the pdf).
    :param event: A Lambda event (referring to an S3 event object created event).
    :return:
    """
    if not filename.endswith(_SUPPORTED_FILE_EXTENSION):
        raise Exception(f"Only .pdf files are supported by this module.")


    # Fetch the image bytes
    s3 = boto3.resource('s3')
    obj = s3.Object(bucketname, filename)
    infile = obj.get()['Body'].read()
    print("Successfully retrieved S3 object.")

    images = convert_from_bytes(infile,
                                dpi=DPI,
                                fmt=FMT)
    print("Successfully converted pdf to image.")
    print(filename)
    short_filename = filename.split('/')[1]
    print(short_filename)
    
    if 'part' in short_filename:
        part_number = int(short_filename.split('part_')[1].split('_')[0])
        part_no = (50 * part_number) + 1
        total_pages = int(short_filename.split('part_')[1].split('_')[1].replace('.pdf',''))
    else:
        part_no = 0
        total_pages = ''
    for page_num, image in enumerate(images,part_no):
        
        if '_part' in short_filename:
            directory = short_filename.split('_part')[0]
            npages = int(short_filename.split('_')[-1].replace('.pdf','').replace('.tif',''))
            total_pages = directory.split('page')[-1].split('.')[0]
            location = OUTPUT_S3_IMAGE_PREFIX  + "/" + directory + "/" + directory + "_page_" + str(page_num+1) +'_of_'+ str(total_pages) + '.' + FMT
        else:
            directory = short_filename.replace('.pdf','').replace('.tif','').split('zigna_revmaxai_page')[0]
            npages = page_num + 1
            new_dir = directory.split('zigna_revmaxai_page')[-1]
            total_pages = short_filename.split('zigna_revmaxai_page')[-1].split('.')[0]
            print(directory, npages,new_dir,total_pages)
            location = OUTPUT_S3_IMAGE_PREFIX  + "/" + new_dir + "/" + new_dir + "_page_" + str(page_num+1) +'_of_'+ str(total_pages) + '.' + FMT

        print(f"Saving page number {str(page_num)} to S3 at location: {OUTPUT_BUCKET_NAME}, {location}.")

        # Load it into the buffer and save the boytjie to S3
        buffer = BytesIO()
        image.save(buffer, FMT.upper())
        buffer.seek(0)

        s3.Object(
            OUTPUT_BUCKET_NAME,
            location
        ).put(
            Body=buffer,
            Metadata={
               'ORIGINAL_DOCUMENT_BUCKET': bucketname,
                'ORIGINAL_DOCUMENT_KEY': filename,
                'PAGE_NUMBER': str(page_num),
                'PAGE_COUNT': str(len(images))
            }
        )
        
    b = s3.Bucket(OUTPUT_BUCKET_NAME)
    c = b.objects.filter(Prefix = OUTPUT_S3_IMAGE_PREFIX  + "/" + directory + "/")
    print(directory)
    timages = [img.key for img in c]
    print(len(timages),npages)
    
    if len(timages) == npages:
        if '_part' in filename:
            inp_file_name = filename.split('_part')[0] + '.pdf'
            print(inp_file_name)
            copy_source = {'Bucket': OUTPUT_BUCKET_NAME,'Key': inp_file_name.replace('split','input')}
            s3r.meta.client.copy(copy_source, OUTPUT_BUCKET_NAME, inp_file_name.replace('split','processed'))
            delete_object(OUTPUT_BUCKET_NAME,inp_file_name.replace('split','input'))
            print('processing done..........')
            print(len(timages), npages)
        else:
            print(filename)
            copy_source = {'Bucket': OUTPUT_BUCKET_NAME,'Key': filename.replace('split','input')}
            s3r.meta.client.copy(copy_source, OUTPUT_BUCKET_NAME, filename.replace('split','processed'))
            delete_object(OUTPUT_BUCKET_NAME,filename.replace('split','input'))
            print('processing done..........')
            print(len(timages), npages) 
            
    else:
        print(len(timages), npages)
        print('processing..........')
            

    print(f"PDF document ({filename}) successfully converted to a series of images.")


