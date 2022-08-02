import boto3 
from tqdm import tqdm
import json
import zipfile
from io import BytesIO
from datetime import datetime
import gzip

class S3_operations:

    def __init__(self, bucket_region_name, access_key, secret_access_key, bucket_name):
    
    # initialize connection
        self.bucket_name = bucket_name
        self.s3r = boto3.resource("s3", region_name=bucket_region_name, aws_access_key_id=access_key,aws_secret_access_key=secret_access_key)

        self.s3c = boto3.client("s3", region_name=bucket_region_name, aws_access_key_id=access_key,aws_secret_access_key=secret_access_key)
        
        self.bucket = self.s3r.Bucket(self.bucket_name)

    def get_total_files(self):
        
         # list all files in the bucket
        self.files_in_bucket = list(self.bucket.objects.all())
        pf_count = 0
        for i in tqdm(self.files_in_bucket):
            pf_count+=1

        print(f'Total files in {self.bucket_name} - {pf_count}')

    def get_files_in_folder(self, folder_name):

        file_count = 0
        self.files_in_bucket = list(self.bucket.objects.all())
        for i in tqdm(self.files_in_bucket):
            if folder_name in i.key:
                file_count+=1
        print('......... \n')
        print(f'Total files in {self.bucket_name} in folder {folder_name} -{file_count}')
        
        
    def get_processed_file_numbers(self, folder_name):

        file_count = 0
        file_numbers = []
        self.files_in_bucket = list(self.bucket.objects.all())
        for i in tqdm(self.files_in_bucket):
            if folder_name in i.key:
                try:
                    f_no = i.key.split('/')[1]
                    file_numbers.append(f_no)
                    file_count+=1
                except:
                    print(i.key.split('/')[1].replace('file','').replace('.json',''))
        print('......... \n')
        print(f'Total files in {self.bucket_name} in folder {folder_name} -{file_count}')
        return file_numbers
        
    def get_file(self,processed_bname, folder_name, output_bname, filerange):
        
        try:
            fname = folder_name + f'file{filerange}.json'
            response = self.s3c.get_object(Bucket=processed_bname, Key=fname)
            data = response['Body'].read()
            json_data = json.loads(data)
            target_file_name = folder_name + json_data['fname']+'.'+json_data['ftype']
            target_object = self.s3c.get_object(Bucket=output_bname, Key=target_file_name)
            size = int(target_object.get('ResponseMetadata').get('HTTPHeaders').get('content-length'))
            print(target_file_name, size)
        except:
            print(f'{filerange} File not found')

    def get_processed_file_info(self,processed_bname, folder_name, output_bname, filerange):
        if type(filerange) == list:
            for i in range(filerange[0],filerange[1]):
                self.get_file(processed_bname, folder_name, output_bname, i)
        else:
            self.get_file(processed_bname, folder_name, output_bname, filerange)

    def zip_extract(self, bucket_name, key):
        """
        ****LAMBDA SPECIFIC****
        Extracts zipped files less than 1.5 gb
        Stores the unzipped files in specified s3 bucket.
        """
        zip_obj = self.s3r.Object(bucket_name="bucket_name", key=key)
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

    def zip_extract_large(self, path,out_path, ix_begin, ix_end):
        """
        ****EC2 & AWS WORKSPACE SPECIFIC**** 
        Extracts large zipped files but stores the unzipped files in local storage.
        If run in a EC2 instance or workspace which has a s3 mount, it can store the unzipped files in s3 bucket (yet to experiment)
        """

        with zipfile.ZipFile(path, 'r') as zf:
            infos = zf.infolist()
            print(infos)
            for ix in range(max(0, ix_begin), min(ix_end, len(infos))):
                print(infos[ix])
                zf.extract(infos[ix], out_path)
            zf.close()

    def gzip_extract(self,bucket, gzipped_key, uncompressed_key):
        """
         ****LAMBDA & EC2 SPECIFIC****
         Extracts gzip files upto 1.5 GB via lambda and stores the unzipped file in the specified s3 location.
         Extracts gzip files greater than 1.5 GB via AWS workspace. EC2 (we will have to experiment)

        """
        now  = datetime.now()
        print(now)
        self.s3c.upload_fileobj(
            Fileobj=gzip.GzipFile(
                None,
                'rb',
                fileobj=BytesIO(self.s3c.get_object(Bucket=bucket, Key=gzipped_key)['Body'].read())),
            Bucket=bucket,
            Key=uncompressed_key)   

        now  = datetime.now()
        print(now) 




# Execution example
# o = S3_operations('us-east-2', 'AKIA5GWC4RWCD6OAL5PW', 'IL9UykncKScuGTj9cq4J1EywN6UPJY6Q7ViDJoOB', 'revmaxai-bulk-download-processed')

# o.get_total_files()


# o.get_files_in_folder('humana/')



# file_names = o.get_processed_file_numbers('humana/')


# o.get_processed_file_info('revmaxai-bulk-download-processed', 'humana/', 'revmaxai-bulk-download-output', [-10,50])
