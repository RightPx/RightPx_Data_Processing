# -*- coding: utf-8 -*-
"""
Created on Sun Nov 27 02:57:07 2022

@author: User
"""


import os
from tqdm import tqdm
import boto3
from smart_open import open
from io import BytesIO
from pikepdf import Pdf


class ChunkPdf():
    
    def __init__(self, input_bucket, input_folder, access_key = 'AKIA5GWC4RWCLLVC5TCO',secret_key='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'):
        
        session = boto3.Session(aws_access_key_id=access_key,aws_secret_access_key=secret_key)
        self.s3r = session.resource('s3')
        self.s3c = session.client('s3')
        
        self.input_bucket = input_bucket
        self.input_folder = input_folder
        b = self.s3r.Bucket(self.input_bucket)
        c = b.objects.filter(Prefix = self.input_folder)
        self.files = [i.key for i in c]

    def chunk_pdf(self,output_bucket,output_folder, chunk_size = 50):
        
        self.output_bucket = output_bucket
        self.output_folder = output_folder
        self.chunk_size = chunk_size 

        for key in tqdm(self.files[1:]):
            data = self.s3c.get_object(Bucket = self.input_bucket ,Key = key)['Body'].read()
            pdf = Pdf.open(BytesIO(data))
            num_pages = len(pdf.pages)
            if num_pages <= self.chunk_size :
                copy_source = {
                    'Bucket': self.input_bucket,
                    'Key': key
                }
                chng_folder = key.replace(self.input_folder,self.output_folder)
                pg_no = 'page' + f'{num_pages}.pdf'
                self.out_folder = chng_folder.replace('.pdf',pg_no)
                self.s3r.meta.client.copy(copy_source, self.output_bucket, self.out_folder)
            else:
                pname = ''
                part = 0
                dst = Pdf.new()
                for n, page in enumerate(pdf.pages):
                    if n % self.chunk_size  == 0 and n!=0:
                        self.s3r.Bucket(self.output_bucket).upload_file(pname,self.output_folder+pname)
                        os.remove(pname)
                        part +=1
                        dst = Pdf.new()
                    dst.pages.append(page)
                    split_fname = key.split('/')[-1].split('.pdf')[0]
                    tag = f'_part_{part}_{len(pdf.pages)}' + '.pdf'
                    pname = split_fname+tag
                    dst.save(pname)
                self.s3r.Bucket(self.output_bucket).upload_file(pname,self.output_folder+pname)
                os.remove(pname)        


if __name__ == "__main__":
    o = ChunkPdf('human-review-stage','input-pdf/')
    o.chunk_pdf('human-review-stage', 'split-pdf/')

