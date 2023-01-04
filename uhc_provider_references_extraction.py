# -*- coding: utf-8 -*-
"""
Created on Mon Jan  2 12:43:22 2023

@author: suriya
"""


import os
import ijson
from smart_open import open
import pandas as pd
from tqdm import tqdm
import boto3
import json 
import decimal
from io import BytesIO

s3 = boto3.resource("s3", region_name='us-east-2', aws_access_key_id='access key',aws_secret_access_key='secret key')

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return str(obj)
        return json.JSONEncoder.default(self, obj)
    


df = pd.read_excel(r"C:\Users\suriya\Downloads\jan_uhc_in_network_links_priority.xlsx")

os.chdir(r"C:\Users\suriya\Downloads\1")
session = boto3.Session(aws_access_key_id='access key',aws_secret_access_key='secret key')
for url in tqdm(df["s3_uri"].iloc[898:901]):
    fname = url.split('/')[-1].replace('.json','')
    with open(url, 'rb', transport_params={'client': session.client('s3')}) as fout:
        objects = ijson.items(fout, 'provider_references.item')
        for n, obj in enumerate(objects):
            with open(f'{n}.json','w') as f:
                json.dump(obj,f, cls = DecimalEncoder)
            s3.meta.client.upload_file(f'{n}.json', 'zigna-nsa-data',f'provider-references/Q1-2023/jan-uhc/{fname}/{n}.json')
            os.remove(f'{n}.json')
