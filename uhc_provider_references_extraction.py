# -*- coding: utf-8 -*-
"""
Created on Wed Jan  4 14:37:02 2023

@author: HospDataScrap
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
    


df = pd.read_excel(r"D:\Users\HospDataScrap\Downloads\jan_uhc_in_network_links_priority.xlsx")

os.chdir(r"D:\Users\HospDataScrap\Downloads\2")
session = boto3.Session(aws_access_key_id='access key',aws_secret_access_key='secret key')
for url in tqdm(df["s3_uri"].iloc[901:910]):
    fname = url.split('/')[-1].replace('.json','')
    df1 = pd.read_excel(r"D:\Users\HospDataScrap\Downloads\jan_payers\uhc\uhc_pro_ref_count_with_fname(901-1000).csv")
    for index, row in df1.iterrows():
        if fname == row['file_name']:
            # print(fname)
            # print(row['file_name'])
            n_value = row['n-count']
    with open(url, 'rb', transport_params={'client': session.client('s3')}) as fout:
        objects = ijson.items(fout, 'provider_references.item')
        for n, obj in enumerate(objects):
            if n == n_value:
                break
            
            with open(f'{n}.json','w') as f:
                json.dump(obj,f, cls = DecimalEncoder)
            s3.meta.client.upload_file(f'{n}.json', 'zigna-nsa-data',f'provider-references/Q1-2023/jan-uhc/{fname}/{n}.json')
            os.remove(f'{n}.json')
