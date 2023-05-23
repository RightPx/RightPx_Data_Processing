# -*- coding: utf-8 -*-
"""
Created on Tue Jan  3 07:58:50 2023

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
    
session = boto3.Session(aws_access_key_id='access key',aws_secret_access_key='secret key')


#######
def prov_ref(j,n_value):
    global n
    global g 
    g = pd.DataFrame()
    
    with open(j, 'rb', transport_params={'client': session.client('s3')}) as fout:
        objects = ijson.items(fout, 'provider_references.item')
        fname1 = j.split('?')[0].split('/')[-1].replace('.json','')
        for n, k in enumerate(objects):
            c = dict()
            try:
                if n >= n_value:
                    pgi = k.get('provider_group_id')
                    with open(k.get('location'),'rb') as f:
                        fname = k.get('location').split('?')[0].split('/')[-1].replace('.json','')
                        data = json.load(f)
                        data.update({'provider_group_id':pgi})
                        os.chdir(r'C:\Users\suriya\Downloads\1')
                        with open(f'{fname}.json','w') as f:
                            json.dump(data,f, cls = DecimalEncoder)
                        s3.meta.client.upload_file(f'{fname}.json', 'zigna-nsa-data', f'provider-references/Q1-2023/jan-cigna/{fname1}/{fname}.json')
                        os.remove(f'{fname}.json') 
            except:
                c['provider_group_id'] = k.get('provider_group_id')
                c['location'] =  k.get('location')
                c['n_value'] = n
                f = pd.DataFrame.from_dict(c,orient='index').transpose()
                g = pd.concat([g,f])
    
    if len(g) > 0:
        if len(g.n_value) > 0:
            for l in g.n_value:
                prov_ref(j, l)

            
prov_ref(j = 's3://zigna-nsa-data/raw-json-files/Q1-2023/jan-cigna/2023-01-01_cigna-health-life-insurance-company_localplus-sar_in-network-rates.json', n_value = 0)



