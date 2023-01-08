# -*- coding: utf-8 -*-
"""
Created on Sun Jan  8 16:16:55 2023

@author: Suriya
"""

import os
import pandas as pd

def  cmsdata(path, indices, columns, **kwargs):
    
    idx = []
    df = pd.DataFrame()
    c = 0
    out_path = kwargs.get('outdir',None)
    
    for i in indices:
        idx.append((i[0]-1,i[1]))

    with open(path) as f:
        lines = f.readlines()
    for n in idx:
        d = pd.Series(lines).apply(lambda x : x[n[0]:n[1]])
        df.loc[:,columns[c]] = d
        c+=1
    
    if out_path:
        fname = path.split('\\')[-1].replace('.text','.csv')
        op = out_path + '\\' + fname
        df.to_csv(op)
    else:
        op = path.replace('.txt','.csv')
        df.to_csv(op)
    
    print('Preview of the data:')
    print(df.head())
    df.drop(columns = 'Filler',inplace = True)
    return df

path = r"C:\Users\User\Downloads\PFALL23.txt"

columns = ['Filler',	'Year',	'Filler',	'Carrier Number',	'Filler',	'Locality',	'Filler',	'HCPCS Code',	'Filler',	'Modifier',	'Filler',	'Non Facility Fee Schedule Amount',	'Filler',	'Facility Fee Schedule Amount',	'Filler',	'Filler',	'Filler',	'PCTC Indicator',	'Filler',	'Status Code', 'Filler',	'Multiple Surgery Indicator',	'Filler',	'50% Therapy Reduction Amount','Filler',	'50% Therapy Reduction Amount',	'Filler',	'OPPS Indicator',	'Filler',	'OPPS Non Facility Fee Amount',	'Filler',	'OPPS Faclility Fee Amount',	'Filler']

indices = [(1, 1), (2, 5), (6, 8), (9, 13),(14,16),(17,18),(19,21),(22,26),(27,29),(30,31),(32,34),(35,44),(45,47),(48,57),(58,60),(61,61),(62,64),(65,65),(66,68),(69,69),(70,72),(73,73),(74,76),(77,86),(87,89),(90,99),(100,102),(103,103),(104,106),(107,116),(117,119),(120,129),(130,130)]


data = cmsdata(path, indices, columns, outdir = r"C:\Users\User\Downloads")

