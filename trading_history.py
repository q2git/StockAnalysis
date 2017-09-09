# -*- coding: utf-8 -*-
"""
Created on Sat Sep  9 10:02:48 2017

@author: q2git
"""

import pandas as pd
import glob
import os


def txtTodf(fp):
    "read text file to dataframe"
    txt = open(fp, encoding='gbk').read()
    txt = txt.replace('=','').replace('"','')
    lines = txt.split('\n')
    table = [l.split('\t') for l in lines]
    
    df = pd.DataFrame(table)
    df.dropna(inplace=True)
    df.columns = df.ix[1]
    df = df.ix[2:]
    
    'convert specifed columns to numeric value'
    col_idxs = [3,4,5,6,7,10,11,12,13]
    for col_idx in col_idxs:
        df[df.columns[col_idx]] = pd.to_numeric(df[df.columns[col_idx]], errors='coerce')
        
    return df


def main():
    
    fplist = glob.glob(os.path.join(r'D:\06_Testdata\Data', '*.xls'))
    df = pd.DataFrame()
    
    for fp in fplist:
        print('Processing ', fp, ' ...')
        df = df.append(txtTodf(fp))
        
    df.to_excel('test.xlsx', index=False)
