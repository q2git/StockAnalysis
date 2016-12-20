# -*- coding: utf-8 -*-
"""
Created on Thu Oct 27 19:00:18 2016

@author: q2git
"""

import tushare as ts
import pandas as pd
import numpy as np
import time
import os



def get_today_codes():
    for i in range(3):
        try:
            df = ts.get_today_all()
            df = df[df.open!=0]
            return df
        except Exception as e:
            print 'Error: {}, retry {}.'.format(e, i+1)
    return False
            
            
def get_today_quotes(codes, qty=50):
    df = pd.DataFrame()
    for i in range(0, len(codes), qty):
        try:
            d = ts.get_realtime_quotes(codes[i:i+qty])
            df = df.append(d, ignore_index=True)
        except Exception as e:
            print e
            
    return df
    

def calc_amount(df):
    df = df[df['ask']!=0]
    cols = df.columns.sort_values()
    cols_b = cols[cols.str.contains('b\d_[pv]')]
    cols_s = cols[cols.str.contains('a\d_[pv]')] 
            
    amt_buy = np.multiply( df[cols_b[0::2]], df[cols_b[1::2]]).sum(1)
    amt_sell = np.multiply( df[cols_s[0::2]], df[cols_s[1::2]]).sum(1)
    
    df = df[['name','date','time','code','price',]]
    df.loc[:,'amt_b&s'] = np.add(amt_buy, amt_sell)
    df.loc[:,'s/b'] = np.divide(amt_sell, amt_buy)
    df.set_index('code', inplace=True)  
    df.sort_values('s/b', ascending=False, inplace=True)
    return df

    
def monitor():
    codes = get_today_codes().code
    try:
        while 1:
            df = get_today_quotes(codes)
            cols = df.columns.tolist()
            map(cols.remove, ['code','name','date','time'])           
            df[cols] = df[cols].convert_objects(convert_numeric=True)
            df = calc_amount(df)
            os.system('cls')
            print df.head(20)
            time.sleep(3)

    except (KeyboardInterrupt, SystemExit):
        print '\n'
        print '#'*5,'System Exit'.upper(),'#'*5

    
if __name__ == '__main__':
    monitor()

                       
                       
                       
                       
                       