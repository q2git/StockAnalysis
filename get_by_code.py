# -*- coding: utf-8 -*-
"""
Created on Sun Oct 02 11:23:54 2016

@author: q2git

# fetching stock data by code from internet and storing it into database
"""

import tushare as ts
import pandas as pd
import datetime
import os


DATA_FOLDER = 'data'


def get_codes():
    codes = ts.get_stock_basics().index.tolist() #stock codes 
    return codes

    
def gen_daterange(start):
    if not start:
        today = pd.to_datetime(datetime.date.today())
        start = today - pd.Timedelta(days=30)
    #bdays = pd.bdate_range(start='2016-09-01', end='2016-10-01') #working days    
    bdays = pd.period_range(start=start, periods=20, freq='B')
    return bdays
 
    
def get_tick(code, start=None):
    bdays = gen_daterange(start)
    fn = os.path.join(DATA_FOLDER,'{}_tick_{}.csv'.format(code,start))

    data = []
    print 'writting tick data to [{}]... '.format(fn)
    for day in bdays:
        print 'reading [{}]-->'.format(day),  
        df = ts.get_tick_data(code=code, date=day,retry_count=3, pause=1,) 
        
        if not pd.isnull(df.price[0]):    
            df['time'] = str(day)+' '+df.time
            data.append(df)
            
        print 'ok.'
        
    dfs = pd.concat(data)
    dfs.to_csv(fn, encoding='gb2312', index=False)
    print 'done.'
    
    
def get_kdata(code): 
    fn  = os.path.join(DATA_FOLDER,'{}_kdata.xlsx'.format(code))    
    with pd.ExcelWriter(fn) as writer:
        for ktype in ['D','W','M','5','15','30','60']:
            print 'writting [{}] to [{}]... '.format(ktype,fn),
            df = ts.get_hist_data(code=code,ktype=ktype,retry_count=3, pause=3)
            df.to_excel(writer, sheet_name='{}'.format(ktype))
            print 'done.'
 
            
            
def main():
    code = str(raw_input('Stock Code: '))
    start = str(raw_input('Start Day: '))
    
    if code in get_codes():
        print '*'*20,'GET K DATA','*'*20
        get_kdata(code)
        print '*'*20,'GET TICK DATA','*'*20
        get_tick(code,start)
    else:
        print 'wrong code.'
    
    
if __name__ == '__main__':
    main()
    raw_input('END.') #press any key to exit