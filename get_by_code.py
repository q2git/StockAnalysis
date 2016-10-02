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
    ''' get all stock codes '''
    return ts.get_stock_basics().index.tolist() #stock codes 


    
def get_today():
    ''' get today's data '''
    print '*'*20,'DAILY TRAING DATA','*'*20
    
    fn = os.path.join(DATA_FOLDER,'daily','{}.csv'.format(datetime.date.today()))
    print 'writting trading data to [{}] ... '.format(fn)
    df = ts.get_today_all() #ts.get_stock_basics()
    df.to_csv(fn, encoding='gbk', ) 
    print 'done.'
    
    
def gen_bdays():
    ''' generate bussiness date range '''   
    start = str(raw_input('Start Day: '))
    end = str(raw_input('End Day: '))
    
    today = pd.to_datetime(datetime.date.today())
    if not start:
        if not end:
            end = today
        start = pd.to_datetime(end) - pd.Timedelta(days=30)                                 
    else:
        if not end:
            end = pd.to_datetime(start) + pd.Timedelta(days=30)
 
    #bdays = pd.bdate_range(start='2016-09-01', periods=20,) #working days    
    bdays = pd.period_range(start=start, end=end, freq='B')
    return bdays
 
    
def get_tick(code):
    ''' get tick data '''
    print '*'*20,'TICK DATA (DEFAULT:30days)','*'*20
        
    bdays = gen_bdays()
    fn = os.path.join(DATA_FOLDER,'{}_tick_{}.csv'.format(code,bdays[0]))

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
    ''' get k data '''
    print '*'*20,'K DATA (D,W,...,5)','*'*20
    
    fn  = os.path.join(DATA_FOLDER,'{}_kdata.xlsx'.format(code))   
    
    with pd.ExcelWriter(fn) as writer:
        for ktype in ['D','W','M','5','15','30','60']:
            print 'writting [{}] to [{}]... '.format(ktype,fn),
            df = ts.get_hist_data(code=code,ktype=ktype,retry_count=3, pause=3)
            df.to_excel(writer, sheet_name='{}'.format(ktype))
            print 'done.'
 
            
            
def main():
    get_today()
    
    code = str(raw_input('Stock Code: '))
    
    if code in get_codes():
        get_kdata(code)
        get_tick(code)
    else:
        print 'wrong code.'
    
    
if __name__ == '__main__':
    main()
    raw_input('END.') #press any key to exit