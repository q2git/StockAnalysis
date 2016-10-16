# -*- coding: utf-8 -*-
"""
Created on Sun Oct 02 11:23:54 2016

@author: q2git

# get stock data by code and storing it into excel
"""

import tushare as ts
import pandas as pd
import datetime
import os

TODAY = datetime.date.today()
DATA_FOLDER = r'data\code'
if not os.path.exists(DATA_FOLDER):
    os.mkdir(DATA_FOLDER)
    
    
def get_codes():
    ''' get all stock codes '''
    return ts.get_stock_basics().index.tolist() #stock codes 
    
    
def gen_bdays():
    ''' generate bussiness date range '''   
    start = str(raw_input('Start Day: '))
    end = str(raw_input('End Day: '))
    
    today = pd.to_datetime(TODAY)
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
    print '*'*10,'TICK DATA (DEFAULT:30days)','*'*10
  
    bdays = gen_bdays()
    #fn = os.path.join(DATA_FOLDER,'{}_tick_{}.xlsx'.format(code,bdays[0]))

    data = []
    #print 'writting tick data to [{}]... '.format(fn)
    for day in bdays:
        print 'getting [{}]-->'.format(day),  
        df = ts.get_tick_data(code=code, date=day,retry_count=3, pause=1,) 
        
        if not pd.isnull(df.price[0]):    
            df['date'] = str(day)
            df = df.set_index('date')
            df.type = df.type.apply(lambda x: x.decode('utf8'))
            data.append(df)
            
        print 'done.'
        
    df = pd.concat(data)
    #dfs.to_excel(fn, index=False)
    #dfs.to_csv(fn, encoding='gb2312', index=False)
    #print 'done.'
    return ('TICK', df)
    
    
    
def get_kdata(code): 
    ''' get bu fu quan k data '''
    print '*'*10,'K DATA (D,W,...,5)','*'*10

    #fn  = os.path.join(DATA_FOLDER,'{}_kdata.xlsx'.format(code))   
    
    #with pd.ExcelWriter(fn) as writer:
    dfs = []
    for ktype in ['D','W','M','5','15','30','60']:
        print 'getting [{}] data ... '.format(ktype),
        df = ts.get_hist_data(code=code,ktype=ktype,retry_count=3, pause=3)
        #df.to_excel(writer, sheet_name='{}'.format(ktype))
        dfs.append((ktype,df))
        print 'done.'
    
    return dfs
 

def get_qfq(code): 
    ''' get qiang fu quan data '''
    print '*'*10,'QFQ DATA','*'*10
    
    #fn  = os.path.join(DATA_FOLDER,'{}_qfq.xlsx'.format(code))   

    print 'getting [{}] data ... '.format('QFQ'),
    df = ts.get_h_data(code=code, start='2000-01-01',
                       end=str(TODAY), 
                       autype='qfq', #qfq-前复权 hfq-后复权 None-不复权，默认为qfq
                       index=False, #是否是大盘指数
                       retry_count=3,pause=1,) 

    #df.to_excel(fn)
    print 'done.' 
    return ('QFQ', df)            
            


def writer(code, *data): 
    ''' writer data to excel file '''
    print '*'*10,'EXCEL WRITER','*'*10
    
    fn  = os.path.join(DATA_FOLDER,'{}_{}.xlsx'.format(code,TODAY))   
                             
    with pd.ExcelWriter(fn) as writer:
        for sht, df in data:
            print 'writting [{}] data to [{}]... '.format(sht, fn),             
            df.to_excel(writer, sheet_name=sht)
            print 'done.'            
            

            
def main():
    
    code = str(raw_input('Stock Code: '))
    
    if code in get_codes():
        dfs = get_kdata(code)
        df1 = get_tick(code)
        df0 = get_qfq(code)
        writer(code, df0, df1, *dfs)
    else:
        print 'wrong code.'
    
    
if __name__ == '__main__':
    main()
    raw_input('END.') #press any key to exit