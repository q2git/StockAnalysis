# -*- coding: utf-8 -*-
"""
Created on Tue Oct 11 16:53:08 2016
@author: q2git
"""

import pandas as pd
import numpy as np
import sqlite3
import os
from commons import DATA_DIR, STAT_DIR

    
def db2df(years='2016', ktype='D', table='stocks', que=None):
    """ reading data from db and return a generator """
    
    for year in years.split('.'):
 
        db = os.path.join(DATA_DIR, '{}_{}.db'.format(year, ktype))
        
        msg = 'Reading table [{}] from [{}] ...'.format(table, db)
        if que is not None:
            que.put((0,msg))
        else:
            print msg, 
            
        with sqlite3.connect(db) as con:
            sql = 'select * from {}'.format(table)  #stocks or indexs 
            df = pd.read_sql(sql, con)
            df.drop_duplicates(inplace=True)
            
        if que is not None:
            que.put((0,'Done.\r\n'))
        else:
            print 'Done.' 
    
        yield df      


def add_cols(df, ma_days=[30,60], rmxx_days=[60], que=None):
    """ add columns moving average and rolling-max/min to dataframe 
    usage: df = add_MAs_RMs(df, mas=[30,60], rms=[30,90]) """
    
    msg = 'Adding columns ma{} rmxx{} to df...'.format(ma_days,rmxx_days)
    if que is not None:
        que.put((0,msg))
    else:
        print msg,
            
    df0 = df.set_index('date').sort_index()
    
    for day in ma_days:
        ma = df0.groupby('code')['close'].rolling(day).mean()\
             .reset_index().rename(columns={'close':'ma{}'.format(day)})
        
        df = pd.merge(df, ma, on=['code', 'date'])
        
    for day in rmxx_days:
        c_max = df0.groupby('code')['close'].rolling(day).max()\
                .reset_index().rename(columns={'close':'rmax{}'.format(day)})
                
        c_min = df0.groupby('code')['close'].rolling(day).min()\
                .reset_index().rename(columns={'close':'rmin{}'.format(day)})
    
        df = pd.merge(df, c_max, on=['code', 'date'])
        df = pd.merge(df, c_min, on=['code', 'date'])

    if que is not None:
        que.put((0,'Done.\r\n'))
    else:
        print 'Done.' 
    
    return df 
    

def stat_daily(s):
    """ apply function for daily """
    
    kwargs = {}
    pct_coff = 100.0/s.code.count() #to percentage
    # p change
    p_changes=[1,5,9]
    for i in p_changes:
        k1 = 'p_change: >+{:.0f}%'.format(i)
        k2 = 'p_change: <-{:.0f}%'.format(i)
        kwargs[k1] = np.where(s['p_change']>=i, 1.0, 0).sum() * pct_coff
        kwargs[k2] = np.where(s['p_change']<=-i, 1.0, 0).sum() * pct_coff   

    mas = s.columns.str.extract('(^ma\d+)', expand=False).dropna().tolist() #extract ma
    mas.sort(key=lambda x: int(x[2:])) 
    # ma trends
    for ma in mas:
        k1 = 'close>: {}'.format(ma) #above      
        kwargs[k1] = np.where(s['close']>=s[ma], 1.0, 0).sum() * pct_coff          
    
    trends=[0,1,2] #trends[0] means close>ma5>ma10>ma..
    for i in trends:
        _mas = mas[i:]
        _mas.insert(0,'close')
        cmp_pairs = zip(_mas[i:], _mas[i+1:])
        k1 = 'trend: ' + '>'.join(_mas[:2]) #up trend (close>=ma5>=10>=20...)
        k2 = 'trend: ' +'<'.join(_mas[:2]) #down trend (close<ma5<10<20...)
        c1 = reduce(lambda m,n: m&n, map(lambda (x,y):s[x]>=s[y], cmp_pairs))
        c2 = reduce(lambda m,n: m&n, map(lambda (x,y):s[x]<s[y], cmp_pairs)) 
        kwargs[k1] = np.where(c1, 1.0, 0).sum() * pct_coff
        kwargs[k2] = np.where(c2, 1.0, 0).sum() * pct_coff     

    # rolling_max/min
    rmaxs = s.columns.str.extract('(^rmax\d+)', expand=False).dropna().tolist() #extract rmax
    rmins = s.columns.str.extract('(^rmin\d+)', expand=False).dropna().tolist() #extract rmin
    for rmax, rmin in zip(rmaxs, rmins):
        k1 = 'close=: {}'.format(rmax) #close = highest
        k2 = 'close=: {}'.format(rmin) #close = lowest          
        kwargs[k1] = np.where(s['close']==s[rmax], 1.0, 0).sum() * pct_coff
        kwargs[k2] = np.where(s['close']==s[rmin], 1.0, 0).sum() * pct_coff

    # close, swing, volumn
    kwargs['avg: close'] =  np.multiply(s['close'], s['volume']).sum() / s['volume'].sum()
    kwargs['avg: swing'] = ((s['high']-s['low']) / s['low']).mean() * 100
    kwargs['avg: volume'] = s['volume'].mean()          
     
    ser = pd.Series(data=kwargs.values(), index=kwargs.keys()).sort_index()
    
    return ser                    


def df_idxs_codes(years='2016', ktype='D', add_mas=[30,60], 
         add_rmxxs=[30,90], que=None):
    """ get dfs of indexs and codes """
    # indexs
    dfi = pd.concat(db2df(years, ktype, table='indexs', que=que), 
                    ignore_index=True)
    dfi = dfi.groupby(['code','date'])['close'].max().unstack(0)
    dfi.rename(columns=lambda x:'idx: {}'.format(x), inplace=True)   
    # codes    
    dfc = pd.concat(db2df(years, ktype, que=que), ignore_index=True) 
    dfc = add_cols(dfc, add_mas, add_rmxxs, que)

    if que is not None:
        que.put((2,(dfi,dfc))) 
        
    return (dfi, dfc)
                         
                     
def stat(dfi, dfc, w=None, k=None, que=None):
    """ statistic analysis """
    
    if que is not None:     
        que.put((0,'Performing statistics...'))
    else:
        print 'Performing statistics...',
    
    dfc = dfc.groupby('date').apply(stat_daily) # statistical analysis
   
    if w and k:
        for colname in dfc.columns:
            if colname.startswith((k,)):
                dfc[colname] = dfc[colname].rolling(window=w).mean()
                dfc.rename(columns={colname:'{}_w{}'.format(colname, w)},inplace=True)

    df = pd.concat([dfi, dfc], axis=1, join_axes=[dfi.index]) #.sort_index(1)
    
    if que is not None:
        que.put((0,'Done.\r\n'))
        que.put((1,df))
    else:
        print 'Done.'
        return df       
    
    
if __name__ == '__main__':
    years = raw_input('Years(=2016, 2015.2016): ')
    if not years: years='2016'
    
    w = raw_input('Window size(=None): ')
    if w: w=int(w)
    
    dfi, dfc = df_idxs_codes(years)
    df = stat(dfi, dfc, w)
    df.to_excel(os.path.join(STAT_DIR, 'stat_{}_daily.xlsx'.format(years)))     
