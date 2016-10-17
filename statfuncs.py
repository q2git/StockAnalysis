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

    
def db2df(years='2016', ktype='D', table='stocks'):
    """ reading data from db and return a generator """
    
    for year in years.split('.'):
 
        db = os.path.join(DATA_DIR, '{}_{}.db'.format(year, ktype))
    
        with sqlite3.connect(db) as con:
            sql = 'select * from {}'.format(table)  #stocks or indexs 
            print 'Reading table [{}] from [{}] ...'.format(table, db), 
            df = pd.read_sql(sql, con)
            print 'Done.'
    
        yield df      


def add_Cols(df, ma_days=[30,60], rmxx_days=[60]):
    """ add columns moving average and rolling-max/min to dataframe 
    usage: df = add_MAs_RMs(df, mas=[30,60], rms=[30,90]) """
    
    print 'Adding columns ma{} rmxx{} to dataframe...'.format(ma_days,rmxx_days),
   
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

    print 'Done.'
    
    return df 
    

def stat_daily(s):
    """ apply function for daily """
    
    kwargs = {}
    pct_coff = 1 #100.0/s.code.count() #to percentage
    # p change
    p_changes=[1,5,9]
    for i in p_changes:
        k1 = 'p_change > +{:.0f}%'.format(i)
        k2 = 'p_change < -{:.0f}%'.format(i)
        kwargs[k1] = np.where(s['p_change']>=i, 1.0, 0).sum() * pct_coff
        kwargs[k2] = np.where(s['p_change']<=-i, 1.0, 0).sum() * pct_coff   

    mas = s.columns.str.extract('(^ma\d+)').dropna().tolist() #extract ma
    mas.sort(key=lambda x: int(x[2:])) 
    # ma trends
    for ma in mas:
        k1 = 'close > {}'.format(ma) #above      
        kwargs[k1] = np.where(s['close']>=s[ma], 1.0, 0).sum() * pct_coff          
    
    trends=[0,1,2] #trends[0] means close>ma5>ma10>ma..
    for i in trends:
        _mas = mas[i:]
        _mas.insert(0,'close')
        cmp_pairs = zip(_mas[i:], _mas[i+1:])
        k1 = 'trend_up:' + ' > '.join(_mas) #up trend (close>=ma5>=10>=20...)
        k2 = 'trend_down:' +' < '.join(_mas) #down trend (close<ma5<10<20...)
        c1 = reduce(lambda m,n: m&n, map(lambda (x,y):s[x]>=s[y], cmp_pairs))
        c2 = reduce(lambda m,n: m&n, map(lambda (x,y):s[x]<s[y], cmp_pairs)) 
        kwargs[k1] = np.where(c1, 1.0, 0).sum() * pct_coff
        kwargs[k2] = np.where(c2, 1.0, 0).sum() * pct_coff     

    # rolling_max/min
    rmaxs = s.columns.str.extract('(^rmax\d+)').dropna().tolist() #extract rmax
    rmins = s.columns.str.extract('(^rmin\d+)').dropna().tolist() #extract rmin
    for rmax, rmin in zip(rmaxs, rmins):
        k1 = 'close = {}'.format(rmax) #close = highest
        k2 = 'close = {}'.format(rmin) #close = lowest          
        kwargs[k1] = np.where(s['close']==s[rmax], 1.0, 0).sum() 
        kwargs[k2] = np.where(s['close']==s[rmin], 1.0, 0).sum() 

    # close, swing, volumn
    kwargs['avg_close'] =  np.multiply(s['close'], s['volume']).sum() / s['volume'].sum()
    kwargs['avg_swing'] = ((s['high']-s['low']) / s['low']).mean()
    kwargs['avg_volume'] = s['volume'].mean()          
     
    ser = pd.Series(data=kwargs.values(), index=kwargs.keys()).sort_index()
    
    return ser                    
                     
                     
def stat(years='2016',  w=None, add_mas=[30,60], add_rmxxs=[30,90]):
    """ statistic analysis """
    
    # indexs
    dfi = pd.concat(db2df(years, table='indexs'), ignore_index=True)
    dfi = dfi.groupby(['code','date'])['close'].max().unstack(0)
    dfi.rename(columns=lambda x:'idx_{}'.format(x), inplace=True)   
    
    df = pd.concat(db2df(years), ignore_index=True)
    df = add_Cols(df, add_mas, add_rmxxs)
    
    print 'Performing statistics...',
    
    df = df.groupby('date').apply(stat_daily) # statistical analysis
   
    if w:
        for colname in df.columns:
            if colname.startswith(('p_change',)):
                df[colname] = df[colname].rolling(window=w).mean()
                df.rename(columns={colname:'{}_w{}'.format(colname, w)},inplace=True)

    df = pd.concat([dfi, df], axis=1, join_axes=[dfi.index]) #.sort_index(1)
    df.to_excel(os.path.join(STAT_DIR, 'stat_{}_daily.xlsx'.format(years))) 
    
    #df.index = pd.to_datetime(df.index, format="%Y-%m-%d") #.to_period(freq='D')
    #df.index = pd.to_datetime(df.index).to_period(freq='D')    
    print 'Done.'
    
    return df       
    
    
if __name__ == '__main__':
    years = raw_input('Years(=2016, 2015.2016): ')
    if not years: years='2016'
    
    idx = raw_input('Index(=sh,sz,hs300,sz50,zxb,cyb): ')
    if not idx: idx='sh'
    
    w = raw_input('Window size(=None): ')
    if w: w=int(w)
    
    df = stat(years, w)
