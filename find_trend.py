# -*- coding: utf-8 -*-
"""
Created on Tue Oct 11 16:53:08 2016

@author: q2git
"""

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import sqlite3
import os


DATA_FOLDER = r'data\all'
STAT_FOLDER = r'data\stat'
if not os.path.exists(STAT_FOLDER):
    os.mkdir(STAT_FOLDER)


def add_MAs(df, *args):
    """ add moving average to dataframe, args=30,60,etc """
    
    df = df.set_index('date').sort_index()
    
    for day in args:
        ma = df.groupby('code')['close'].rolling(day).mean()\
             .reset_index().rename(columns={'close':'ma{}'.format(day)})
          
        df = pd.merge(df, ma, on=['code', 'date'])
    
    return df 

    
def db_to_dfs(years='all', ktype='D'):
    """ reading data from db and return DataFrame object """
    dfidx = pd.DataFrame()
    dfstk = pd.DataFrame()
    
    for year in years.split('.'):
 
        db = os.path.join(DATA_FOLDER, '{}_{}.db'.format(year, ktype))
    
        with sqlite3.connect(db) as con:
            print 'Reading table [indexs] from [{}] ...'.format(db), 
            dfidx.append(pd.read_sql('select * from indexs', con))
            print 'Done.\nReading table [stocks] from [{}] ...'.format(db),
            dfstk.append(pd.read_sql('select * from stocks', con))
            print 'Done.'
        
    return (dfidx, dfstk)   
    


def fun1(s):
    """ apply function """
    # rise and fall ratio
    rise1 = np.where(s.p_change>=0.1, 1.0, 0).sum()
    fall1 = np.where(s.p_change<=-0.1, 1.0, 0).sum()
    szb1 = ((rise1) / s.code.count()) * 100
    xdb1 = ((fall1) / s.code.count()) * 100
     
    rise2 = np.where(s.p_change>=3, 1.0, 0).sum()
    fall2 = np.where(s.p_change<=-3, 1.0, 0).sum() 
    szb2 = ((rise2) / s.code.count()) * 100   
    xdb2 = ((fall2) / s.code.count()) * 100 
     
    rise3 = np.where(s.p_change>=7, 1.0, 0).sum()
    fall3 = np.where(s.p_change<=-7, 1.0, 0).sum() 
    szb3 = ((rise3) / s.code.count()) * 100   
    xdb3 = ((fall3) / s.code.count()) * 100
    
    # rising trend
    crit1 = (s.close>=s.ma5) & (s.ma5>=s.ma10) & (s.ma10>=s.ma20)\
             & (s.ma20>=s.ma30) & (s.ma30>=s.ma60)
    crit2 = (s.close>=s.ma20) & (s.ma20>=s.ma30) & (s.ma30>=s.ma60)
    sstd1 = (np.where(crit1, 1.0, 0).sum() / s.code.count()) * 100
    sstd2 = (np.where(crit2, 1.0, 0).sum() / s.code.count()) * 100  

    # falling trend
    crit1 = (s.close<s.ma5) & (s.ma5<s.ma10) & (s.ma10<s.ma20)\
             & (s.ma20<s.ma30) & (s.ma30<s.ma60)
    crit2 = (s.close<s.ma20) & (s.ma20<s.ma30) & (s.ma30<s.ma60)
    xdtd1 = (np.where(crit1, 1.0, 0).sum() / s.code.count()) * 100
    xdtd2 = (np.where(crit2, 1.0, 0).sum() / s.code.count()) * 100 
    
    return pd.Series([sstd1, sstd2, xdtd1, xdtd2, szb1, szb2, szb3, 
                      xdb1, xdb2, xdb3, ], 
                     index=['sstd1', 'sstd2', 'xdtd1', 'xdtd2', 'szb1', 
                     'szb2', 'szb3', 'xdb1', 'xdb2', 'xdb3', ])
                     
                     
                     
def stat(years='2016', stk='000001', window=10):
    """ statistic analysis """
    
    dfidxs, dfstks = db_to_dfs()

    dfidxs = dfidxs.groupby(['code','date']).max() 
    dfidx = dfidxs.ix['sh',['close']].rename(columns={'close':'idx'})
    
    dfstks = add_MAs(dfstks, 30, 60)
    dfstk = dfstks[dfstks['code']==stk].set_index('date')[['close']]\
            .rename(columns={'close':stk})
    
    dfgb = dfstks.groupby('date').apply(fun1)
    df = pd.concat([dfidx, dfstk, dfgb], axis=1, join_axes=[dfidx.index])

    
    #df = pd.concat(dfs)
    #window = 5
    for name in ['szb1', 'szb2', 'szb3', 'xdb1', 'xdb2', 'xdb3', ]:
        df[name] = df[name].rolling(window=window).mean()
  
             
    return df
