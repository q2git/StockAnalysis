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
    ''' add moving average to dataframe '''
    
    df0 = df.set_index('date').sort_index()
    
    for day in args:
        ma = df0.groupby('code')['close'].rolling(day).mean()\
             .reset_index().rename(columns={'close':'ma{}'.format(day)})
        
        df = pd.merge(df, ma, on=['code', 'date'])
    
    return df 

    
def db2df(years='2016', ktype='D', table='stocks'):
    """ reading data from db and return DataFrame object """
    
    df = pd.DataFrame()
    
    for year in years.split('.'):
 
        db = os.path.join(DATA_FOLDER, '{}_{}.db'.format(year, ktype))
    
        with sqlite3.connect(db) as con:
            sql = 'select * from {}'.format(table)  #stocks or indexs 
            
            print 'Reading table [{}] from [{}] ...'.format(table, db), 
            df = df.append(pd.read_sql(sql, con), ignore_index=True)
            print 'Done.'
    
    return df      


def fun1(s):
    """ apply function """
    kwargs = {}
    # rise and fall ratio
    for i in [0.1, 3, 7]:
        k1 = 'pct_sz{:.0f}'.format(i)
        k2 = 'pct_xd{:.0f}'.format(i)
        rise = np.where(s.p_change>=i, 1.0, 0).sum()
        fall = np.where(s.p_change<=-i, 1.0, 0).sum()
        kwargs[k1] = ((rise) / s.code.count()) * 100
        kwargs[k2] = ((fall) / s.code.count()) * 100     
    
    # rising trend        
    c1 = (s.close>=s.ma5) & (s.ma5>=s.ma10) & (s.ma10>=s.ma20)\
             & (s.ma20>=s.ma30) & (s.ma30>=s.ma60)
    c2 = (s.close>=s.ma20) & (s.ma20>=s.ma30) & (s.ma30>=s.ma60)
    kwargs['ma_ss5'] = (np.where(c1, 1.0, 0).sum() / s.code.count()) * 100
    kwargs['ma_ss20'] = (np.where(c2, 1.0, 0).sum() / s.code.count()) * 100  

    # falling trend
    c3 = (s.close<s.ma5) & (s.ma5<s.ma10) & (s.ma10<s.ma20)\
             & (s.ma20<s.ma30) & (s.ma30<s.ma60)
    c4 = (s.close<s.ma20) & (s.ma20<s.ma30) & (s.ma30<s.ma60)
    kwargs['ma_xj5'] = (np.where(c3, 1.0, 0).sum() / s.code.count()) * 100
    kwargs['ma_xj20'] = (np.where(c4, 1.0, 0).sum() / s.code.count()) * 100 
    
    return pd.Series(data=kwargs.values(), index=kwargs.keys())
                     
                     
                     
def stat(years='2016', window=10):
    """ statistic analysis """
    
    dfidxs = db2df(years, table='indexs')
    dfidxs = dfidxs.groupby(['code','date'])['close'].max().unstack(0)
    dfidx = dfidxs['sh'].rename('idx')
    
    dfstks = db2df(years)
    dfstks = add_MAs(dfstks, 30, 60)

    df = dfstks.groupby('date').apply(fun1)

    for colname in df.columns:
        if not colname.startswith('ma_'):
            df[colname] = df[colname].rolling(window=window).mean()

    df = pd.concat([dfidx, df], axis=1, join_axes=[dfidx.index])
           
    return df.sort_index(1)

    
def plot1(df):
    
    for key in ['pct_sz','pct_xd','ma_ss','ma_xj']:
        ks = []

        for colname in df.columns:
            if colname.startswith(key):
                ks.append(colname)
                
        ks.append('idx')

        df[ks].plot(kind='line', grid=True, subplots=True, legend=False, 
                    rot='vertical', xticks=np.arange(0, len(df), 10))  
                 
        for ax in plt.gcf().axes:
            ax.legend(loc='center left', bbox_to_anchor=(1.0, 0.5)) 
           
   
     
def main():
    years = raw_input('Years(eg, 2015.2016) or all: ')
    w = input('window: ')
    
    df = stat(years, w)
    
    plot1(df)
     
    plt.show() 
 


    
    
if __name__ == '__main__':
    main()
    
    